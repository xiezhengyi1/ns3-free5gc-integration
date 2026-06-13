from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import select
import socket
from typing import Any

from bridge.user_plane.coordinator import PacketCoordinator
from bridge.user_plane.frame_port import TunTapFramePort
from bridge.user_plane.gate import FrameGate
from bridge.user_plane.gtpu import FlowBinding, FlowClassifier
from bridge.user_plane.kpi import PacketKpiCollector
from bridge.user_plane.protocol import (
    Message,
    MessageType,
    StreamDecoder,
    encode_message,
)


@dataclass(frozen=True, slots=True)
class GateRuntimeConfig:
    gnb_tap: str
    upf_tap: str
    socket_path: str
    authorization_socket: str
    max_pending_packets: int
    max_pending_bytes: int
    fail_closed: bool
    gnb_ips: tuple[str, ...]
    upf_ips: tuple[str, ...]
    bindings: tuple[FlowBinding, ...]
    virtual_expiry_us_by_flow: dict[str, int]
    event_log: str | None = None
    kpi_log: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GateRuntimeConfig":
        flow_rows = payload.get("flows", [])
        if not isinstance(flow_rows, list):
            raise ValueError("flows must be a list")
        bindings: list[FlowBinding] = []
        expiry: dict[str, int] = {}
        for row in flow_rows:
            if not isinstance(row, dict):
                raise ValueError("each flow must be an object")
            flow_id = str(row["flow_id"])
            if flow_id in expiry:
                raise ValueError(f"duplicate flow_id {flow_id}")
            expiry_us = int(row.get("virtual_expiry_us", 1_000_000))
            if expiry_us <= 0:
                raise ValueError(f"flow {flow_id} virtual_expiry_us must be positive")
            expiry[flow_id] = expiry_us
            bindings.append(
                FlowBinding(
                    flow_id=flow_id,
                    ue_ip=str(row["ue_ip"]),
                    qfi=(int(row["qfi"]) if row.get("qfi") is not None else None),
                    inner_protocol=(
                        int(row["inner_protocol"])
                        if row.get("inner_protocol") is not None
                        else None
                    ),
                    ue_port=(
                        int(row["ue_port"]) if row.get("ue_port") is not None else None
                    ),
                    remote_port=(
                        int(row["remote_port"])
                        if row.get("remote_port") is not None
                        else None
                    ),
                )
            )
        max_packets = int(payload.get("max_pending_packets", 8192))
        max_bytes = int(payload.get("max_pending_bytes", 64 * 1024 * 1024))
        if max_packets <= 0 or max_bytes <= 0:
            raise ValueError("pending capacities must be positive")
        gnb_ips = tuple(str(value) for value in payload.get("gnb_ips", ()))
        upf_ips = tuple(str(value) for value in payload.get("upf_ips", ()))
        if not gnb_ips or not upf_ips:
            raise ValueError("gnb_ips and upf_ips must not be empty")
        return cls(
            gnb_tap=str(payload["gnb_tap"]),
            upf_tap=str(payload["upf_tap"]),
            socket_path=str(payload["socket_path"]),
            authorization_socket=str(
                payload.get("authorization_socket", f"{payload['socket_path']}.agents")
            ),
            max_pending_packets=max_packets,
            max_pending_bytes=max_bytes,
            fail_closed=bool(payload.get("fail_closed", True)),
            gnb_ips=gnb_ips,
            upf_ips=upf_ips,
            bindings=tuple(bindings),
            virtual_expiry_us_by_flow=expiry,
            event_log=(
                str(payload["event_log"]) if payload.get("event_log") else None
            ),
            kpi_log=str(payload["kpi_log"]) if payload.get("kpi_log") else None,
        )


class GateRuntime:
    def __init__(self, config: GateRuntimeConfig) -> None:
        self.config = config
        self.coordinator = PacketCoordinator(
            max_pending_packets=config.max_pending_packets,
            max_pending_bytes=config.max_pending_bytes,
            fail_closed=config.fail_closed,
        )
        self.kpi = PacketKpiCollector()
        self.ports = {
            "gnb": TunTapFramePort(config.gnb_tap),
            "upf": TunTapFramePort(config.upf_tap),
        }
        self.peer: socket.socket | None = None
        self.agent_peers: list[socket.socket] = []
        self.current_epoch_id: int | None = None
        self.current_ns3_time_us: int | None = None
        self.gate = FrameGate(
            classifier=FlowClassifier(
                config.bindings,
                gnb_ips=config.gnb_ips,
                upf_ips=config.upf_ips,
            ),
            coordinator=self.coordinator,
            kpi=self.kpi,
            ports=self.ports,
            peer_send=self._send_peer,
            virtual_expiry_us_by_flow=config.virtual_expiry_us_by_flow,
        )

    def run(self) -> None:
        socket_path = Path(self.config.socket_path)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        socket_path.unlink(missing_ok=True)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        authorization_path = Path(self.config.authorization_socket)
        authorization_path.parent.mkdir(parents=True, exist_ok=True)
        authorization_path.unlink(missing_ok=True)
        authorization_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        authorization_listener.bind(str(authorization_path))
        authorization_listener.listen(16)
        decoder = StreamDecoder()
        try:
            while True:
                readers: list[Any] = [
                    listener,
                    authorization_listener,
                    *self.ports.values(),
                ]
                if self.peer is not None:
                    readers.append(self.peer)
                ready, _, _ = select.select(readers, [], [])
                for source in ready:
                    if source is listener:
                        peer, _ = listener.accept()
                        if self.peer is not None:
                            peer.close()
                            continue
                        self.peer = peer
                        decoder = StreamDecoder()
                    elif source is authorization_listener:
                        agent, _ = authorization_listener.accept()
                        self.agent_peers.append(agent)
                    elif source is self.peer:
                        data = self.peer.recv(65536)
                        if not data:
                            self._disconnect_peer()
                            continue
                        for message in decoder.feed(data):
                            self._handle_peer_message(message)
                    else:
                        ingress = "gnb" if source is self.ports["gnb"] else "upf"
                        frame = source.read()
                        if (
                            self.peer is None
                            or self.current_epoch_id is None
                            or self.current_ns3_time_us is None
                        ):
                            continue
                        self.gate.capture(
                            frame=frame,
                            ingress_port=ingress,
                            epoch_id=self.current_epoch_id,
                            ns3_time_us=self.current_ns3_time_us,
                        )
        finally:
            if self.peer is not None:
                self.peer.close()
            for agent in self.agent_peers:
                agent.close()
            listener.close()
            authorization_listener.close()
            for port in self.ports.values():
                port.close()
            socket_path.unlink(missing_ok=True)
            authorization_path.unlink(missing_ok=True)

    def _handle_peer_message(self, message: Message) -> None:
        if message.message_type is MessageType.EPOCH_START:
            expected_epoch = int(message.payload["epoch_id"])
            ns3_time_us = int(message.payload["ns3_time_us"])
            actual_epoch = self.coordinator.begin_epoch(start_ns3_us=ns3_time_us)
            if actual_epoch != expected_epoch:
                raise RuntimeError(
                    f"ns-3 epoch {expected_epoch} does not match gate epoch {actual_epoch}"
                )
            self.current_epoch_id = actual_epoch
            self.current_ns3_time_us = ns3_time_us
            return
        if message.message_type in {
            MessageType.PACKET_DELIVER,
            MessageType.PACKET_DROP,
        }:
            self.current_ns3_time_us = int(message.payload["ns3_time_us"])
            self.gate.handle_peer_message(message)
            return
        if message.message_type is MessageType.TICK_COMPLETE:
            epoch_id = int(message.payload["epoch_id"])
            end_ns3_us = int(message.payload["ns3_time_us"])
            epoch = self.coordinator.epoch(epoch_id)
            summary = self.kpi.complete_epoch(
                epoch_id=epoch_id,
                start_ns3_us=epoch.start_ns3_us,
                end_ns3_us=end_ns3_us,
            )
            self._append_json(self.config.kpi_log, asdict(summary))
            for event in self.kpi.events:
                if event.epoch_id == epoch_id:
                    self._append_json(self.config.event_log, asdict(event))
            self.current_ns3_time_us = end_ns3_us
            return
        if message.message_type is MessageType.HELLO:
            return
        if message.message_type is MessageType.AUTHORIZE_SEND:
            encoded = encode_message(message)
            live_agents: list[socket.socket] = []
            for agent in self.agent_peers:
                try:
                    agent.sendall(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    agent.close()
                else:
                    live_agents.append(agent)
            self.agent_peers = live_agents
            return
        raise ValueError(f"unsupported peer message {message.message_type.name}")

    def _send_peer(self, message: Message) -> None:
        if self.peer is None:
            raise ConnectionError("ns-3 peer is not connected")
        self.peer.sendall(encode_message(message))

    def _disconnect_peer(self) -> None:
        if self.peer is None:
            return
        self.peer.close()
        self.peer = None
        if self.current_ns3_time_us is not None:
            self.gate.peer_disconnected(ns3_time_us=self.current_ns3_time_us)
        self.current_epoch_id = None
        self.current_ns3_time_us = None

    @staticmethod
    def _append_json(path: str | None, payload: dict[str, Any]) -> None:
        if path is None:
            return
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the GTP-U-aware NR frame gate")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("gate config root must be an object")
    GateRuntime(GateRuntimeConfig.from_dict(payload)).run()
    return 0
