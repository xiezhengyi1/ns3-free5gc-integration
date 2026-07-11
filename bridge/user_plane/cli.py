from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import ipaddress
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
from bridge.user_plane.routing import EndpointLink, EndpointRouter


def _reset_output_log(path: str | None) -> None:
    if path is None:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("", encoding="utf-8")


@dataclass(frozen=True, slots=True)
class GateLinkConfig:
    link_id: str
    gnb_name: str
    upf_name: str
    gnb_tap: str
    upf_tap: str
    gnb_ip: str
    upf_ip: str

    def __post_init__(self) -> None:
        for role, address in (("gNB", self.gnb_ip), ("UPF", self.upf_ip)):
            try:
                ipaddress.IPv4Address(address)
            except ipaddress.AddressValueError as exc:
                raise ValueError(f"N3 {role} endpoint must be a valid IPv4 address: {address}") from exc

    @property
    def gnb_port(self) -> str:
        return f"gnb:{self.gnb_name}"

    @property
    def upf_port(self) -> str:
        return f"upf:{self.upf_name}"


@dataclass(frozen=True, slots=True)
class GateRuntimeConfig:
    links: tuple[GateLinkConfig, ...]
    socket_path: str
    authorization_socket: str
    max_pending_packets: int
    max_pending_bytes: int
    bindings: tuple[FlowBinding, ...]
    virtual_expiry_us_by_flow: dict[str, int]
    event_log: str | None = None
    kpi_log: str | None = None

    @property
    def gnb_ips(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(link.gnb_ip for link in self.links))

    @property
    def upf_ips(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(link.upf_ip for link in self.links))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GateRuntimeConfig":
        if "fail_closed" in payload:
            raise ValueError("fail_closed was removed; the gate is always fail-closed")
        links = cls._parse_links(payload)
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
                    gnb_ip=str(row["gnb_ip"]),
                    upf_ip=str(row["upf_ip"]),
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
        return cls(
            links=links,
            socket_path=str(payload["socket_path"]),
            authorization_socket=str(
                payload.get("authorization_socket", f"{payload['socket_path']}.agents")
            ),
            max_pending_packets=max_packets,
            max_pending_bytes=max_bytes,
            bindings=tuple(bindings),
            virtual_expiry_us_by_flow=expiry,
            event_log=(
                str(payload["event_log"]) if payload.get("event_log") else None
            ),
            kpi_log=str(payload["kpi_log"]) if payload.get("kpi_log") else None,
        )

    @staticmethod
    def _parse_links(payload: dict[str, Any]) -> tuple[GateLinkConfig, ...]:
        removed_keys = {"gnb_tap", "upf_tap", "gnb_ips", "upf_ips"} & payload.keys()
        if removed_keys:
            names = ", ".join(sorted(removed_keys))
            raise ValueError(f"legacy top-level gate fields were removed: {names}; use links[]")
        raw_links = payload.get("links")
        if not isinstance(raw_links, list) or not raw_links:
            raise ValueError("links must be a non-empty list")

        links: list[GateLinkConfig] = []
        endpoint_values: dict[tuple[str, str], tuple[str, str]] = {}
        tap_owners: dict[str, tuple[str, str]] = {}
        seen_link_ids: set[str] = set()
        seen_pairs: set[tuple[str, str]] = set()
        for index, row in enumerate(raw_links, start=1):
            if not isinstance(row, dict):
                raise ValueError("each N3 link must be an object")
            link = GateLinkConfig(
                link_id=str(row.get("link_id", f"n3-{index}")),
                gnb_name=str(row["gnb_name"]),
                upf_name=str(row["upf_name"]),
                gnb_tap=str(row["gnb_tap"]),
                upf_tap=str(row["upf_tap"]),
                gnb_ip=str(row["gnb_ip"]),
                upf_ip=str(row["upf_ip"]),
            )
            if not all(
                (
                    link.link_id,
                    link.gnb_name,
                    link.upf_name,
                    link.gnb_tap,
                    link.upf_tap,
                    link.gnb_ip,
                    link.upf_ip,
                )
            ):
                raise ValueError("N3 link fields must not be empty")
            if link.link_id in seen_link_ids:
                raise ValueError(f"duplicate N3 link_id {link.link_id}")
            seen_link_ids.add(link.link_id)
            pair = (link.gnb_name, link.upf_name)
            if pair in seen_pairs:
                raise ValueError(f"duplicate N3 link {link.gnb_name}->{link.upf_name}")
            seen_pairs.add(pair)
            for role, name, tap, address in (
                ("gnb", link.gnb_name, link.gnb_tap, link.gnb_ip),
                ("upf", link.upf_name, link.upf_tap, link.upf_ip),
            ):
                endpoint = (role, name)
                value = (tap, address)
                previous = endpoint_values.setdefault(endpoint, value)
                if previous != value:
                    raise ValueError(f"N3 endpoint {role}:{name} has conflicting tap or IP")
                owner = tap_owners.setdefault(tap, endpoint)
                if owner != endpoint:
                    raise ValueError(f"tap {tap} is assigned to multiple N3 endpoints")
            links.append(link)
        return tuple(links)


class AuthorizationRelay:
    """Retain current-epoch authorizations until an agent can receive them."""

    def __init__(self) -> None:
        self._peers: list[Any] = []
        self._pending: dict[int, Message] = {}

    def add_peer(self, peer: Any) -> None:
        try:
            for message in self._pending.values():
                peer.sendall(encode_message(message))
        except (BrokenPipeError, ConnectionResetError, OSError):
            peer.close()
            return
        self._peers.append(peer)

    def publish(self, message: Message) -> None:
        if message.message_type is not MessageType.AUTHORIZE_SEND:
            raise ValueError("authorization relay accepts only AUTHORIZE_SEND")
        authorization_id = int(message.payload["authorization_id"])
        previous = self._pending.get(authorization_id)
        if previous is not None and previous != message:
            raise ValueError(f"authorization_id {authorization_id} was reused")
        self._pending[authorization_id] = message
        self._broadcast(encode_message(message))

    def complete_epoch(self, epoch_id: int) -> None:
        self._pending = {
            authorization_id: message
            for authorization_id, message in self._pending.items()
            if int(message.payload["epoch_id"]) != epoch_id
        }

    def close(self) -> None:
        for peer in self._peers:
            peer.close()
        self._peers.clear()

    def _broadcast(self, encoded: bytes) -> None:
        live_peers: list[Any] = []
        for peer in self._peers:
            try:
                peer.sendall(encoded)
            except (BrokenPipeError, ConnectionResetError, OSError):
                peer.close()
            else:
                live_peers.append(peer)
        self._peers = live_peers


class GateRuntime:
    def __init__(self, config: GateRuntimeConfig) -> None:
        self.config = config
        self.coordinator = PacketCoordinator(
            max_pending_packets=config.max_pending_packets,
            max_pending_bytes=config.max_pending_bytes,
        )
        self.kpi = PacketKpiCollector()
        tap_by_port: dict[str, str] = {}
        endpoint_links: list[EndpointLink] = []
        for link in config.links:
            tap_by_port[link.gnb_port] = link.gnb_tap
            tap_by_port[link.upf_port] = link.upf_tap
            endpoint_links.append(
                EndpointLink(
                    gnb_port=link.gnb_port,
                    upf_port=link.upf_port,
                    gnb_ip=link.gnb_ip,
                    upf_ip=link.upf_ip,
                )
            )
        self.ports = {
            port_name: TunTapFramePort(tap_name)
            for port_name, tap_name in tap_by_port.items()
        }
        self._port_name_by_identity = {
            id(port): port_name for port_name, port in self.ports.items()
        }
        self.router = EndpointRouter(endpoint_links)
        self.peer: socket.socket | None = None
        self.authorization_relay = AuthorizationRelay()
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
            egress_resolver=self.router.route,
        )

    def run(self) -> None:
        _reset_output_log(self.config.event_log)
        _reset_output_log(self.config.kpi_log)
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
                        self.authorization_relay.add_peer(agent)
                    elif source is self.peer:
                        data = self.peer.recv(65536)
                        if not data:
                            self._disconnect_peer()
                            continue
                        for message in decoder.feed(data):
                            self._handle_peer_message(message)
                    else:
                        ingress = self._port_name_by_identity[id(source)]
                        frame = source.read()
                        if (
                            self.peer is None
                            or self.current_epoch_id is None
                            or self.current_ns3_time_us is None
                        ):
                            self.gate.capture_pre_epoch(
                                frame=frame,
                                ingress_port=ingress,
                            )
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
            self.authorization_relay.close()
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
            self.authorization_relay.complete_epoch(epoch_id)
            self.current_ns3_time_us = end_ns3_us
            return
        if message.message_type is MessageType.HELLO:
            return
        if message.message_type is MessageType.AUTHORIZE_SEND:
            self.authorization_relay.publish(message)
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
