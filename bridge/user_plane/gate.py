from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping

from bridge.user_plane.coordinator import (
    CapacityError,
    PacketCoordinator,
)
from bridge.user_plane.frame_port import FramePort
from bridge.user_plane.gtpu import DecisionKind, FlowClassifier
from bridge.user_plane.kpi import PacketKpiCollector
from bridge.user_plane.protocol import Message, MessageType


@dataclass(slots=True)
class GateStats:
    bypassed: int = 0
    captured: int = 0
    released: int = 0
    dropped: int = 0
    unmapped: int = 0
    malformed: int = 0
    capacity_drops: int = 0


class FrameGate:
    def __init__(
        self,
        *,
        classifier: FlowClassifier,
        coordinator: PacketCoordinator,
        kpi: PacketKpiCollector,
        ports: Mapping[str, FramePort],
        peer_send: Callable[[Message], object],
        virtual_expiry_us_by_flow: Mapping[str, int],
    ) -> None:
        if set(ports) != {"gnb", "upf"}:
            raise ValueError("ports must contain exactly 'gnb' and 'upf'")
        self.classifier = classifier
        self.coordinator = coordinator
        self.kpi = kpi
        self.ports = dict(ports)
        self.peer_send = peer_send
        self.virtual_expiry_us_by_flow = dict(virtual_expiry_us_by_flow)
        self.stats = GateStats()
        self._next_sequence = 1
        self._egress_by_packet: dict[int, str] = {}

    def capture(
        self,
        *,
        frame: bytes,
        ingress_port: str,
        epoch_id: int,
        ns3_time_us: int,
    ) -> int | None:
        egress_port = self._egress(ingress_port)
        decision = self.classifier.classify(frame)
        if decision.kind is DecisionKind.CONTROL_BYPASS:
            self._write_frame(egress_port, frame)
            self.stats.bypassed += 1
            return None
        if decision.kind is DecisionKind.UNMAPPED:
            self.stats.unmapped += 1
            return None
        if decision.kind is DecisionKind.MALFORMED:
            self.stats.malformed += 1
            return None
        if decision.flow_id is None or decision.direction is None or decision.packet is None:
            raise RuntimeError("managed classification is missing packet metadata")
        expiry_us = self.virtual_expiry_us_by_flow.get(decision.flow_id)
        if expiry_us is None:
            self.stats.unmapped += 1
            return None
        try:
            record = self.coordinator.capture(
                frame=frame,
                flow_id=decision.flow_id,
                direction=decision.direction.value,
                epoch_id=epoch_id,
                enqueue_ns3_us=ns3_time_us,
                virtual_expiry_us=expiry_us,
            )
        except CapacityError:
            self.stats.capacity_drops += 1
            return None
        self._egress_by_packet[record.packet_id] = egress_port
        shadow_size = decision.packet.inner_size_bytes or len(frame)
        self.coordinator.mark_submitted(record.packet_id)
        self.kpi.submitted(
            packet_id=record.packet_id,
            epoch_id=epoch_id,
            flow_id=decision.flow_id,
            direction=decision.direction.value,
            enqueue_ns3_us=ns3_time_us,
            size_bytes=shadow_size,
        )
        self.peer_send(
            Message(
                MessageType.PACKET_ENQUEUE,
                sequence=self._take_sequence(),
                payload={
                    "packet_id": record.packet_id,
                    "epoch_id": epoch_id,
                    "flow_id": decision.flow_id,
                    "direction": decision.direction.value,
                    "size_bytes": shadow_size,
                    "enqueue_ns3_us": ns3_time_us,
                    "virtual_expiry_us": expiry_us,
                    "teid": decision.packet.teid,
                    "qfi": decision.packet.qfi,
                },
            )
        )
        self.stats.captured += 1
        return record.packet_id

    def handle_peer_message(self, message: Message) -> None:
        payload = message.payload
        if message.message_type is MessageType.PACKET_DELIVER:
            packet_id = int(payload["packet_id"])
            action = self.coordinator.mark_delivered(
                packet_id,
                epoch_id=int(payload["epoch_id"]),
                ns3_time_us=int(payload["ns3_time_us"]),
            )
            self.kpi.delivered(
                packet_id=packet_id,
                deliver_ns3_us=int(payload["ns3_time_us"]),
            )
            egress_port = self._egress_by_packet.pop(packet_id)
            self._write_frame(egress_port, action.frame)
            self.coordinator.complete_action(packet_id)
            self.stats.released += 1
            return
        if message.message_type is MessageType.PACKET_DROP:
            packet_id = int(payload["packet_id"])
            reason = str(payload.get("reason", "ns3-drop"))
            self.coordinator.mark_dropped(
                packet_id,
                epoch_id=int(payload["epoch_id"]),
                ns3_time_us=int(payload["ns3_time_us"]),
                reason=reason,
            )
            self.kpi.dropped(
                packet_id=packet_id,
                drop_ns3_us=int(payload["ns3_time_us"]),
                reason=reason,
            )
            self._egress_by_packet.pop(packet_id)
            self.coordinator.complete_action(packet_id)
            self.stats.dropped += 1
            return
        raise ValueError(
            f"frame gate cannot handle message type {message.message_type.name}"
        )

    def expire(self, *, ns3_time_us: int) -> None:
        for action in self.coordinator.expire(ns3_now_us=ns3_time_us):
            record = self.coordinator.packet(action.packet_id)
            self.kpi.dropped(
                packet_id=action.packet_id,
                drop_ns3_us=ns3_time_us,
                reason=action.reason or "virtual-expiry",
            )
            self._egress_by_packet.pop(action.packet_id)
            self.coordinator.complete_action(action.packet_id)
            self.stats.dropped += 1

    def peer_disconnected(self, *, ns3_time_us: int) -> None:
        for action in self.coordinator.peer_disconnected(ns3_time_us=ns3_time_us):
            self.kpi.dropped(
                packet_id=action.packet_id,
                drop_ns3_us=ns3_time_us,
                reason=action.reason or "ns3-peer-disconnected",
            )
            self._egress_by_packet.pop(action.packet_id)
            self.coordinator.complete_action(action.packet_id)
            self.stats.dropped += 1

    def _take_sequence(self) -> int:
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def _egress(self, ingress_port: str) -> str:
        if ingress_port == "gnb":
            return "upf"
        if ingress_port == "upf":
            return "gnb"
        raise ValueError(f"unknown ingress port {ingress_port}")

    def _write_frame(self, port_name: str, frame: bytes) -> None:
        written = self.ports[port_name].write(frame)
        if written != len(frame):
            raise OSError(
                f"short frame write on {port_name}: expected {len(frame)}, wrote {written}"
            )
