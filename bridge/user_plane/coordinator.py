from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class CoordinatorError(RuntimeError):
    pass


class CapacityError(CoordinatorError):
    pass


class PacketState(str, Enum):
    CAPTURED = "captured"
    SUBMITTED = "submitted"
    DELIVERED = "delivered"
    DROPPED = "dropped"
    RELEASED = "released"
    DISCARDED = "discarded"


class ActionKind(str, Enum):
    RELEASE = "release"
    DISCARD = "discard"


@dataclass(frozen=True, slots=True)
class GateAction:
    kind: ActionKind
    packet_id: int
    frame: bytes
    reason: str | None = None


@dataclass(slots=True)
class PacketRecord:
    packet_id: int
    epoch_id: int
    flow_id: str
    direction: str
    frame: bytes
    enqueue_ns3_us: int
    state: PacketState = PacketState.CAPTURED
    terminal_ns3_us: int | None = None
    terminal_reason: str | None = None
    history: list[PacketState] = field(default_factory=lambda: [PacketState.CAPTURED])


@dataclass(slots=True)
class EpochRecord:
    epoch_id: int
    start_ns3_us: int
    packet_ids: list[int] = field(default_factory=list)


_ALLOWED_TRANSITIONS = {
    PacketState.CAPTURED: {
        PacketState.SUBMITTED,
        PacketState.DROPPED,
    },
    PacketState.SUBMITTED: {
        PacketState.DELIVERED,
        PacketState.DROPPED,
    },
    PacketState.DELIVERED: {PacketState.RELEASED},
    PacketState.DROPPED: {PacketState.DISCARDED},
    PacketState.RELEASED: set(),
    PacketState.DISCARDED: set(),
}
_NS3_TERMINAL = {
    PacketState.DELIVERED,
    PacketState.DROPPED,
    PacketState.RELEASED,
    PacketState.DISCARDED,
}


class PacketCoordinator:
    def __init__(
        self,
        *,
        max_pending_packets: int,
        max_pending_bytes: int,
    ) -> None:
        if max_pending_packets <= 0 or max_pending_bytes <= 0:
            raise ValueError("pending packet and byte limits must be positive")
        self.max_pending_packets = max_pending_packets
        self.max_pending_bytes = max_pending_bytes
        self._next_packet_id = 1
        self._next_epoch_id = 1
        self._packets: dict[int, PacketRecord] = {}
        self._epochs: dict[int, EpochRecord] = {}
        self._pending_packets = 0
        self._pending_bytes = 0

    @property
    def pending_packets(self) -> int:
        return self._pending_packets

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    def begin_epoch(self, *, start_ns3_us: int) -> int:
        if start_ns3_us < 0:
            raise CoordinatorError("epoch start time must not be negative")
        epoch_id = self._next_epoch_id
        self._next_epoch_id += 1
        self._epochs[epoch_id] = EpochRecord(epoch_id, start_ns3_us)
        return epoch_id

    def capture(
        self,
        *,
        frame: bytes,
        flow_id: str,
        direction: str,
        epoch_id: int,
        enqueue_ns3_us: int,
    ) -> PacketRecord:
        epoch = self._epochs.get(epoch_id)
        if epoch is None:
            raise CoordinatorError(f"unknown epoch {epoch_id}")
        if enqueue_ns3_us < epoch.start_ns3_us:
            raise CoordinatorError("packet enqueue time precedes epoch start")
        if self._pending_packets >= self.max_pending_packets:
            raise CapacityError("pending packet capacity exceeded")
        if self._pending_bytes + len(frame) > self.max_pending_bytes:
            raise CapacityError("pending byte capacity exceeded")

        packet_id = self._next_packet_id
        self._next_packet_id += 1
        record = PacketRecord(
            packet_id=packet_id,
            epoch_id=epoch_id,
            flow_id=flow_id,
            direction=direction,
            frame=bytes(frame),
            enqueue_ns3_us=enqueue_ns3_us,
        )
        self._packets[packet_id] = record
        epoch.packet_ids.append(packet_id)
        self._pending_packets += 1
        self._pending_bytes += len(record.frame)
        return record

    def packet(self, packet_id: int) -> PacketRecord:
        try:
            return self._packets[packet_id]
        except KeyError as exc:
            raise CoordinatorError(f"unknown packet {packet_id}") from exc

    def epoch(self, epoch_id: int) -> EpochRecord:
        try:
            return self._epochs[epoch_id]
        except KeyError as exc:
            raise CoordinatorError(f"unknown epoch {epoch_id}") from exc

    def mark_submitted(self, packet_id: int) -> PacketRecord:
        record = self.packet(packet_id)
        self._transition(record, PacketState.SUBMITTED)
        return record

    def mark_delivered(
        self, packet_id: int, *, epoch_id: int, ns3_time_us: int
    ) -> GateAction:
        record = self._result_record(packet_id, epoch_id, ns3_time_us)
        self._transition(record, PacketState.DELIVERED)
        record.terminal_ns3_us = ns3_time_us
        return GateAction(ActionKind.RELEASE, packet_id, record.frame)

    def mark_dropped(
        self,
        packet_id: int,
        *,
        epoch_id: int,
        ns3_time_us: int,
        reason: str,
    ) -> GateAction:
        record = self._result_record(packet_id, epoch_id, ns3_time_us)
        self._transition(record, PacketState.DROPPED)
        record.terminal_ns3_us = ns3_time_us
        record.terminal_reason = reason
        return GateAction(ActionKind.DISCARD, packet_id, record.frame, reason)

    def complete_action(self, packet_id: int) -> PacketRecord:
        record = self.packet(packet_id)
        if record.state is PacketState.DELIVERED:
            self._transition(record, PacketState.RELEASED)
        elif record.state is PacketState.DROPPED:
            self._transition(record, PacketState.DISCARDED)
        else:
            raise CoordinatorError(
                f"packet {packet_id} cannot complete action from state {record.state.value}"
            )
        self._pending_packets -= 1
        self._pending_bytes -= len(record.frame)
        return record

    def is_epoch_complete(self, epoch_id: int) -> bool:
        epoch = self.epoch(epoch_id)
        return all(self._packets[packet_id].state in _NS3_TERMINAL for packet_id in epoch.packet_ids)

    def peer_disconnected(self, *, ns3_time_us: int) -> list[GateAction]:
        actions: list[GateAction] = []
        for record in self._packets.values():
            if record.state not in {PacketState.CAPTURED, PacketState.SUBMITTED}:
                continue
            self._transition(record, PacketState.DROPPED)
            record.terminal_ns3_us = ns3_time_us
            record.terminal_reason = "ns3-peer-disconnected"
            actions.append(
                GateAction(
                    ActionKind.DISCARD,
                    record.packet_id,
                    record.frame,
                    record.terminal_reason,
                )
            )
        return actions

    def _result_record(
        self, packet_id: int, epoch_id: int, ns3_time_us: int
    ) -> PacketRecord:
        record = self.packet(packet_id)
        if record.epoch_id != epoch_id:
            raise CoordinatorError(
                f"packet {packet_id} belongs to epoch {record.epoch_id}, not {epoch_id}"
            )
        if ns3_time_us < record.enqueue_ns3_us:
            raise CoordinatorError("terminal ns-3 time precedes enqueue time")
        if record.state is not PacketState.SUBMITTED:
            raise CoordinatorError(
                f"packet {packet_id} result is invalid in state {record.state.value}"
            )
        return record

    @staticmethod
    def _transition(record: PacketRecord, next_state: PacketState) -> None:
        if next_state not in _ALLOWED_TRANSITIONS[record.state]:
            raise CoordinatorError(
                f"packet {record.packet_id} cannot transition from "
                f"{record.state.value} to {next_state.value}"
            )
        record.state = next_state
        record.history.append(next_state)
