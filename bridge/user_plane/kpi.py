from __future__ import annotations

from dataclasses import dataclass
import math


class KpiError(RuntimeError):
    pass


@dataclass(slots=True)
class PacketEvent:
    packet_id: int
    epoch_id: int
    flow_id: str
    direction: str
    enqueue_ns3_us: int
    size_bytes: int
    warmup: bool
    terminal_kind: str | None = None
    terminal_ns3_us: int | None = None
    reason: str | None = None
    delay_us: int | None = None
    ipdv_us: int | None = None


@dataclass(frozen=True, slots=True)
class TerminalMetric:
    packet_id: int
    delay_us: int | None
    ipdv_us: int | None


@dataclass(frozen=True, slots=True)
class TickKpi:
    epoch_id: int
    start_ns3_us: int
    end_ns3_us: int
    submitted_packets: int
    delivered_packets: int
    dropped_packets: int
    delivered_bytes: int
    throughput_mbps: float
    delay_p50_us: int | None
    delay_p95_us: int | None
    delay_p99_us: int | None
    ipdv_p50_us: int | None
    ipdv_p95_us: int | None
    ipdv_p99_us: int | None


def _nearest_rank(values: list[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile / 100.0 * len(ordered)))
    return ordered[rank - 1]


class PacketKpiCollector:
    def __init__(self) -> None:
        self._events: dict[int, PacketEvent] = {}
        self._previous_delay: dict[tuple[str, str], int] = {}

    @property
    def events(self) -> tuple[PacketEvent, ...]:
        return tuple(self._events.values())

    def submitted(
        self,
        *,
        packet_id: int,
        epoch_id: int,
        flow_id: str,
        direction: str,
        enqueue_ns3_us: int,
        size_bytes: int,
        warmup: bool = False,
    ) -> PacketEvent:
        if packet_id in self._events:
            raise KpiError(f"packet {packet_id} was already submitted")
        if enqueue_ns3_us < 0:
            raise KpiError("enqueue timestamp must not be negative")
        if size_bytes < 0:
            raise KpiError("packet size must not be negative")
        event = PacketEvent(
            packet_id=packet_id,
            epoch_id=epoch_id,
            flow_id=flow_id,
            direction=direction,
            enqueue_ns3_us=enqueue_ns3_us,
            size_bytes=size_bytes,
            warmup=warmup,
        )
        self._events[packet_id] = event
        return event

    def delivered(self, *, packet_id: int, deliver_ns3_us: int) -> TerminalMetric:
        event = self._terminal_event(packet_id, deliver_ns3_us)
        delay = deliver_ns3_us - event.enqueue_ns3_us
        ipdv: int | None = None
        if not event.warmup:
            key = (event.flow_id, event.direction)
            previous = self._previous_delay.get(key)
            if previous is not None:
                ipdv = delay - previous
            self._previous_delay[key] = delay
            event.delay_us = delay
            event.ipdv_us = ipdv
        event.terminal_kind = "delivered"
        event.terminal_ns3_us = deliver_ns3_us
        return TerminalMetric(packet_id, event.delay_us, event.ipdv_us)

    def dropped(
        self, *, packet_id: int, drop_ns3_us: int, reason: str
    ) -> TerminalMetric:
        event = self._terminal_event(packet_id, drop_ns3_us)
        event.terminal_kind = "dropped"
        event.terminal_ns3_us = drop_ns3_us
        event.reason = reason
        return TerminalMetric(packet_id, None, None)

    def complete_epoch(
        self, *, epoch_id: int, start_ns3_us: int, end_ns3_us: int
    ) -> TickKpi:
        if start_ns3_us < 0 or end_ns3_us <= start_ns3_us:
            raise KpiError("epoch timestamps must define a positive interval")
        events = [event for event in self._events.values() if event.epoch_id == epoch_id]
        pending = [event.packet_id for event in events if event.terminal_kind is None]
        if pending:
            raise KpiError(f"epoch {epoch_id} packets are not terminal: {pending}")
        delivered = [event for event in events if event.terminal_kind == "delivered"]
        dropped = [event for event in events if event.terminal_kind == "dropped"]
        if len(events) != len(delivered) + len(dropped):
            raise KpiError("packet conservation invariant failed")

        delays = [event.delay_us for event in delivered if event.delay_us is not None]
        ipdvs = [event.ipdv_us for event in delivered if event.ipdv_us is not None]
        delivered_bytes = sum(event.size_bytes for event in delivered)
        duration_us = end_ns3_us - start_ns3_us
        return TickKpi(
            epoch_id=epoch_id,
            start_ns3_us=start_ns3_us,
            end_ns3_us=end_ns3_us,
            submitted_packets=len(events),
            delivered_packets=len(delivered),
            dropped_packets=len(dropped),
            delivered_bytes=delivered_bytes,
            throughput_mbps=delivered_bytes * 8.0 / duration_us,
            delay_p50_us=_nearest_rank(delays, 50),
            delay_p95_us=_nearest_rank(delays, 95),
            delay_p99_us=_nearest_rank(delays, 99),
            ipdv_p50_us=_nearest_rank(ipdvs, 50),
            ipdv_p95_us=_nearest_rank(ipdvs, 95),
            ipdv_p99_us=_nearest_rank(ipdvs, 99),
        )

    def _terminal_event(self, packet_id: int, terminal_ns3_us: int) -> PacketEvent:
        try:
            event = self._events[packet_id]
        except KeyError as exc:
            raise KpiError(f"unknown packet {packet_id}") from exc
        if event.terminal_kind is not None:
            raise KpiError(f"packet {packet_id} already has a terminal event")
        if terminal_ns3_us < event.enqueue_ns3_us:
            raise KpiError(
                f"packet {packet_id} terminal timestamp precedes enqueue timestamp"
            )
        return event
