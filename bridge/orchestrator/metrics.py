from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sys
import time
from typing import Any


class MetricsValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ObservedPacketEvent:
    packet_id: int
    epoch_id: int
    flow_id: str
    direction: str
    enqueue_ns3_us: int
    terminal_kind: str
    terminal_ns3_us: int
    size_bytes: int
    warmup: bool
    delay_us: int | None
    ipdv_us: int | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class ObservedEpochKpi:
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


@dataclass(frozen=True, slots=True)
class GatedMetricsReport:
    events: tuple[ObservedPacketEvent, ...]
    epochs: tuple[ObservedEpochKpi, ...]

    @property
    def total_submitted_packets(self) -> int:
        return sum(epoch.submitted_packets for epoch in self.epochs)

    @property
    def total_delivered_packets(self) -> int:
        return sum(epoch.delivered_packets for epoch in self.epochs)

    @property
    def total_dropped_packets(self) -> int:
        return sum(epoch.dropped_packets for epoch in self.epochs)


def load_gated_user_plane_metrics(
    event_log: str | Path,
    kpi_log: str | Path,
) -> GatedMetricsReport:
    events = _load_events(Path(event_log))
    epochs = _load_epochs(Path(kpi_log))
    _validate_epoch_coverage(events, epochs)
    _validate_ipdv(events)
    _validate_epoch_kpis(events, epochs)
    return GatedMetricsReport(events=tuple(events), epochs=tuple(epochs))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise MetricsValidationError(f"metrics log does not exist: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise MetricsValidationError(
                    f"{path}:{line_number} contains invalid JSON"
                ) from exc
            if not isinstance(payload, dict):
                raise MetricsValidationError(
                    f"{path}:{line_number} must contain a JSON object"
                )
            rows.append(payload)
    return rows


def _load_events(path: Path) -> list[ObservedPacketEvent]:
    seen_packet_ids: set[int] = set()
    events: list[ObservedPacketEvent] = []
    for row in _load_jsonl(path):
        event = ObservedPacketEvent(
            packet_id=_required_int(row, "packet_id"),
            epoch_id=_required_int(row, "epoch_id"),
            flow_id=_required_str(row, "flow_id"),
            direction=_required_str(row, "direction"),
            enqueue_ns3_us=_required_int(row, "enqueue_ns3_us"),
            terminal_kind=_required_str(row, "terminal_kind"),
            terminal_ns3_us=_required_int(row, "terminal_ns3_us"),
            size_bytes=_required_int(row, "size_bytes"),
            warmup=_required_bool(row, "warmup"),
            delay_us=_optional_int(row.get("delay_us")),
            ipdv_us=_optional_int(row.get("ipdv_us")),
            reason=_optional_str(row.get("reason")),
        )
        if event.packet_id in seen_packet_ids:
            raise MetricsValidationError(f"duplicate packet_id {event.packet_id}")
        seen_packet_ids.add(event.packet_id)
        _validate_event(event)
        events.append(event)
    if not events:
        raise MetricsValidationError(f"no packet events found in {path}")
    packet_ids = [event.packet_id for event in events]
    if packet_ids != list(range(1, len(packet_ids) + 1)):
        raise MetricsValidationError("packet IDs must be contiguous and ordered")
    return events


def _load_epochs(path: Path) -> list[ObservedEpochKpi]:
    seen_epoch_ids: set[int] = set()
    epochs: list[ObservedEpochKpi] = []
    for row in _load_jsonl(path):
        epoch = ObservedEpochKpi(
            epoch_id=_required_int(row, "epoch_id"),
            start_ns3_us=_required_int(row, "start_ns3_us"),
            end_ns3_us=_required_int(row, "end_ns3_us"),
            submitted_packets=_required_int(row, "submitted_packets"),
            delivered_packets=_required_int(row, "delivered_packets"),
            dropped_packets=_required_int(row, "dropped_packets"),
            delivered_bytes=_required_int(row, "delivered_bytes"),
            throughput_mbps=_required_float(row, "throughput_mbps"),
            delay_p50_us=_optional_int(row.get("delay_p50_us")),
            delay_p95_us=_optional_int(row.get("delay_p95_us")),
            delay_p99_us=_optional_int(row.get("delay_p99_us")),
            ipdv_p50_us=_optional_int(row.get("ipdv_p50_us")),
            ipdv_p95_us=_optional_int(row.get("ipdv_p95_us")),
            ipdv_p99_us=_optional_int(row.get("ipdv_p99_us")),
        )
        if epoch.epoch_id in seen_epoch_ids:
            raise MetricsValidationError(f"duplicate epoch_id {epoch.epoch_id}")
        seen_epoch_ids.add(epoch.epoch_id)
        if epoch.start_ns3_us < 0 or epoch.end_ns3_us <= epoch.start_ns3_us:
            raise MetricsValidationError(
                f"epoch {epoch.epoch_id} has invalid ns-3 time bounds"
            )
        if epoch.submitted_packets != epoch.delivered_packets + epoch.dropped_packets:
            raise MetricsValidationError(
                f"epoch {epoch.epoch_id} packet conservation failed"
            )
        if min(
            epoch.submitted_packets,
            epoch.delivered_packets,
            epoch.dropped_packets,
            epoch.delivered_bytes,
        ) < 0:
            raise MetricsValidationError(
                f"epoch {epoch.epoch_id} contains a negative counter"
            )
        if not math.isfinite(epoch.throughput_mbps) or epoch.throughput_mbps < 0:
            raise MetricsValidationError(
                f"epoch {epoch.epoch_id} has invalid throughput_mbps"
            )
        epochs.append(epoch)
    if not epochs:
        raise MetricsValidationError(f"no epoch KPI rows found in {path}")
    if epochs[0].epoch_id != 1:
        raise MetricsValidationError("epoch IDs must start at 1")
    for previous, current in zip(epochs, epochs[1:]):
        if current.epoch_id != previous.epoch_id + 1:
            raise MetricsValidationError("epoch IDs must be contiguous and ordered")
        if current.start_ns3_us < previous.end_ns3_us:
            raise MetricsValidationError("epoch virtual-time ranges overlap")
    return epochs


def _validate_event(event: ObservedPacketEvent) -> None:
    if event.packet_id <= 0 or event.epoch_id <= 0:
        raise MetricsValidationError("packet and epoch IDs must be positive")
    if event.enqueue_ns3_us < 0 or event.terminal_ns3_us < 0:
        raise MetricsValidationError(f"packet {event.packet_id} has negative timestamp")
    if event.size_bytes <= 0:
        raise MetricsValidationError(f"packet {event.packet_id} has non-positive size")
    if event.direction not in {"uplink", "downlink"}:
        raise MetricsValidationError(
            f"packet {event.packet_id} has invalid direction {event.direction}"
        )
    if event.terminal_ns3_us < event.enqueue_ns3_us:
        raise MetricsValidationError(
            f"packet {event.packet_id} terminal timestamp precedes enqueue"
        )
    if event.terminal_kind not in {"delivered", "dropped"}:
        raise MetricsValidationError(
            f"packet {event.packet_id} is not terminal: {event.terminal_kind}"
        )
    if event.terminal_kind == "delivered":
        expected_delay = event.terminal_ns3_us - event.enqueue_ns3_us
        if event.warmup:
            if event.delay_us is not None or event.ipdv_us is not None:
                raise MetricsValidationError(
                    f"warmup packet {event.packet_id} must not publish KPI samples"
                )
        elif event.delay_us != expected_delay:
            raise MetricsValidationError(
                f"packet {event.packet_id} delay_us does not match ns-3 timestamps"
            )
    elif event.delay_us is not None or event.ipdv_us is not None:
        raise MetricsValidationError(
            f"dropped packet {event.packet_id} must not publish delay or IPDV samples"
        )


def _validate_epoch_coverage(
    events: list[ObservedPacketEvent],
    epochs: list[ObservedEpochKpi],
) -> None:
    event_epoch_ids = {event.epoch_id for event in events}
    kpi_epoch_ids = {epoch.epoch_id for epoch in epochs}
    missing_kpis = event_epoch_ids - kpi_epoch_ids
    extra_kpis = kpi_epoch_ids - event_epoch_ids
    if missing_kpis:
        raise MetricsValidationError(
            f"missing KPI rows for epoch(s): {_format_ids(missing_kpis)}"
        )
    if extra_kpis:
        raise MetricsValidationError(
            f"KPI rows without packet events: {_format_ids(extra_kpis)}"
        )


def _validate_ipdv(events: list[ObservedPacketEvent]) -> None:
    previous_delay_by_flow: dict[tuple[str, str], int] = {}
    delivered = [
        event
        for event in events
        if event.terminal_kind == "delivered" and not event.warmup
    ]
    delivered.sort(key=lambda event: (event.terminal_ns3_us, event.packet_id))
    for event in delivered:
        if event.delay_us is None:
            raise MetricsValidationError(
                f"packet {event.packet_id} is missing observed delay_us"
            )
        key = (event.flow_id, event.direction)
        previous_delay = previous_delay_by_flow.get(key)
        expected_ipdv = (
            None if previous_delay is None else event.delay_us - previous_delay
        )
        if event.ipdv_us != expected_ipdv:
            raise MetricsValidationError(
                f"packet {event.packet_id} ipdv_us does not match packet events"
            )
        previous_delay_by_flow[key] = event.delay_us


def _validate_epoch_kpis(
    events: list[ObservedPacketEvent],
    epochs: list[ObservedEpochKpi],
) -> None:
    events_by_epoch: dict[int, list[ObservedPacketEvent]] = {}
    for event in events:
        events_by_epoch.setdefault(event.epoch_id, []).append(event)
    for epoch in epochs:
        expected = _derive_epoch(events_by_epoch.get(epoch.epoch_id, []), epoch)
        _assert_equal(epoch, expected, "submitted_packets")
        _assert_equal(epoch, expected, "delivered_packets")
        _assert_equal(epoch, expected, "dropped_packets")
        _assert_equal(epoch, expected, "delivered_bytes")
        _assert_float_equal(epoch, expected, "throughput_mbps")
        _assert_equal(epoch, expected, "delay_p50_us")
        _assert_equal(epoch, expected, "delay_p95_us")
        _assert_equal(epoch, expected, "delay_p99_us")
        _assert_equal(epoch, expected, "ipdv_p50_us")
        _assert_equal(epoch, expected, "ipdv_p95_us")
        _assert_equal(epoch, expected, "ipdv_p99_us")


def _derive_epoch(
    events: list[ObservedPacketEvent],
    epoch: ObservedEpochKpi,
) -> ObservedEpochKpi:
    for event in events:
        if not epoch.start_ns3_us <= event.enqueue_ns3_us <= epoch.end_ns3_us:
            raise MetricsValidationError(
                f"packet {event.packet_id} enqueue time is outside epoch {epoch.epoch_id}"
            )
        if not epoch.start_ns3_us <= event.terminal_ns3_us <= epoch.end_ns3_us:
            raise MetricsValidationError(
                f"packet {event.packet_id} terminal time is outside epoch {epoch.epoch_id}"
            )
    delivered = [event for event in events if event.terminal_kind == "delivered"]
    dropped = [event for event in events if event.terminal_kind == "dropped"]
    delays = [event.delay_us for event in delivered if event.delay_us is not None]
    ipdvs = [event.ipdv_us for event in delivered if event.ipdv_us is not None]
    delivered_bytes = sum(event.size_bytes for event in delivered)
    duration_us = epoch.end_ns3_us - epoch.start_ns3_us
    return ObservedEpochKpi(
        epoch_id=epoch.epoch_id,
        start_ns3_us=epoch.start_ns3_us,
        end_ns3_us=epoch.end_ns3_us,
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


def _nearest_rank(values: list[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile / 100.0 * len(ordered)))
    return ordered[rank - 1]


def _assert_equal(
    observed: ObservedEpochKpi,
    expected: ObservedEpochKpi,
    field_name: str,
) -> None:
    if getattr(observed, field_name) != getattr(expected, field_name):
        raise MetricsValidationError(
            f"epoch {observed.epoch_id} {field_name} does not match packet events"
        )


def _assert_float_equal(
    observed: ObservedEpochKpi,
    expected: ObservedEpochKpi,
    field_name: str,
) -> None:
    observed_value = getattr(observed, field_name)
    expected_value = getattr(expected, field_name)
    if not math.isclose(observed_value, expected_value, rel_tol=1e-9, abs_tol=1e-9):
        raise MetricsValidationError(
            f"epoch {observed.epoch_id} {field_name} does not match packet events"
        )


def _required_int(row: dict[str, Any], key: str) -> int:
    if key not in row:
        raise MetricsValidationError(f"missing required metrics field {key}")
    value = row[key]
    if type(value) is not int:
        raise MetricsValidationError(f"metrics field {key} must be an integer")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise MetricsValidationError("optional metrics value must be an integer")
    return value


def _required_float(row: dict[str, Any], key: str) -> float:
    if key not in row:
        raise MetricsValidationError(f"missing required metrics field {key}")
    value = row[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetricsValidationError(f"metrics field {key} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise MetricsValidationError(f"metrics field {key} must be finite")
    return result


def _required_bool(row: dict[str, Any], key: str) -> bool:
    if key not in row or type(row[key]) is not bool:
        raise MetricsValidationError(f"metrics field {key} must be boolean")
    return row[key]


def _required_str(row: dict[str, Any], key: str) -> str:
    if key not in row:
        raise MetricsValidationError(f"missing required metrics field {key}")
    value = row[key]
    if not isinstance(value, str) or not value:
        raise MetricsValidationError(f"metrics field {key} must be a string")
    return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MetricsValidationError("optional metrics value must be a string")
    return value


def _format_ids(values: set[int]) -> str:
    return ", ".join(str(value) for value in sorted(values))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate GTP-U gate packet-event and KPI logs."
    )
    parser.add_argument("--event-log", required=True)
    parser.add_argument("--kpi-log", required=True)
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=0.0,
        help="wait for asynchronously flushed logs to become valid",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    deadline = time.monotonic() + max(0.0, args.wait_seconds)
    while True:
        try:
            report = load_gated_user_plane_metrics(args.event_log, args.kpi_log)
            break
        except MetricsValidationError as exc:
            if time.monotonic() >= deadline:
                print(str(exc), file=sys.stderr)
                return 1
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    print(
        json.dumps(
            {
                "epochs": len(report.epochs),
                "submitted_packets": report.total_submitted_packets,
                "delivered_packets": report.total_delivered_packets,
                "dropped_packets": report.total_dropped_packets,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
