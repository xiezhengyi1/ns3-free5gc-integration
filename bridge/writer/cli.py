"""CLI for ingesting tick snapshots into local state and optional HTTP sink."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from bridge.common.schema import TickSnapshot
from bridge.writer.http_sink import HttpIngestionClient
from bridge.writer.log_parser import (
    ObservationClock,
    parse_free5gc_compose_line,
    parse_ueransim_compose_line,
)
from bridge.writer.local_store import SnapshotStore
from bridge.writer.postgres_graph_store import PostgresGraphStore


class Ns3ClockReference:
    def __init__(self, path: str) -> None:
        self.path = Path(path).expanduser().resolve()
        self._mtime_ns: int | None = None
        self._tick_index = 0

    def current_tick(self) -> int:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return self._tick_index
        if self._mtime_ns == stat.st_mtime_ns:
            return self._tick_index
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._tick_index
        tick_index = payload.get("tick_index")
        if tick_index is not None:
            self._tick_index = int(tick_index)
            self._mtime_ns = stat.st_mtime_ns
        return self._tick_index


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state-db", required=True, help="sqlite path for local state")
    parser.add_argument("--archive-dir", required=True, help="directory for archived ticks")
    parser.add_argument("--ingestion-url", help="optional HTTP ingestion endpoint")
    parser.add_argument("--graph-db-url", help="optional PostgreSQL graph database URL")
    parser.add_argument(
        "--graph-trigger-event",
        default="sim_tick",
        help="trigger_event prefix used when persisting graph snapshots",
    )
    parser.add_argument(
        "--live-graph-snapshot-id",
        help="update one graph snapshot row instead of creating one graph snapshot per tick",
    )
    parser.add_argument(
        "--ensure-graph-schema",
        action="store_true",
        help="create graph tables if they do not already exist",
    )
    parser.add_argument("--seed", type=int, help="seed associated with the run")
    parser.add_argument(
        "--topology-version",
        default="v1",
        help="topology version label stored with the run",
    )
    parser.add_argument("--real-traffic-state-file", help="JSONL file written by run_real_ue_flows.py")
    parser.add_argument("--real-traffic-timeout-seconds", type=float, default=15.0)


def _build_writer(
    args: argparse.Namespace,
) -> tuple[SnapshotStore, HttpIngestionClient | None, PostgresGraphStore | None]:
    store = SnapshotStore(args.state_db, args.archive_dir)
    client = HttpIngestionClient(args.ingestion_url) if args.ingestion_url else None
    graph_store = (
        PostgresGraphStore(args.graph_db_url, ensure_schema=args.ensure_graph_schema)
        if args.graph_db_url
        else None
    )
    return store, client, graph_store


def _merge_real_traffic_state(snapshot: TickSnapshot, args: argparse.Namespace) -> TickSnapshot:
    state_file = getattr(args, "real_traffic_state_file", None)
    if not state_file:
        return snapshot

    path = Path(state_file).expanduser().resolve()
    deadline = time.monotonic() + float(getattr(args, "real_traffic_timeout_seconds", 15.0))
    state_payload: dict[str, object] | None = None
    while time.monotonic() <= deadline:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    tick_index = int(payload.get("tick_index", -1))
                    if tick_index <= snapshot.tick_index:
                        state_payload = payload
        except FileNotFoundError:
            pass
        if state_payload is not None:
            break
        time.sleep(0.05)

    if state_payload is None:
        return snapshot

    tick_seconds = max(0.001, float(getattr(args, "tick_ms", 1000)) / 1000.0)
    real_flows = {
        str(item["flow_id"]): item
        for item in state_payload.get("flows", [])
        if isinstance(item, dict) and "flow_id" in item
    }
    ue_ip_by_supi: dict[str, str] = {}
    for flow in snapshot.flows:
        real_flow = real_flows.get(flow.flow_id)
        if real_flow is None:
            continue
        ue_ip = str(real_flow["ue_ip"])
        source_port = int(real_flow["source_port"])
        destination_port = int(real_flow["destination_port"])
        packet_size_bytes = int(real_flow.get("packet_size_bytes", 0) or 0)
        ul_packets_sent = int(real_flow.get("ul_packets_sent", 0) or 0)
        dl_packets_sent = int(real_flow.get("dl_packets_sent", 0) or 0)
        throughput_ul_mbps = (
            ul_packets_sent * packet_size_bytes * 8.0 / tick_seconds / 1e6
            if packet_size_bytes > 0
            else 0.0
        )
        throughput_dl_mbps = (
            dl_packets_sent * packet_size_bytes * 8.0 / tick_seconds / 1e6
            if packet_size_bytes > 0
            else 0.0
        )
        baseline_loss = _as_float(flow.sla.get("loss_rate")) if isinstance(flow.sla, dict) else None
        loss_rate = baseline_loss if baseline_loss is not None else 0.0
        packet_sent = ul_packets_sent + dl_packets_sent
        packet_received = max(0, int(round(packet_sent * max(0.0, 1.0 - loss_rate))))
        traffic = dict(flow.traffic)
        traffic["five_tuple"] = {
            "protocol": 17,
            "source_ip": ue_ip,
            "source_port": source_port,
            "destination_ip": traffic.get("five_tuple", {}).get("destination_ip", "8.8.8.8")
            if isinstance(traffic.get("five_tuple"), dict)
            else "8.8.8.8",
            "destination_port": destination_port,
        }
        if dl_packets_sent > 0:
            traffic["reverse_five_tuple"] = {
                "protocol": 17,
                "source_ip": traffic["five_tuple"]["destination_ip"],
                "source_port": destination_port,
                "destination_ip": ue_ip,
                "destination_port": source_port,
            }
        else:
            traffic.pop("reverse_five_tuple", None)
        if ul_packets_sent > 0 and dl_packets_sent > 0:
            traffic["direction"] = "bidirectional"
        elif dl_packets_sent > 0:
            traffic["direction"] = "downlink"
        else:
            traffic["direction"] = "uplink"
        traffic["ue_interface"] = str(real_flow["interface"])
        traffic["dl_upf_container"] = str(real_flow["dl_container"])
        flow.traffic = traffic
        flow.throughput_ul_mbps = throughput_ul_mbps
        flow.throughput_dl_mbps = throughput_dl_mbps
        flow.loss_rate = loss_rate
        telemetry = dict(flow.telemetry)
        telemetry["loss_rate"] = loss_rate
        telemetry["packet_sent"] = packet_sent
        telemetry["packet_received"] = packet_received
        telemetry["throughput_ul"] = throughput_ul_mbps
        telemetry["throughput_dl"] = throughput_dl_mbps
        flow.telemetry = telemetry
        ue_ip_by_supi[flow.supi] = ue_ip

    for ue in snapshot.ues:
        if ue.supi in ue_ip_by_supi:
            ue.ip_address = ue_ip_by_supi[ue.supi]

    if snapshot.flows:
        mean_delay_ms = sum(float(flow.delay_ms) for flow in snapshot.flows) / len(snapshot.flows)
        mean_loss_rate = sum(float(flow.loss_rate) for flow in snapshot.flows) / len(snapshot.flows)
        throughput_dl_total = sum(float(flow.throughput_dl_mbps) for flow in snapshot.flows)
        throughput_ul_total = sum(float(flow.throughput_ul_mbps) for flow in snapshot.flows)
        kpis = dict(snapshot.kpis)
        kpis["active_flows"] = float(len(snapshot.flows))
        kpis["mean_delay_ms"] = mean_delay_ms
        kpis["mean_loss_rate"] = mean_loss_rate
        kpis["throughput_dl_mbps_total"] = throughput_dl_total
        kpis["throughput_ul_mbps_total"] = throughput_ul_total
        snapshot.kpis = kpis

        reward_inputs = dict(snapshot.reward_inputs)
        reward_inputs["delay_penalty"] = mean_delay_ms
        reward_inputs["loss_penalty"] = mean_loss_rate
        reward_inputs["throughput_score"] = throughput_dl_total + throughput_ul_total
        snapshot.reward_inputs = reward_inputs
    return snapshot


def _ingest_line(
    line: str,
    store: SnapshotStore,
    client: HttpIngestionClient | None,
    graph_store: PostgresGraphStore | None,
    args: argparse.Namespace,
) -> None:
    if not line.strip():
        return
    snapshot = TickSnapshot.from_dict(json.loads(line))
    snapshot = _merge_real_traffic_state(snapshot, args)
    result = store.ingest_snapshot(
        snapshot,
        seed=args.seed,
        topology_version=args.topology_version,
    )
    if result["inserted"] and client is not None:
        result["http"] = client.post_snapshot(snapshot)
    if result["inserted"] and graph_store is not None:
        trigger_event = f"{args.graph_trigger_event}:{snapshot.run_id}:{snapshot.tick_index}"
        if args.live_graph_snapshot_id:
            graph_result = graph_store.upsert_live_graph_snapshot(
                snapshot,
                snapshot_id=args.live_graph_snapshot_id,
                trigger_event=trigger_event,
            )
        else:
            graph_result = graph_store.ingest_snapshot(
                snapshot,
                trigger_event=trigger_event,
            )
        result["graph"] = graph_result.to_dict()
    print(json.dumps(result, ensure_ascii=False))


def _append_event(store: SnapshotStore, event_payload: dict[str, object]) -> None:
    print(json.dumps(event_payload, ensure_ascii=False))


def _follow_compose_logs(args: argparse.Namespace) -> int:
    store, _, _ = _build_writer(args)
    store.upsert_run(
        args.run_id,
        args.scenario_id,
        seed=args.seed,
        topology_version=args.topology_version,
    )

    parser = (
        parse_free5gc_compose_line if args.parser == "free5gc" else parse_ueransim_compose_line
    )
    clock_reference = Ns3ClockReference(args.clock_file) if args.clock_file else None
    clock = ObservationClock(args.tick_ms)
    command = ["docker", "compose"]
    if args.project_name:
        command.extend(["-p", args.project_name])
    command.extend(
        [
            "-f",
            str(Path(args.compose_file).expanduser().resolve()),
            "logs",
            "-f",
            "--no-color",
            "--timestamps",
            "--tail",
            str(args.tail),
            *args.service,
        ]
    )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        assert process.stdout is not None
        for line in process.stdout:
            events = parser(
                line,
                run_id=args.run_id,
                scenario_id=args.scenario_id,
                tick_index=(clock_reference.current_tick() if clock_reference else clock.current_tick()),
            )
            for event in events:
                result = store.append_event(event)
                result["source"] = args.parser
                _append_event(store, result)
        return process.wait()
    finally:
        if process.poll() is None:
            process.terminate()


def _ingest_file(args: argparse.Namespace) -> int:
    store, client, graph_store = _build_writer(args)
    with Path(args.path).expanduser().resolve().open("r", encoding="utf-8") as handle:
        for line in handle:
            _ingest_line(line, store, client, graph_store, args)
    return 0


def _ingest_stdin(args: argparse.Namespace) -> int:
    store, client, graph_store = _build_writer(args)
    for line in sys.stdin:
        _ingest_line(line, store, client, graph_store, args)
    return 0


def _next_complete_jsonl_line(
    handle: object,
    pending: str,
    *,
    flush_pending: bool,
) -> tuple[str, str | None]:
    chunk = handle.readline()
    if not chunk:
        if flush_pending and pending:
            return "", pending
        return pending, None

    pending += chunk
    if not pending.endswith("\n"):
        return pending, None
    return "", pending


def _follow_jsonl(args: argparse.Namespace) -> int:
    store, client, graph_store = _build_writer(args)
    path = Path(args.path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)

    with path.open("r", encoding="utf-8") as handle:
        if args.from_end:
            handle.seek(0, 2)
        pending = ""
        while True:
            pending, line = _next_complete_jsonl_line(
                handle,
                pending,
                flush_pending=bool(args.stop_at_eof),
            )
            if line is not None:
                _ingest_line(line, store, client, graph_store, args)
                continue
            if args.stop_at_eof:
                break
            time.sleep(args.poll_interval)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest ns-3 snapshots and core/RAN observations")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_file = subparsers.add_parser("ingest-file", help="ingest a full JSONL file")
    ingest_file.add_argument("path", help="JSONL file path")
    _add_common_arguments(ingest_file)
    ingest_file.set_defaults(handler=_ingest_file)

    ingest_stdin = subparsers.add_parser("ingest-stdin", help="ingest JSONL from stdin")
    _add_common_arguments(ingest_stdin)
    ingest_stdin.set_defaults(handler=_ingest_stdin)

    follow = subparsers.add_parser("follow-jsonl", help="tail a JSONL file")
    follow.add_argument("path", help="JSONL file path")
    follow.add_argument("--poll-interval", type=float, default=1.0, help="polling interval in seconds")
    follow.add_argument("--from-end", action="store_true", help="start tailing from EOF")
    follow.add_argument("--stop-at-eof", action="store_true", help="exit after current EOF")
    _add_common_arguments(follow)
    follow.set_defaults(handler=_follow_jsonl)

    follow_logs = subparsers.add_parser(
        "follow-compose-logs",
        help="follow docker compose logs and extract semantic events",
    )
    follow_logs.add_argument(
        "--parser",
        choices=["free5gc", "ueransim"],
        required=True,
        help="log parser to apply",
    )
    follow_logs.add_argument("--compose-file", required=True, help="generated compose file path")
    follow_logs.add_argument("--project-name", help="explicit docker compose project name")
    follow_logs.add_argument("--run-id", required=True, help="run identifier")
    follow_logs.add_argument("--scenario-id", required=True, help="scenario identifier")
    follow_logs.add_argument("--tick-ms", type=int, default=1000, help="event tick window in milliseconds")
    follow_logs.add_argument("--clock-file", help="optional ns-3 clock file used as the authoritative tick source")
    follow_logs.add_argument(
        "--tail",
        default="all",
        help="docker compose log tail value passed through to the CLI",
    )
    follow_logs.add_argument(
        "--service",
        action="append",
        required=True,
        help="service name to follow; may be repeated",
    )
    _add_common_arguments(follow_logs)
    follow_logs.set_defaults(handler=_follow_compose_logs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
