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
    result = store.ingest_snapshot(
        snapshot,
        seed=args.seed,
        topology_version=args.topology_version,
    )
    if result["inserted"] and client is not None:
        result["http"] = client.post_snapshot(snapshot)
    if result["inserted"] and graph_store is not None:
        graph_result = graph_store.ingest_snapshot(
            snapshot,
            trigger_event=f"{args.graph_trigger_event}:{snapshot.run_id}:{snapshot.tick_index}",
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
                tick_index=clock.current_tick(),
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


def _follow_jsonl(args: argparse.Namespace) -> int:
    store, client, graph_store = _build_writer(args)
    path = Path(args.path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)

    with path.open("r", encoding="utf-8") as handle:
        if args.from_end:
            handle.seek(0, 2)
        while True:
            line = handle.readline()
            if line:
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