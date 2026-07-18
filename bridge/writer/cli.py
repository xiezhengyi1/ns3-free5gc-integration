
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
    extract_compose_timestamp,
    parse_free5gc_compose_line,
    parse_ueransim_compose_line,
)
from bridge.writer.local_store import SnapshotStore
from bridge.writer.owner import SnapshotStoreClient, serve_forever
from bridge.writer.postgres_graph_store import PostgresGraphStore


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state-db", required=True, help="sqlite path for local state")
    parser.add_argument("--archive-dir", required=True, help="directory for archived ticks")
    parser.add_argument("--writer-socket", help="single-writer owner Unix socket; followers never open SQLite directly")
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


def _build_writer(
    args: argparse.Namespace,
) -> tuple[SnapshotStore | SnapshotStoreClient, HttpIngestionClient | None, PostgresGraphStore | None]:
    store = SnapshotStoreClient(args.writer_socket) if args.writer_socket else SnapshotStore(args.state_db, args.archive_dir)
    client = HttpIngestionClient(args.ingestion_url) if args.ingestion_url else None
    graph_store = (
        PostgresGraphStore(args.graph_db_url, ensure_schema=args.ensure_graph_schema)
        if args.graph_db_url
        else None
    )
    return store, client, graph_store


def _ingest_line(
    line: str,
    store: SnapshotStore | SnapshotStoreClient,
    client: HttpIngestionClient | None,
    graph_store: PostgresGraphStore | None,
    args: argparse.Namespace,
) -> str:
    if not line.strip():
        return "ingested"
    snapshot = TickSnapshot.from_dict(json.loads(line))
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
    return "ingested"


def _append_event(store: SnapshotStore | SnapshotStoreClient, event_payload: dict[str, object]) -> None:
    print(json.dumps(event_payload, ensure_ascii=False))


def _current_event_tick(store: SnapshotStore | SnapshotStoreClient, run_id: str) -> int:
    latest_tick = store.latest_snapshot_tick(run_id)
    return max(0, latest_tick)


def _resolve_event_tick(store: SnapshotStore | SnapshotStoreClient, run_id: str, raw_line: str) -> int:
    observed_at = extract_compose_timestamp(raw_line)
    if observed_at is None:
        return _current_event_tick(store, run_id)
    return store.resolve_tick_for_observed_at(run_id, observed_at)


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
            event_tick = _resolve_event_tick(store, args.run_id, line)
            events = parser(
                line,
                run_id=args.run_id,
                scenario_id=args.scenario_id,
                tick_index=event_tick,
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
        deferred_line: str | None = None
        while True:
            current_size = path.stat().st_size
            if current_size < handle.tell():
                handle.seek(0)
                pending = ""
                if deferred_line is None:
                    continue
            if deferred_line is not None:
                outcome = _ingest_line(deferred_line, store, client, graph_store, args)
                if outcome in {"ingested", "skipped"}:
                    deferred_line = None
                    continue
                time.sleep(args.poll_interval)
                continue
            pending, line = _next_complete_jsonl_line(
                handle,
                pending,
                flush_pending=bool(args.stop_at_eof),
            )
            if line is not None:
                outcome = _ingest_line(line, store, client, graph_store, args)
                if outcome == "pending":
                    deferred_line = line
                continue
            if args.stop_at_eof:
                break
            time.sleep(args.poll_interval)
    return 0


def _graph_snapshot_exists(args: argparse.Namespace) -> int:
    store = PostgresGraphStore(args.graph_db_url, ensure_schema=args.ensure_graph_schema)
    print(
        json.dumps(
            {
                "snapshot_id": args.snapshot_id,
                "exists": store.snapshot_exists(args.snapshot_id),
            },
            ensure_ascii=False,
        )
    )
    return 0


def _serve_owner(args: argparse.Namespace) -> int:
    serve_forever(socket_path=args.writer_socket, state_db=args.state_db, archive_dir=args.archive_dir)
    return 0


def _delete_graph_snapshot(args: argparse.Namespace) -> int:
    store = PostgresGraphStore(args.graph_db_url, ensure_schema=args.ensure_graph_schema)
    print(json.dumps(store.delete_snapshot(args.snapshot_id), ensure_ascii=False))
    return 0


def _prune_graph_snapshots(args: argparse.Namespace) -> int:
    store = PostgresGraphStore(args.graph_db_url, ensure_schema=args.ensure_graph_schema)
    keep_snapshot_ids: list[str] = []
    if args.keep_snapshot_id:
        keep_snapshot_ids.append(args.keep_snapshot_id)
    if args.keep_latest:
        latest_snapshot_id = store.latest_snapshot_id()
        if latest_snapshot_id:
            keep_snapshot_ids.append(latest_snapshot_id)
    result = store.prune_snapshots(keep_snapshot_ids=keep_snapshot_ids)
    if args.keep_latest:
        result["latest_snapshot_id"] = store.latest_snapshot_id()
    print(json.dumps(result, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest ns-3 snapshots and core/RAN observations")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve-owner", help="run the sole local SQLite writer owner")
    _add_common_arguments(serve)
    serve.set_defaults(handler=_serve_owner)

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
    follow.add_argument("--tick-ms", type=int, default=1000, help="tick duration in milliseconds")
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

    graph_exists = subparsers.add_parser("graph-snapshot-exists", help="check whether a graph snapshot id exists")
    graph_exists.add_argument("--graph-db-url", required=True, help="PostgreSQL graph database URL")
    graph_exists.add_argument("--snapshot-id", required=True, help="graph snapshot id to inspect")
    graph_exists.add_argument(
        "--ensure-graph-schema",
        action="store_true",
        help="create graph tables if they do not already exist",
    )
    graph_exists.set_defaults(handler=_graph_snapshot_exists)

    delete_graph = subparsers.add_parser("delete-graph-snapshot", help="delete one graph snapshot and its rows")
    delete_graph.add_argument("--graph-db-url", required=True, help="PostgreSQL graph database URL")
    delete_graph.add_argument("--snapshot-id", required=True, help="graph snapshot id to delete")
    delete_graph.add_argument(
        "--ensure-graph-schema",
        action="store_true",
        help="create graph tables if they do not already exist",
    )
    delete_graph.set_defaults(handler=_delete_graph_snapshot)

    prune_graph = subparsers.add_parser("prune-graph-snapshots", help="delete all graph snapshots except the kept ones")
    prune_graph.add_argument("--graph-db-url", required=True, help="PostgreSQL graph database URL")
    prune_graph.add_argument("--keep-snapshot-id", help="graph snapshot id to keep")
    prune_graph.add_argument(
        "--keep-latest",
        action="store_true",
        help="keep the latest graph snapshot row instead of deleting everything",
    )
    prune_graph.add_argument(
        "--ensure-graph-schema",
        action="store_true",
        help="create graph tables if they do not already exist",
    )
    prune_graph.set_defaults(handler=_prune_graph_snapshots)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
