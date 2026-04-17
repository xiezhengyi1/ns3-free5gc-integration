"""CLI for ingesting tick snapshots into local state and optional HTTP sink."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from bridge.common.schema import TickSnapshot
from bridge.writer.http_sink import HttpIngestionClient
from bridge.writer.local_store import SnapshotStore


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state-db", required=True, help="sqlite path for local state")
    parser.add_argument("--archive-dir", required=True, help="directory for archived ticks")
    parser.add_argument("--ingestion-url", help="optional HTTP ingestion endpoint")
    parser.add_argument("--seed", type=int, help="seed associated with the run")
    parser.add_argument(
        "--topology-version",
        default="v1",
        help="topology version label stored with the run",
    )


def _build_writer(args: argparse.Namespace) -> tuple[SnapshotStore, HttpIngestionClient | None]:
    store = SnapshotStore(args.state_db, args.archive_dir)
    client = HttpIngestionClient(args.ingestion_url) if args.ingestion_url else None
    return store, client


def _ingest_line(
    line: str,
    store: SnapshotStore,
    client: HttpIngestionClient | None,
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
    print(json.dumps(result, ensure_ascii=False))


def _ingest_file(args: argparse.Namespace) -> int:
    store, client = _build_writer(args)
    with Path(args.path).expanduser().resolve().open("r", encoding="utf-8") as handle:
        for line in handle:
            _ingest_line(line, store, client, args)
    return 0


def _ingest_stdin(args: argparse.Namespace) -> int:
    store, client = _build_writer(args)
    for line in sys.stdin:
        _ingest_line(line, store, client, args)
    return 0


def _follow_jsonl(args: argparse.Namespace) -> int:
    store, client = _build_writer(args)
    path = Path(args.path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)

    with path.open("r", encoding="utf-8") as handle:
        if args.from_end:
            handle.seek(0, 2)
        while True:
            line = handle.readline()
            if line:
                _ingest_line(line, store, client, args)
                continue
            if args.stop_at_eof:
                break
            time.sleep(args.poll_interval)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest ns-3 tick snapshots")
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())