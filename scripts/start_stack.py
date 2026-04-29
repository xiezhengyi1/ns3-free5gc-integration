#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bridge.orchestrator.cli import main as orchestrator_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and start an integration run")
    parser.add_argument(
        "input_path",
        help="path to a scenario YAML or an existing run-manifest.json",
    )
    parser.add_argument("--run-id", help="explicit run identifier when input_path is a scenario YAML")
    parser.add_argument("--live-graph-snapshot-id", help="override writer-follow-ns3 live graph snapshot id")
    parser.add_argument("--step", action="append", help="run only selected steps")
    parser.add_argument("--dry-run", action="store_true", help="print commands only")
    parser.add_argument(
        "--wait-background",
        action="store_true",
        help="wait for background commands before exiting",
    )
    return parser

def _capture_prepare_manifest(
    input_path: Path,
    run_id: str | None,
    live_graph_snapshot_id: str | None,
) -> Path:
    buffer = StringIO()
    with redirect_stdout(buffer):
        orchestrator_main(
            [
                "prepare-run",
                str(input_path),
                *([] if not run_id else ["--run-id", run_id]),
                *([] if not live_graph_snapshot_id else ["--live-graph-snapshot-id", live_graph_snapshot_id]),
            ]
        )
    payload = buffer.getvalue().strip()
    if not payload:
        raise SystemExit("prepare-run produced no manifest output")
    manifest = Path(json.loads(payload)["run_dir"]) / "run-manifest.json"
    if not manifest.exists():
        raise SystemExit(f"manifest was not generated: {manifest}")
    return manifest


def _should_cleanup_stack(args: argparse.Namespace) -> bool:
    return not args.dry_run and not args.step


def _load_manifest(manifest_path: Path) -> dict:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _iter_processes() -> list[tuple[int, str]]:
    processes: list[tuple[int, str]] = []
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            cmdline = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8").strip()
        except OSError:
            continue
        if cmdline:
            processes.append((int(proc_dir.name), cmdline))
    return processes


def _process_matches_cleanup_scope(*, pid: int, cmdline: str, run_id: str, run_dir: str) -> bool:
    if pid in {os.getpid(), os.getppid()}:
        return False

    normalized = cmdline.strip()
    if not normalized:
        return False

    if run_dir and run_dir in normalized:
        return True

    run_tokens = [
        f"--run-id {run_id}",
        f"--runId={run_id}",
        f"/{run_id}/",
    ]
    return any(token in normalized for token in run_tokens)


def _kill_residual_processes(manifest: dict) -> None:
    run_id = str(manifest.get("run_id") or "").strip()
    run_dir = str(manifest.get("run_dir") or "").strip()
    if not run_id and not run_dir:
        return

    signal_order = (signal.SIGTERM, signal.SIGKILL)
    wait_seconds = {signal.SIGTERM: 3.0, signal.SIGKILL: 0.0}
    targeted_groups: set[int] = set()
    targeted_pids: set[int] = set()

    for pid, cmdline in _iter_processes():
        if not _process_matches_cleanup_scope(pid=pid, cmdline=cmdline, run_id=run_id, run_dir=run_dir):
            continue
        try:
            pgid = os.getpgid(pid)
        except OSError:
            continue
        targeted_groups.add(pgid)
        targeted_pids.add(pid)

    if not targeted_groups and not targeted_pids:
        return

    for sig in signal_order:
        for pgid in list(targeted_groups):
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                continue
        for pid in list(targeted_pids):
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                continue
        if wait_seconds[sig] <= 0:
            continue
        deadline = time.time() + wait_seconds[sig]
        while time.time() < deadline:
            alive = False
            for pid in list(targeted_pids):
                try:
                    os.kill(pid, 0)
                    alive = True
                    break
                except ProcessLookupError:
                    continue
            if not alive:
                break
            time.sleep(0.1)


def _cleanup_stack(manifest_path: Path) -> None:
    manifest: dict | None = None
    try:
        manifest = _load_manifest(manifest_path)
    except Exception as exc:
        print(f"failed to load manifest for cleanup: {exc}", file=sys.stderr)

    try:
        orchestrator_main(["start", str(manifest_path), "--step", "compose-down"])
    except Exception as exc:
        print(f"cleanup failed: {exc}", file=sys.stderr)
    finally:
        if manifest is not None:
            try:
                _kill_residual_processes(manifest)
            except Exception as exc:
                print(f"residual process cleanup failed: {exc}", file=sys.stderr)


def _remove_path(path: Path) -> None:
    if path.is_dir():
        for child in path.iterdir():
            _remove_path(child)
        path.rmdir()
        return
    path.unlink()


def _reset_run_artifacts(manifest: dict) -> None:
    run_id = str(manifest.get("run_id") or "").strip()
    run_dir = Path(str(manifest.get("run_dir") or "")).expanduser().resolve()
    snapshot_file = Path(str(manifest.get("snapshot_file") or "")).expanduser().resolve()
    clock_file = Path(str(manifest.get("clock_file") or "")).expanduser().resolve()
    state_db = Path(str(manifest.get("state_db") or "")).expanduser().resolve()
    archive_dir = Path(str(manifest.get("archive_dir") or "")).expanduser().resolve()

    files_to_remove = [
        snapshot_file,
        clock_file,
        clock_file.parent / "real-ue-flows.jsonl",
        state_db,
        state_db.with_name(state_db.name + "-shm"),
        state_db.with_name(state_db.name + "-wal"),
        run_dir / "state" / "policy-acceptor-state.json",
        archive_dir / run_id / "latest.json",
    ]
    dirs_to_remove = [
        archive_dir / run_id / "ticks",
    ]

    for path in files_to_remove:
        try:
            if path.exists():
                _remove_path(path)
        except FileNotFoundError:
            continue

    for path in dirs_to_remove:
        try:
            if path.exists():
                _remove_path(path)
        except FileNotFoundError:
            continue


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input_path).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"input path does not exist: {input_path}")

    manifest_path = input_path
    if input_path.suffix.lower() != ".json":
        manifest_path = _capture_prepare_manifest(input_path, args.run_id, args.live_graph_snapshot_id)

    manifest = _load_manifest(manifest_path)
    logs_dir = Path(str(manifest.get("run_dir") or "")).expanduser().resolve() / "logs"
    print(f"manifest={manifest_path}")
    print(f"logs={logs_dir}")
    if _should_cleanup_stack(args):
        try:
            _kill_residual_processes(manifest)
        except Exception as exc:
            print(f"pre-start residual cleanup failed: {exc}", file=sys.stderr)
        try:
            _reset_run_artifacts(manifest)
        except Exception as exc:
            print(f"pre-start artifact cleanup failed: {exc}", file=sys.stderr)

    argv = ["start", str(manifest_path)]
    for step in args.step or []:
        argv.extend(["--step", step])
    if args.dry_run:
        argv.append("--dry-run")
    argv.append("--stream-output")
    if args.wait_background:
        argv.append("--wait-background")
    try:
        return orchestrator_main(argv)
    except KeyboardInterrupt:
        if _should_cleanup_stack(args):
            _cleanup_stack(manifest_path)
        raise
    except subprocess.CalledProcessError:
        if _should_cleanup_stack(args):
            _cleanup_stack(manifest_path)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
