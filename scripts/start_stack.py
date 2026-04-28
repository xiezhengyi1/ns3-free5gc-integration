#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
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
    parser.add_argument("--step", action="append", help="run only selected steps")
    parser.add_argument("--dry-run", action="store_true", help="print commands only")
    parser.add_argument(
        "--wait-background",
        action="store_true",
        help="wait for background commands before exiting",
    )
    return parser

def _capture_prepare_manifest(input_path: Path, run_id: str | None) -> Path:
    buffer = StringIO()
    with redirect_stdout(buffer):
        orchestrator_main(
            [
                "prepare-run",
                str(input_path),
                *([] if not run_id else ["--run-id", run_id]),
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


def _cleanup_stack(manifest_path: Path) -> None:
    try:
        orchestrator_main(["start", str(manifest_path), "--step", "compose-down"])
    except Exception as exc:
        print(f"cleanup failed: {exc}", file=sys.stderr)


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input_path).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"input path does not exist: {input_path}")

    manifest_path = input_path
    if input_path.suffix.lower() != ".json":
        manifest_path = _capture_prepare_manifest(input_path, args.run_id)

    argv = ["start", str(manifest_path)]
    for step in args.step or []:
        argv.extend(["--step", step])
    if args.dry_run:
        argv.append("--dry-run")
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
