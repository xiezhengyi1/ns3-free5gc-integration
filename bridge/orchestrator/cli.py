"""CLI entrypoint for rendering and starting integration runs."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from bridge.common.ids import generate_run_id
from bridge.common.scenario import load_scenario
from bridge.orchestrator.config_renderer import render_run_assets


def _prepare_run(args: argparse.Namespace) -> int:
    project_root = Path(__file__).resolve().parents[2]
    scenario = load_scenario(args.scenario)
    run_id = args.run_id or generate_run_id(scenario.scenario_id)
    rendered = render_run_assets(project_root, scenario, run_id)
    print(json.dumps(rendered.manifest.to_dict(), indent=2, ensure_ascii=False))
    return 0


def _start_run(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest).expanduser().resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    commands = manifest["commands"]
    if args.step:
        wanted = set(args.step)
        commands = [command for command in commands if command["name"] in wanted]

    processes: list[subprocess.Popen[str]] = []
    try:
        for command in commands:
            if args.dry_run:
                print(json.dumps(command, ensure_ascii=False))
                continue
            if command.get("background"):
                process = subprocess.Popen(
                    command["argv"],
                    cwd=command["cwd"],
                    env={**command.get("env", {})} or None,
                    text=True,
                )
                processes.append(process)
                print(f"started background command {command['name']} pid={process.pid}")
            else:
                subprocess.run(
                    command["argv"],
                    cwd=command["cwd"],
                    env={**command.get("env", {})} or None,
                    check=True,
                    text=True,
                )
        return 0
    finally:
        if args.wait_background:
            for process in processes:
                process.wait()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and start integration runs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare-run", help="render configs and manifest")
    prepare.add_argument("scenario", help="path to scenario YAML")
    prepare.add_argument("--run-id", help="explicit run identifier")
    prepare.set_defaults(handler=_prepare_run)

    start = subparsers.add_parser("start", help="execute manifest commands")
    start.add_argument("manifest", help="path to run-manifest.json")
    start.add_argument("--step", action="append", help="run only selected steps")
    start.add_argument("--dry-run", action="store_true", help="print commands only")
    start.add_argument(
        "--wait-background",
        action="store_true",
        help="wait for background commands before exiting",
    )
    start.set_defaults(handler=_start_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())