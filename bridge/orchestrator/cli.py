"""CLI entrypoint for rendering and starting integration runs."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path

from bridge.common.ids import generate_run_id
from bridge.common.scenario import load_scenario
from bridge.orchestrator.config_renderer import render_run_assets


def _prepare_run(args: argparse.Namespace) -> int:
    project_root = Path(__file__).resolve().parents[2]
    scenario = load_scenario(args.scenario)
    run_id = args.run_id or generate_run_id(scenario.scenario_id)
    rendered = render_run_assets(
        project_root,
        scenario,
        run_id,
        live_graph_snapshot_id=args.live_graph_snapshot_id,
    )
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
            child_env = os.environ.copy()
            child_env.update(command.get("env", {}) or {})
            stream_output = _should_stream_command(command)
            if command.get("background"):
                process = subprocess.Popen(
                    command["argv"],
                    cwd=command["cwd"],
                    env=child_env,
                    text=True,
                    start_new_session=True,
                    stdout=None if stream_output else subprocess.DEVNULL,
                    stderr=None if stream_output else subprocess.DEVNULL,
                )
                processes.append(process)
            else:
                _run_command(command, child_env, stream_output=stream_output)
        return 0
    except BaseException:
        _terminate_background_processes(processes)
        raise
    finally:
        if args.wait_background:
            for process in processes:
                process.wait()


def _run_command(command: dict[str, object], child_env: dict[str, str], *, stream_output: bool) -> None:
    argv = command["argv"]
    cwd = command["cwd"]
    retries = 3 if _is_retryable_compose_up(argv) or _is_retryable_bridge_setup(command) else 1
    last_error: subprocess.CalledProcessError | None = None
    for attempt in range(1, retries + 1):
        try:
            subprocess.run(
                argv,
                cwd=cwd,
                env=child_env,
                check=True,
                text=True,
                stdout=None if stream_output else subprocess.DEVNULL,
                stderr=None if stream_output else subprocess.DEVNULL,
            )
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            message = str(exc)
            if attempt >= retries or not _is_retryable_failure(command, message):
                raise
            time.sleep(2)
    if last_error is not None:
        raise last_error


def _is_retryable_compose_up(argv: object) -> bool:
    return (
        isinstance(argv, list)
        and len(argv) >= 6
        and argv[:2] == ["docker", "compose"]
        and "up" in argv
    )


def _is_retryable_compose_failure(message: str) -> bool:
    return "already in progress" in message or "No such container" in message


def _is_retryable_bridge_setup(command: dict[str, object]) -> bool:
    return str(command.get("name") or "").strip() == "bridge-setup"


def _is_retryable_failure(command: dict[str, object], message: str) -> bool:
    if _is_retryable_bridge_setup(command):
        return True
    return _is_retryable_compose_failure(message)


def _should_stream_command(command: dict[str, object]) -> bool:
    return str(command.get("name") or "") == "policy-acceptor"


def _terminate_background_processes(processes: list[subprocess.Popen[str]]) -> None:
    active = [process for process in processes if process.poll() is None]
    for process in active:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    deadline = time.time() + 5
    while active and time.time() < deadline:
        active = [process for process in active if process.poll() is None]
        time.sleep(0.1)
    for process in active:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            continue


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and start integration runs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare-run", help="render configs and manifest")
    prepare.add_argument("scenario", help="path to scenario YAML")
    prepare.add_argument("--run-id", help="explicit run identifier")
    prepare.add_argument("--live-graph-snapshot-id", help="override writer-follow-ns3 live graph snapshot id")
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
