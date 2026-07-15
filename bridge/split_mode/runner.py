"""Execute a split-mode manifest."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from typing import NoReturn


def _is_python_command(argv: list[str]) -> bool:
    if not argv:
        return False
    executable = Path(argv[0]).name.lower()
    return executable.startswith("python")


def _terminate_process_group(process: subprocess.Popen[str], sig: signal.Signals) -> None:
    try:
        pgid = os.getpgid(process.pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return


def _shutdown_processes(processes: list[subprocess.Popen[str]]) -> None:
    for process in reversed(processes):
        _terminate_process_group(process, signal.SIGTERM)
    for process in reversed(processes):
        if process.poll() is None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _terminate_process_group(process, signal.SIGKILL)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass


def _raise_interrupt(signum: int) -> NoReturn:
    raise KeyboardInterrupt(f"received signal {signum}")


def run_manifest(manifest_path: Path, *, wait_background: bool = False) -> int:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    logs_dir = manifest_path.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    processes: list[subprocess.Popen[str]] = []
    log_handles = []
    compose_down = next(item for item in manifest["commands"] if item["name"] == "compose-down")
    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, lambda signum, frame: _raise_interrupt(signum))
    signal.signal(signal.SIGTERM, lambda signum, frame: _raise_interrupt(signum))

    try:
        for command in manifest["commands"]:
            name = str(command["name"])
            if name == "compose-down":
                continue
            argv = [str(item) for item in command["argv"]]
            env = os.environ.copy()
            env.update({str(key): str(value) for key, value in command.get("env", {}).items()})
            if _is_python_command(argv):
                env["PYTHONUNBUFFERED"] = "1"
            log_path = logs_dir / f"{name}.log"
            handle = log_path.open("w", encoding="utf-8")
            log_handles.append(handle)
            process = subprocess.Popen(
                argv,
                cwd=str(command["cwd"]),
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            processes.append(process)
            if not wait_background:
                print(f"started {name} pid={process.pid} log={log_path}")
            if not command.get("background"):
                rc = process.wait()
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, argv)
        if wait_background:
            for process in processes:
                if process.poll() is None:
                    process.wait()
        return 0
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        _shutdown_processes(processes)
        with (logs_dir / "compose-down.log").open("a", encoding="utf-8") as handle:
            subprocess.run(
                [str(item) for item in compose_down["argv"]],
                cwd=str(compose_down["cwd"]),
                env=os.environ.copy(),
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        for handle in log_handles:
            handle.close()


def run_fast_reset_server(manifest_path: Path) -> int:
    """Run the manifest-owned reset supervisor for a long-lived episode.

    The supervisor, rather than this one-shot runner, owns the Compose and
    runtime process lifecycles.  It performs the initial cold reset before
    serving `/v1/reset`, so the advertised endpoint is live as soon as the
    split-mode launcher reports readiness.
    """
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    fast_reset = manifest.get("fast_reset") if isinstance(manifest, dict) else None
    argv = fast_reset.get("serve_argv") if isinstance(fast_reset, dict) else None
    if not isinstance(argv, list) or not argv or not all(str(item).strip() for item in argv):
        raise ValueError("split-mode manifest does not define fast_reset.serve_argv")

    env = os.environ.copy()
    if _is_python_command([str(item) for item in argv]):
        env["PYTHONUNBUFFERED"] = "1"
    completed = subprocess.run(
        [str(item) for item in argv],
        cwd=str(manifest_path.parent),
        env=env,
        text=True,
        check=False,
    )
    return int(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute a split-mode manifest")
    parser.add_argument("manifest", help="path to run-manifest.split.json")
    parser.add_argument(
        "--wait-background",
        dest="wait_background",
        action="store_true",
        help="wait for background commands before exiting",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    return run_manifest(
        Path(args.manifest).expanduser().resolve(),
        wait_background=bool(args.wait_background),
    )


if __name__ == "__main__":
    raise SystemExit(main())
