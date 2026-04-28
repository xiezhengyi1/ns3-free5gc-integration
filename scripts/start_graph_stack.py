#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bridge.common.ids import generate_run_id
from bridge.common.scenario import ScenarioConfig
from bridge.orchestrator.config_renderer import render_run_assets


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render graph-driven YAML and start free5GC, UERANSIM and ns-3")
    parser.add_argument("scenario", help="base scenario YAML with topology.graph_file or topology.graph_snapshot_id")
    parser.add_argument("--run-id", help="explicit run identifier")
    parser.add_argument("--graph-file", help="override topology.graph_file")
    parser.add_argument("--graph-snapshot-id", help="override topology.graph_snapshot_id")
    parser.add_argument("--graph-db-url", help="override writer/topology graph database URL")
    parser.add_argument("--live-graph-snapshot-id", help="PostgreSQL graph snapshot row to update in place")
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    scenario_path = Path(args.scenario).expanduser().resolve()
    with scenario_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("scenario YAML root must be a mapping")

    if args.graph_file or args.graph_snapshot_id or args.graph_db_url:
        topology = dict(payload.get("topology") or {})
        writer = dict(payload.get("writer") or {})
        if args.graph_file:
            topology["graph_file"] = str(Path(args.graph_file).expanduser().resolve())
        if args.graph_snapshot_id:
            topology["graph_snapshot_id"] = args.graph_snapshot_id
        if args.graph_db_url:
            topology["graph_db_url"] = args.graph_db_url
            writer["graph_db_url"] = args.graph_db_url
        payload["topology"] = topology
        payload["writer"] = writer

    topology_payload = dict(payload.get("topology") or {})
    writer_payload = dict(payload.get("writer") or {})
    graph_db_url = topology_payload.get("graph_db_url") or writer_payload.get("graph_db_url")
    if topology_payload.get("graph_snapshot_id"):
        if not graph_db_url:
            raise ValueError("topology.graph_snapshot_id requires writer.graph_db_url or topology.graph_db_url")
    if graph_db_url:
        db_url = urlparse(str(graph_db_url))
        host = db_url.hostname or "localhost"
        port = db_url.port or 5432
        try:
            with socket.create_connection((host, port), timeout=2.0):
                pass
        except OSError as exc:
            raise SystemExit(
                f"graph database is unreachable at {host}:{port}. "
                "Start the PostgreSQL service or SSH tunnel, or pass --graph-db-url with a reachable URL."
            ) from exc

    scenario = ScenarioConfig.from_dict(payload, base_dir=scenario_path.parent)
    if scenario.tick_ms <= 0:
        raise ValueError("tick_ms must be positive")
    if not scenario.writer.graph_db_url:
        raise ValueError("writer.graph_db_url is required for graph delta writes")
    if not (scenario.topology.graph_file or scenario.topology.graph_snapshot_id):
        raise ValueError("topology.graph_file or topology.graph_snapshot_id is required")

    run_id = args.run_id or generate_run_id(scenario.scenario_id)
    live_graph_snapshot_id = args.live_graph_snapshot_id or f"live-{scenario.scenario_id}"

    stale_pids: list[int] = []
    own_pid = os.getpid()
    process_markers = (
        "nr_multignb_multiupf",
        "run_ns3_twin.sh",
        "run_real_ue_flows.py",
        "bridge.writer.cli",
    )
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        if pid == own_pid:
            continue
        try:
            cmdline = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "ignore")
        except OSError:
            continue
        if run_id not in cmdline or not any(marker in cmdline for marker in process_markers):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            stale_pids.append(pid)
        except ProcessLookupError:
            pass
    if stale_pids:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if all(not Path(f"/proc/{pid}").exists() for pid in stale_pids):
                break
            time.sleep(0.1)
        for pid in stale_pids:
            if Path(f"/proc/{pid}").exists():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        print(f"stopped stale run processes for run_id={run_id}: {stale_pids}")

    rendered = render_run_assets(project_root, scenario, run_id)
    manifest = rendered.manifest.to_dict()
    logs_dir = rendered.run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    python_executable = project_root / ".venv" / "bin" / "python3"
    python_command = str(python_executable) if python_executable.exists() else sys.executable

    for stale_output in (
        Path(str(manifest["snapshot_file"])),
        Path(str(manifest["clock_file"])),
        Path(str(manifest["snapshot_file"])).with_name("real-ue-flows.jsonl"),
    ):
        stale_output.parent.mkdir(parents=True, exist_ok=True)
        stale_output.write_text("", encoding="utf-8")

    graph_yaml = rendered.generated_dir / "scenario-from-graph.yaml"
    graph_yaml.write_text(
        yaml.safe_dump(json.loads(json.dumps(asdict(scenario))), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    print(f"run_id={run_id}")
    print(f"graph_yaml={graph_yaml}")
    print(f"manifest={rendered.run_dir / 'run-manifest.json'}")
    print(f"logs={logs_dir}")
    print(f"live_graph_snapshot_id={live_graph_snapshot_id}")

    processes: list[subprocess.Popen[str]] = []
    log_handles = []
    compose_down = next(command for command in manifest["commands"] if command["name"] == "compose-down")
    exit_code = 0

    try:
        for command in manifest["commands"]:
            name = str(command["name"])
            if name == "compose-down":
                continue
            argv_items = [str(item) for item in command["argv"]]
            if argv_items and argv_items[0] == "python3":
                argv_items[0] = python_command
            if name == "writer-follow-ns3":
                if "--ensure-graph-schema" not in argv_items:
                    argv_items.append("--ensure-graph-schema")
                if "--live-graph-snapshot-id" not in argv_items:
                    argv_items.extend(["--live-graph-snapshot-id", live_graph_snapshot_id])
            env = os.environ.copy()
            env.update({str(key): str(value) for key, value in command.get("env", {}).items()})
            if argv_items and argv_items[0] == python_command:
                env["PYTHONUNBUFFERED"] = "1"
            log_path = logs_dir / f"{name}.log"
            log_handle = log_path.open("w", encoding="utf-8")
            log_handles.append(log_handle)
            process = subprocess.Popen(
                argv_items,
                cwd=str(command["cwd"]),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            processes.append(process)
            print(f"started {name} pid={process.pid} log={log_path}")
            if not command.get("background"):
                exit_code = process.wait()
                if exit_code != 0:
                    raise subprocess.CalledProcessError(exit_code, argv_items)
        return 0
    except KeyboardInterrupt:
        exit_code = 130
        print("interrupted; stopping modules")
        return exit_code
    except subprocess.CalledProcessError as exc:
        exit_code = exc.returncode
        print(f"command failed rc={exit_code}: {' '.join(exc.cmd)}", file=sys.stderr)
        return exit_code
    finally:
        for process in reversed(processes):
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        continue
                    process.wait(timeout=5)
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        with (logs_dir / "compose-down.log").open("a", encoding="utf-8") as log_handle:
            env = os.environ.copy()
            env.update({str(key): str(value) for key, value in compose_down.get("env", {}).items()})
            subprocess.run(
                [str(item) for item in compose_down["argv"]],
                cwd=str(compose_down["cwd"]),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        for log_handle in log_handles:
            log_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
