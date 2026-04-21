#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bridge.common.schema import TickSnapshot
from bridge.common.ids import generate_run_id
from bridge.common.scenario import load_scenario
from bridge.common.topology import resolve_scenario_topology
from bridge.orchestrator.config_renderer import render_run_assets


def _command_by_name(manifest: dict[str, object], name: str) -> dict[str, object]:
    return next(command for command in manifest["commands"] if command["name"] == name)


def _command_env(command: dict[str, object]) -> dict[str, str]:
    env = os.environ.copy()
    env.update({str(key): str(value) for key, value in command.get("env", {}).items()})
    return env


def _run_command(command: dict[str, object], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command["argv"],
        cwd=command["cwd"],
        env=_command_env(command),
        text=True,
        check=check,
    )


def _start_background_command(
    command: dict[str, object],
    *,
    log_path: Path,
    ensure_graph_schema: bool = False,
) -> tuple[subprocess.Popen[str], Any]:
    argv = list(command["argv"])
    if ensure_graph_schema and "--graph-db-url" in argv and "--ensure-graph-schema" not in argv:
        argv.append("--ensure-graph-schema")
    log_handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        argv,
        cwd=command["cwd"],
        env=_command_env(command),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, log_handle


def _background_log_key(command_name: str) -> str:
    if command_name == "writer-follow-free5gc":
        return "free5gc"
    if command_name == "writer-follow-ueransim":
        return "ueransim"
    if command_name == "writer-follow-ns3":
        return "ns3"
    return command_name.replace("-", "_")


def _extract_option(argv: list[str], option: str) -> str | None:
    if option not in argv:
        return None
    index = argv.index(option)
    if index + 1 >= len(argv):
        return None
    return argv[index + 1]


def _count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _query_sqlite_counts(state_db: Path, run_id: str) -> dict[str, object]:
    connection = sqlite3.connect(state_db)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM sim_event WHERE run_id = ?", (run_id,))
        event_count = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM sim_tick WHERE run_id = ?", (run_id,))
        tick_count = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT MIN(tick_index), MAX(tick_index) FROM sim_tick WHERE run_id = ?",
            (run_id,),
        )
        min_tick, max_tick = cursor.fetchone()
        cursor.execute(
            "SELECT event_type, COUNT(*) FROM sim_event WHERE run_id = ? GROUP BY event_type ORDER BY event_type",
            (run_id,),
        )
        event_type_counts = {event_type: count for event_type, count in cursor.fetchall()}
        cursor.execute(
            "SELECT event_type, entity_id, payload_json FROM sim_event WHERE run_id = ? ORDER BY created_at ASC",
            (run_id,),
        )
        entities_by_event_type: dict[str, set[str]] = {}
        services_by_event_type: dict[str, set[str]] = {}
        for event_type, entity_id, payload_json in cursor.fetchall():
            entities_by_event_type.setdefault(str(event_type), set()).add(str(entity_id))
            try:
                decoded_payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                decoded_payload = {}
            service_name = decoded_payload.get("service")
            if isinstance(service_name, str) and service_name:
                services_by_event_type.setdefault(str(event_type), set()).add(service_name)
        return {
            "event_count": event_count,
            "tick_count": tick_count,
            "min_tick": min_tick,
            "max_tick": max_tick,
            "event_type_counts": event_type_counts,
            "entities_by_event_type": {
                event_type: sorted(values)
                for event_type, values in entities_by_event_type.items()
            },
            "services_by_event_type": {
                event_type: sorted(values)
                for event_type, values in services_by_event_type.items()
            },
        }
    finally:
        connection.close()


def _read_first_snapshot(snapshot_file: Path) -> TickSnapshot | None:
    if not snapshot_file.exists():
        return None
    with snapshot_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            return TickSnapshot.from_dict(json.loads(line))
    return None


def _summarize_ns3_snapshot(
    snapshot_file: Path,
    scenario: Any,
) -> dict[str, object]:
    snapshot = _read_first_snapshot(snapshot_file)
    if snapshot is None:
        return {"present": False}

    resolved_topology = resolve_scenario_topology(scenario)
    gnb_index_by_name = {gnb.name: index for index, gnb in enumerate(scenario.gnbs, start=1)}
    expected_ue_bindings = {
        ue.supi: f"gnb-{gnb_index_by_name[resolved_topology.ue_to_gnb[ue.name]]}"
        for ue in scenario.ues
    }
    expected_gnb_upf_map = {
        f"gnb-{index}": resolved_topology.gnb_to_upf[gnb.name]
        for index, gnb in enumerate(scenario.gnbs, start=1)
    }
    actual_ue_bindings = {
        ue.supi: ue.gnb_id
        for ue in snapshot.ues
    }
    actual_gnb_upf_map = {
        gnb.gnb_id: gnb.dst_upf
        for gnb in snapshot.gnbs
        if gnb.dst_upf is not None
    }
    return {
        "present": True,
        "tick_index": snapshot.tick_index,
        "sim_time_ms": snapshot.sim_time_ms,
        "ue_bindings": actual_ue_bindings,
        "expected_ue_bindings": expected_ue_bindings,
        "gnb_upf_map": actual_gnb_upf_map,
        "expected_gnb_upf_map": expected_gnb_upf_map,
        "matches_expected": (
            actual_ue_bindings == expected_ue_bindings
            and actual_gnb_upf_map == expected_gnb_upf_map
        ),
    }


def _expected_physical_upf_count(manifest: dict[str, object]) -> int:
    core_services = manifest.get("core_services", [])
    return sum(
        1
        for service_name in core_services
        if isinstance(service_name, str) and "upf" in service_name.lower()
    )


def _query_graph_counts(graph_db_url: str, run_id: str) -> dict[str, object]:
    import psycopg

    with psycopg.connect(graph_db_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    MIN((graph_summary->>'tick_index')::integer),
                    MAX((graph_summary->>'tick_index')::integer)
                FROM network_graph_snapshot
                WHERE graph_summary->>'run_id' = %s
                """,
                (run_id,),
            )
            snapshot_count, min_tick, max_tick = cursor.fetchone()
            cursor.execute(
                """
                SELECT
                    (graph_summary->>'tick_index')::integer AS tick_index,
                    (graph_summary->>'node_count')::integer AS node_count,
                    (graph_summary->>'edge_count')::integer AS edge_count,
                    (graph_summary->>'metric_count')::integer AS metric_count,
                    (graph_summary->>'delta_node_count')::integer AS delta_node_count,
                    (graph_summary->>'delta_edge_count')::integer AS delta_edge_count,
                    COALESCE((graph_summary->>'deleted_node_count')::integer, 0) AS deleted_node_count,
                    COALESCE((graph_summary->>'deleted_edge_count')::integer, 0) AS deleted_edge_count,
                    COALESCE(graph_summary->>'write_mode', 'snapshot') AS write_mode
                FROM network_graph_snapshot
                WHERE graph_summary->>'run_id' = %s
                ORDER BY (graph_summary->>'tick_index')::integer DESC
                LIMIT 5
                """,
                (run_id,),
            )
            recent = [
                {
                    "tick_index": row[0],
                    "node_count": row[1],
                    "edge_count": row[2],
                    "metric_count": row[3],
                    "delta_node_count": row[4],
                    "delta_edge_count": row[5],
                    "deleted_node_count": row[6],
                    "deleted_edge_count": row[7],
                    "write_mode": row[8],
                }
                for row in reversed(cursor.fetchall())
            ]
    return {
        "snapshot_count": int(snapshot_count),
        "min_tick": min_tick,
        "max_tick": max_tick,
        "recent_snapshots": recent,
    }


def _wait_for_ingestion(
    *,
    state_db: Path,
    snapshot_file: Path,
    run_id: str,
    graph_db_url: str | None,
    timeout_seconds: float,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    expected_ticks = 0
    last_sqlite: dict[str, object] = {
        "event_count": 0,
        "tick_count": 0,
        "min_tick": None,
        "max_tick": None,
        "event_type_counts": {},
    }
    last_graph: dict[str, object] | None = None

    while time.monotonic() < deadline:
        expected_ticks = _count_jsonl_lines(snapshot_file)
        if state_db.exists():
            last_sqlite = _query_sqlite_counts(state_db, run_id)
        if graph_db_url and expected_ticks > 0:
            last_graph = _query_graph_counts(graph_db_url, run_id)
        if expected_ticks > 0 and last_sqlite["tick_count"] >= expected_ticks:
            if graph_db_url is None or (last_graph and last_graph["snapshot_count"] >= expected_ticks):
                return {
                    "expected_ticks": expected_ticks,
                    "sqlite": last_sqlite,
                    "graph": last_graph,
                }
        time.sleep(1.0)

    raise TimeoutError(
        json.dumps(
            {
                "expected_ticks": expected_ticks,
                "sqlite": last_sqlite,
                "graph": last_graph,
            },
            ensure_ascii=False,
        )
    )


def _assert_success(
    summary: dict[str, object],
    *,
    expected_ues: int,
    expected_gnbs: int,
    expected_upfs: int,
    expected_gnb_services: set[str],
) -> None:
    sqlite_summary = summary["sqlite"]
    event_type_counts = sqlite_summary["event_type_counts"]
    entities_by_event_type = sqlite_summary.get("entities_by_event_type", {})
    services_by_event_type = sqlite_summary.get("services_by_event_type", {})
    if sqlite_summary["tick_count"] <= 0:
        raise RuntimeError("ns-3 tick ingestion did not produce any sim_tick rows")
    if len(entities_by_event_type.get("ueransim.registration_success", [])) < expected_ues:
        raise RuntimeError("UERANSIM registration_success count is lower than expected UE count")
    if len(entities_by_event_type.get("ueransim.tun_setup_success", [])) < expected_ues:
        raise RuntimeError("UERANSIM tun_setup_success count is lower than expected UE count")
    ng_setup_services = set(services_by_event_type.get("ueransim.ng_setup_success", []))
    if len(ng_setup_services) < expected_gnbs or not expected_gnb_services.issubset(ng_setup_services):
        raise RuntimeError(
            "UERANSIM gNB NG Setup events do not cover all expected gNB services"
        )
    if event_type_counts.get("free5gc.pfcp_association_ready", 0) < expected_upfs:
        raise RuntimeError("free5GC PFCP association count is lower than expected UPF count")

    graph_summary = summary.get("graph")
    if graph_summary is not None and graph_summary["snapshot_count"] < sqlite_summary["tick_count"]:
        raise RuntimeError("graph snapshot count is lower than ingested sim_tick count")

    ns3_summary = summary.get("ns3")
    if ns3_summary is not None:
        if not ns3_summary.get("present"):
            raise RuntimeError("ns-3 snapshot file is empty")
        if not ns3_summary.get("matches_expected"):
            raise RuntimeError("ns-3 first snapshot does not match resolved graph/policy bindings")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run and validate a one-click graph smoke scenario")
    parser.add_argument(
        "--scenario",
        default="scenarios/baseline_multi_ue.yaml",
        help="scenario YAML to render and execute",
    )
    parser.add_argument("--run-id", help="explicit run identifier")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=180.0,
        help="max time to wait for ns-3 and writer ingestion to settle",
    )
    parser.add_argument(
        "--no-teardown",
        action="store_true",
        help="leave docker stack and follower processes running for debugging",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    scenario_path = Path(args.scenario)
    if not scenario_path.is_absolute():
        scenario_path = project_root / scenario_path
    scenario = load_scenario(scenario_path)
    resolved_topology = resolve_scenario_topology(scenario)
    run_id = args.run_id or generate_run_id(f"{scenario.scenario_id}-graph-smoke")
    rendered = render_run_assets(project_root, scenario, run_id)
    manifest = rendered.manifest.to_dict()

    logs_dir = rendered.run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    state_db = Path(manifest["state_db"])
    archive_dir = Path(manifest["archive_dir"])
    snapshot_file = Path(manifest["snapshot_file"])
    summary_path = rendered.run_dir / "smoke-summary.json"

    background_processes: list[tuple[subprocess.Popen[str], Any]] = []
    should_teardown = not args.no_teardown
    graph_db_url: str | None = None
    log_paths: dict[str, Path] = {}

    try:
        compose_down = _command_by_name(manifest, "compose-down")
        _run_command(compose_down, check=False)

        if state_db.exists():
            state_db.unlink()
        if archive_dir.exists():
            shutil.rmtree(archive_dir)
        archive_dir.mkdir(parents=True, exist_ok=True)
        if snapshot_file.exists():
            snapshot_file.unlink()
        snapshot_file.parent.mkdir(parents=True, exist_ok=True)

        for command in manifest["commands"]:
            command_name = str(command["name"])
            if command_name == "compose-down":
                continue
            if command.get("background"):
                log_key = _background_log_key(command_name)
                log_path = logs_dir / f"writer-{log_key}.log"
                ensure_graph_schema = command_name == "writer-follow-ns3"
                process, log_handle = _start_background_command(
                    command,
                    log_path=log_path,
                    ensure_graph_schema=ensure_graph_schema,
                )
                background_processes.append((process, log_handle))
                log_paths[log_key] = log_path
                if command_name == "writer-follow-ns3":
                    graph_db_url = _extract_option(list(command["argv"]), "--graph-db-url")
                continue
            _run_command(command)

        summary = _wait_for_ingestion(
            state_db=state_db,
            snapshot_file=snapshot_file,
            run_id=run_id,
            graph_db_url=graph_db_url,
            timeout_seconds=args.timeout_seconds,
        )
        summary["ns3"] = _summarize_ns3_snapshot(snapshot_file, scenario)
        _assert_success(
            summary,
            expected_ues=len(scenario.ues),
            expected_gnbs=len(scenario.gnbs),
            expected_upfs=_expected_physical_upf_count(manifest),
            expected_gnb_services=set(manifest["service_map"]["gnb"].values()),
        )

        payload = {
            "run_id": run_id,
            "scenario_id": scenario.scenario_id,
            "scenario_path": str(scenario_path),
            "compose_project_name": manifest["compose_project_name"],
            "expected_ues": len(scenario.ues),
            "expected_gnbs": len(scenario.gnbs),
            "expected_logical_upfs": len(scenario.upfs),
            "expected_physical_upfs": _expected_physical_upf_count(manifest),
            "resolved_topology": resolved_topology.to_dict(),
            "ran_services": manifest["ran_services"],
            "snapshot_file": str(snapshot_file),
            "state_db": str(state_db),
            "archive_dir": str(archive_dir),
            "logs": {
                key: str(path)
                for key, path in sorted(log_paths.items())
            },
            **summary,
        }
        summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    finally:
        for process, log_handle in reversed(background_processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            log_handle.close()
        if should_teardown:
            _run_command(_command_by_name(manifest, "compose-down"), check=False)


if __name__ == "__main__":
    raise SystemExit(main())