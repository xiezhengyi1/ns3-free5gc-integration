"""Aggregate split-mode control-plane state and ns-3 KPI into one JSONL stream."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


def _load_runtime_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "kpi_authority": "ns3",
            "active_session_count": 0,
            "registered_ue_count": 0,
            "sessions": [],
        }
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("runtime state file must contain a JSON object")
    return loaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write split-mode combined results JSONL")
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--runtime-state-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--poll-ms", type=int, default=200)
    args = parser.parse_args(argv)

    state_db = Path(args.state_db).expanduser().resolve()
    runtime_state_file = Path(args.runtime_state_file).expanduser().resolve()
    output_file = Path(args.output_file).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    last_tick = -1
    print(
        json.dumps(
            {
                "component": "split-results",
                "status": "started",
                "output_file": str(output_file),
                "runtime_state_file": str(runtime_state_file),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    while True:
        if not state_db.exists():
            time.sleep(max(0.05, args.poll_ms / 1000.0))
            continue
        with sqlite3.connect(state_db) as connection:
            row = connection.execute(
                """
                SELECT tick_index, sim_time_ms, payload_json
                FROM sim_tick
                WHERE run_id=?
                ORDER BY tick_index DESC
                LIMIT 1
                """,
                (args.run_id,),
            ).fetchone()
        if row is None:
            time.sleep(max(0.05, args.poll_ms / 1000.0))
            continue
        tick_index = int(row[0])
        if tick_index == last_tick:
            time.sleep(max(0.05, args.poll_ms / 1000.0))
            continue
        snapshot = json.loads(str(row[2]))
        runtime_state = _load_runtime_state(runtime_state_file)
        payload = {
            "run_id": args.run_id,
            "scenario_id": args.scenario_id,
            "tick_index": tick_index,
            "sim_time_ms": int(row[1]),
            "kpi_authority": "ns3",
            "control_plane_state": {
                "registered_ue_count": int(runtime_state.get("registered_ue_count", 0) or 0),
                "active_session_count": int(runtime_state.get("active_session_count", 0) or 0),
                "sessions": runtime_state.get("sessions", []),
            },
            "user_plane_kpi": {
                "kpis": snapshot.get("kpis", {}),
                "ues": snapshot.get("ues", []),
                "gnbs": snapshot.get("gnbs", []),
                "flows": snapshot.get("flows", []),
                "slices": snapshot.get("slices", []),
                "external_trace": snapshot.get("external_trace", {}),
            },
        }
        with output_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        last_tick = tick_index
        print(
            json.dumps(
                {
                    "component": "split-results",
                    "status": "wrote",
                    "tick_index": tick_index,
                    "sim_time_ms": int(row[1]),
                    "active_session_count": payload["control_plane_state"]["active_session_count"],
                    "flow_count": len(payload["user_plane_kpi"]["flows"]),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


if __name__ == "__main__":
    raise SystemExit(main())
