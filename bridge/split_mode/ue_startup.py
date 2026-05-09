from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import time


def _count_session_successes(state_db: Path, run_id: str, ue_name: str) -> int:
    with sqlite3.connect(state_db) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM sim_event
            WHERE run_id = ?
              AND event_type = 'ueransim.tun_setup_success'
              AND entity_id LIKE ?
            """,
            (run_id, f"{ue_name}:%"),
        )
        row = cursor.fetchone()
    return int(row[0] if row else 0)


def _count_ue_errors(state_db: Path, run_id: str, ue_name: str) -> int:
    with sqlite3.connect(state_db) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM sim_event
            WHERE run_id = ?
              AND event_type = 'ueransim.error'
              AND entity_id = ?
            """,
            (run_id, ue_name),
        )
        row = cursor.fetchone()
    return int(row[0] if row else 0)


def _wait_for_ue_ready(
    state_db: Path,
    run_id: str,
    ue_name: str,
    expected_sessions: int,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    baseline_errors = _count_ue_errors(state_db, run_id, ue_name) if state_db.exists() else 0
    while time.monotonic() <= deadline:
        success_count = _count_session_successes(state_db, run_id, ue_name) if state_db.exists() else 0
        if success_count >= expected_sessions:
            return
        error_count = _count_ue_errors(state_db, run_id, ue_name) if state_db.exists() else 0
        if error_count > baseline_errors:
            raise RuntimeError(f"{ue_name} hit ueransim.error before reaching {expected_sessions} tun sessions")
        time.sleep(poll_interval_seconds)
    raise TimeoutError(f"{ue_name} did not reach {expected_sessions} tun sessions within {timeout_seconds} seconds")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start split-mode UEs sequentially")
    parser.add_argument("--compose-file", required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    parser.add_argument(
        "--ue",
        action="append",
        default=[],
        help="UE startup spec in the form service_name,ue_name,expected_session_count",
    )
    args = parser.parse_args(argv)

    state_db = Path(args.state_db).expanduser().resolve()
    ue_specs: list[tuple[str, str, int]] = []
    for raw_spec in args.ue:
        service_name, ue_name, expected_session_count = [part.strip() for part in str(raw_spec).split(",", 2)]
        ue_specs.append((service_name, ue_name, max(1, int(expected_session_count))))

    for service_name, ue_name, expected_sessions in ue_specs:
        subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                args.project_name,
                "-f",
                str(Path(args.compose_file).expanduser().resolve()),
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                service_name,
            ],
            check=True,
            text=True,
        )
        _wait_for_ue_ready(
            state_db,
            args.run_id,
            ue_name,
            expected_sessions,
            timeout_seconds=max(1.0, float(args.timeout_seconds)),
            poll_interval_seconds=max(0.05, float(args.poll_interval_seconds)),
        )
        print(
            json.dumps(
                {
                    "service_name": service_name,
                    "ue_name": ue_name,
                    "expected_sessions": expected_sessions,
                    "ready_sessions": _count_session_successes(state_db, args.run_id, ue_name),
                },
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
