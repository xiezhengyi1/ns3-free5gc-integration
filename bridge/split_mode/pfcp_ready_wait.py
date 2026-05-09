from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
import time


def _count_ready_upfs(state_db: Path, run_id: str) -> int:
    with sqlite3.connect(state_db) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(DISTINCT entity_id)
            FROM sim_event
            WHERE run_id = ?
              AND event_type = 'free5gc.pfcp_association_ready'
            """,
            (run_id,),
        )
        row = cursor.fetchone()
    return int(row[0] if row else 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Wait until free5GC PFCP associations are ready")
    parser.add_argument("--state-db", required=True, help="sqlite path written by the event writer")
    parser.add_argument("--run-id", required=True, help="run identifier")
    parser.add_argument("--expected-upfs", required=True, type=int, help="expected number of ready UPFs")
    parser.add_argument("--timeout-seconds", type=float, default=60.0, help="max wait time")
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5, help="sqlite polling interval")
    args = parser.parse_args(argv)

    state_db = Path(args.state_db).expanduser().resolve()
    expected_upfs = max(1, int(args.expected_upfs))
    deadline = time.monotonic() + max(0.0, float(args.timeout_seconds))
    poll_interval = max(0.05, float(args.poll_interval_seconds))

    while time.monotonic() <= deadline:
        ready_upfs = _count_ready_upfs(state_db, args.run_id) if state_db.exists() else 0
        if ready_upfs >= expected_upfs:
            print(
                json.dumps(
                    {
                        "run_id": args.run_id,
                        "state_db": str(state_db),
                        "ready_upfs": ready_upfs,
                        "expected_upfs": expected_upfs,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        time.sleep(poll_interval)

    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "state_db": str(state_db),
                "ready_upfs": _count_ready_upfs(state_db, args.run_id) if state_db.exists() else 0,
                "expected_upfs": expected_upfs,
                "error": "timeout_waiting_for_pfcp_association_ready",
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
