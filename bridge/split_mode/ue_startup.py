from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time


def _session_entity_patterns(ue_name: str, service_name: str) -> tuple[str, ...]:
    """Return the entity-id forms emitted by Compose with or without a prefix."""
    aliases = tuple(dict.fromkeys(value for value in (ue_name, service_name) if value))
    patterns: list[str] = []
    for alias in aliases:
        patterns.extend((f"{alias}:%", f"%-{alias}:%"))
    return tuple(dict.fromkeys(patterns))


def _ue_error_entity_patterns(ue_name: str, service_name: str) -> tuple[str, ...]:
    aliases = tuple(dict.fromkeys(value for value in (ue_name, service_name) if value))
    patterns: list[str] = []
    for alias in aliases:
        patterns.extend((alias, f"%-{alias}"))
    return tuple(dict.fromkeys(patterns))


def _count_session_successes(
    state_db: Path,
    run_id: str,
    ue_name: str,
    *,
    service_name: str = "",
) -> int:
    patterns = _session_entity_patterns(ue_name, service_name)
    if not patterns:
        return 0
    connection = sqlite3.connect(state_db, timeout=30)
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM sim_event
            WHERE run_id = ?
              AND event_type = 'ueransim.tun_setup_success'
              AND ({' OR '.join('entity_id LIKE ?' for _ in patterns)})
            """,
            (run_id, *patterns),
        )
        row = cursor.fetchone()
    finally:
        connection.close()
    return int(row[0] if row else 0)


def _count_ue_errors(
    state_db: Path,
    run_id: str,
    ue_name: str,
    *,
    service_name: str = "",
) -> int:
    patterns = _ue_error_entity_patterns(ue_name, service_name)
    if not patterns:
        return 0
    connection = sqlite3.connect(state_db, timeout=30)
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM sim_event
            WHERE run_id = ?
              AND event_type = 'ueransim.error'
              AND ({' OR '.join('entity_id LIKE ?' for _ in patterns)})
            """,
            (run_id, *patterns),
        )
        row = cursor.fetchone()
    finally:
        connection.close()
    return int(row[0] if row else 0)


def _count_tun_interfaces(*, compose_file: Path, project_name: str, service_name: str) -> int:
    """Count live UERANSIM TUN interfaces without depending on log ingestion."""
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            project_name,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            service_name,
            "ip",
            "-o",
            "link",
            "show",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return 0
    return len(re.findall(r"^\d+:\s+(uesimtun\d+)(?:@[^:]+)?:", completed.stdout, flags=re.MULTILINE))


def _wait_for_ue_ready(
    state_db: Path,
    run_id: str,
    ue_name: str,
    expected_sessions: int,
    timeout_seconds: float,
    poll_interval_seconds: float,
    *,
    compose_file: Path | None = None,
    project_name: str = "",
    service_name: str = "",
) -> str:
    deadline = time.monotonic() + timeout_seconds
    baseline_errors = (
        _count_ue_errors(state_db, run_id, ue_name, service_name=service_name) if state_db.exists() else 0
    )
    next_tun_probe_at = 0.0
    while time.monotonic() <= deadline:
        success_count = (
            _count_session_successes(state_db, run_id, ue_name, service_name=service_name)
            if state_db.exists()
            else 0
        )
        if success_count >= expected_sessions:
            return "writer_event"
        now = time.monotonic()
        if (
            compose_file is not None
            and project_name
            and service_name
            and now >= next_tun_probe_at
        ):
            tun_count = _count_tun_interfaces(
                compose_file=compose_file,
                project_name=project_name,
                service_name=service_name,
            )
            if tun_count >= expected_sessions:
                return "tun_interface"
            # A writer normally reports readiness first.  Do not turn a missing
            # or delayed writer into one Docker exec per UE every 500 ms.
            next_tun_probe_at = now + 2.0
        error_count = (
            _count_ue_errors(state_db, run_id, ue_name, service_name=service_name)
            if state_db.exists()
            else 0
        )
        if error_count > baseline_errors:
            raise RuntimeError(f"{ue_name} hit ueransim.error before reaching {expected_sessions} tun sessions")
        time.sleep(poll_interval_seconds)
    raise TimeoutError(f"{ue_name} did not reach {expected_sessions} tun sessions within {timeout_seconds} seconds")


def _wait_for_started_ue(
    *,
    state_db: Path,
    run_id: str,
    compose_file: Path,
    project_name: str,
    ue_spec: tuple[str, str, int],
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> dict[str, object]:
    service_name, ue_name, expected_sessions = ue_spec
    ready_source = _wait_for_ue_ready(
        state_db,
        run_id,
        ue_name,
        expected_sessions,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        compose_file=compose_file,
        project_name=project_name,
        service_name=service_name,
    )
    return {
        "service_name": service_name,
        "ue_name": ue_name,
        "expected_sessions": expected_sessions,
        "ready_sessions": _count_session_successes(
            state_db,
            run_id,
            ue_name,
            service_name=service_name,
        ),
        "ready_source": ready_source,
    }


def _print_ue_ready(result: dict[str, object]) -> None:
    print(json.dumps(result, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start split-mode UEs sequentially")
    parser.add_argument("--compose-file", required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    parser.add_argument(
        "--already-started",
        action="store_true",
        help="only wait for the declared UEs; do not recreate their Compose services",
    )
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

    compose_file = Path(args.compose_file).expanduser().resolve()
    timeout_seconds = max(1.0, float(args.timeout_seconds))
    poll_interval_seconds = max(0.05, float(args.poll_interval_seconds))

    if args.already_started:
        # compose-up-ue has already created every UE.  Waiting serially here
        # turns one 90-second readiness window into N sequential windows.
        # Check all of them concurrently and report each completion.
        with ThreadPoolExecutor(max_workers=max(1, len(ue_specs))) as executor:
            future_map = {
                executor.submit(
                    _wait_for_started_ue,
                    state_db=state_db,
                    run_id=args.run_id,
                    compose_file=compose_file,
                    project_name=args.project_name,
                    ue_spec=ue_spec,
                    timeout_seconds=timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                ): ue_spec
                for ue_spec in ue_specs
            }
            for future in as_completed(future_map):
                _print_ue_ready(future.result())
        return 0

    for service_name, ue_name, expected_sessions in ue_specs:
        if not args.already_started:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-p",
                    args.project_name,
                    "-f",
                    str(compose_file),
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "--remove-orphans",
                    service_name,
                ],
                check=True,
                text=True,
            )
        _print_ue_ready(
            _wait_for_started_ue(
                state_db=state_db,
                run_id=args.run_id,
                compose_file=compose_file,
                project_name=args.project_name,
                ue_spec=(service_name, ue_name, expected_sessions),
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
