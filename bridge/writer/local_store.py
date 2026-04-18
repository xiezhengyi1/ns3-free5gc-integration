"""Local archival store for tick snapshots and events."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from bridge.common.schema import SimEvent, TickSnapshot


class SnapshotStore:
    def __init__(self, state_db: str | Path, archive_dir: str | Path) -> None:
        self.state_db = Path(state_db).expanduser().resolve()
        self.archive_dir = Path(archive_dir).expanduser().resolve()
        self.state_db.parent.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.state_db, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sim_run (
                    run_id TEXT PRIMARY KEY,
                    scenario_id TEXT NOT NULL,
                    seed INTEGER,
                    topology_version TEXT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sim_tick (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    tick_index INTEGER NOT NULL,
                    sim_time_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_id, tick_index)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sim_event (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    tick_index INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

    def upsert_run(
        self,
        run_id: str,
        scenario_id: str,
        seed: int | None = None,
        topology_version: str = "v1",
        status: str = "running",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO sim_run (run_id, scenario_id, seed, topology_version, started_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    scenario_id=excluded.scenario_id,
                    seed=COALESCE(excluded.seed, sim_run.seed),
                    topology_version=excluded.topology_version,
                    status=excluded.status
                """,
                (run_id, scenario_id, seed, topology_version, now, status),
            )

    def mark_run_status(self, run_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._connect() as connection:
            connection.execute(
                "UPDATE sim_run SET status=?, ended_at=? WHERE run_id=?",
                (status, now, run_id),
            )

    def ingest_snapshot(
        self,
        snapshot: TickSnapshot,
        seed: int | None = None,
        topology_version: str = "v1",
    ) -> dict[str, object]:
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        payload = snapshot.to_dict()
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        self.upsert_run(snapshot.run_id, snapshot.scenario_id, seed, topology_version)

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO sim_tick (run_id, tick_index, sim_time_ms, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot.run_id,
                    snapshot.tick_index,
                    snapshot.sim_time_ms,
                    payload_json,
                    created_at,
                ),
            )
            inserted = cursor.rowcount == 1

        archive_path = self.archive_dir / snapshot.run_id / "ticks" / f"{snapshot.tick_index:06d}.json"
        latest_path = self.archive_dir / snapshot.run_id / "latest.json"
        if inserted:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_text(payload_json + "\n", encoding="utf-8")
            latest_path.write_text(payload_json + "\n", encoding="utf-8")

        return {
            "inserted": inserted,
            "run_id": snapshot.run_id,
            "tick_index": snapshot.tick_index,
            "archive_path": str(archive_path),
            "latest_path": str(latest_path),
        }

    def append_event(self, event: SimEvent) -> dict[str, object]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO sim_event (run_id, tick_index, event_type, entity_type, entity_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.run_id,
                    event.tick_index,
                    event.event_type,
                    event.entity_type,
                    event.entity_id,
                    json.dumps(event.payload_json, ensure_ascii=False, sort_keys=True),
                    event.created_at,
                ),
            )
        return {
            "event_id": cursor.lastrowid,
            "run_id": event.run_id,
            "tick_index": event.tick_index,
            "event_type": event.event_type,
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
        }