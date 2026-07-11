
from __future__ import annotations

import io
import json
import shutil
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bridge.common.schema import TickSnapshot
from bridge.writer.cli import (
    _resolve_event_tick,
    _current_event_tick,
    _next_complete_jsonl_line,
)
from bridge.writer.local_store import SnapshotStore
from tests.test_schema import build_payload


class WriterStoreTest(unittest.TestCase):
    def test_snapshot_store_is_idempotent(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-store-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            snapshot = TickSnapshot.from_dict(build_payload())
            first = store.ingest_snapshot(snapshot)
            second = store.ingest_snapshot(snapshot)
            self.assertTrue(first["inserted"])
            self.assertFalse(second["inserted"])
            self.assertFalse(second["updated"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_snapshot_store_updates_existing_tick_when_payload_changes(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-store-update-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            payload = build_payload()
            first = store.ingest_snapshot(TickSnapshot.from_dict(payload))

            payload["flows"][0]["throughput_ul_mbps"] = 3.5
            payload["reward_inputs"]["throughput_score"] = 14.0
            second = store.ingest_snapshot(TickSnapshot.from_dict(payload))

            latest_path = root / "archive" / "run-1" / "latest.json"
            latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            self.assertTrue(first["inserted"])
            self.assertFalse(second["inserted"])
            self.assertTrue(second["updated"])
            self.assertAlmostEqual(latest_payload["flows"][0]["throughput_ul_mbps"], 3.5)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_snapshot_store_reports_latest_snapshot_tick(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-store-latest-tick-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            self.assertEqual(store.latest_snapshot_tick("run-1"), -1)

            payload = build_payload()
            payload["tick_index"] = 13
            store.ingest_snapshot(TickSnapshot.from_dict(payload))
            payload["tick_index"] = 21
            store.ingest_snapshot(TickSnapshot.from_dict(payload))

            self.assertEqual(store.latest_snapshot_tick("run-1"), 21)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_current_event_tick_follows_latest_ingested_snapshot_tick(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-event-tick-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            self.assertEqual(_current_event_tick(store, "run-1"), 0)

            payload = build_payload()
            payload["tick_index"] = 17
            store.ingest_snapshot(TickSnapshot.from_dict(payload))

            self.assertEqual(_current_event_tick(store, "run-1"), 17)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_snapshot_store_resolves_event_tick_from_snapshot_created_at(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-resolve-tick-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            payload = build_payload()
            payload["tick_index"] = 10
            store.ingest_snapshot(TickSnapshot.from_dict(payload))
            payload["tick_index"] = 20
            store.ingest_snapshot(TickSnapshot.from_dict(payload))

            with store._connect() as connection:  # test-only control of timeline
                base = datetime(2026, 4, 30, 3, 40, 2, 100000, tzinfo=timezone.utc)
                connection.execute(
                    "UPDATE sim_tick SET created_at=? WHERE run_id=? AND tick_index=?",
                    (base.isoformat(timespec="milliseconds"), "run-1", 10),
                )
                connection.execute(
                    "UPDATE sim_tick SET created_at=? WHERE run_id=? AND tick_index=?",
                    ((base + timedelta(milliseconds=500)).isoformat(timespec="milliseconds"), "run-1", 20),
                )

            resolved = store.resolve_tick_for_observed_at(
                "run-1",
                base + timedelta(milliseconds=650),
            )
            self.assertEqual(resolved, 20)
            early = store.resolve_tick_for_observed_at(
                "run-1",
                base - timedelta(milliseconds=50),
            )
            self.assertEqual(early, 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_resolve_event_tick_uses_compose_timestamp(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-resolve-line-tick-"))
        try:
            store = SnapshotStore(root / "state.db", root / "archive")
            payload = build_payload()
            payload["tick_index"] = 12
            store.ingest_snapshot(TickSnapshot.from_dict(payload))

            with store._connect() as connection:  # test-only control of timeline
                connection.execute(
                    "UPDATE sim_tick SET created_at=? WHERE run_id=? AND tick_index=?",
                    ("2026-04-30T03:40:02.300+00:00", "run-1", 12),
                )

            line = (
                "ue-ue1  | 2026-04-30T03:40:02.450000000Z "
                "[2026-04-30 03:40:02.450] [nas] [info] UE switches to state [MM-REGISTERED]"
            )
            self.assertEqual(_resolve_event_tick(store, "run-1", line), 12)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_follow_jsonl_buffers_partial_line_until_complete(self) -> None:
        pending, line = _next_complete_jsonl_line(io.StringIO('{"tick_index": 1'), "", flush_pending=False)

        self.assertEqual(pending, '{"tick_index": 1')
        self.assertIsNone(line)

        pending, line = _next_complete_jsonl_line(io.StringIO(', "run_id": "run-1"}\n'), pending, flush_pending=False)

        self.assertEqual(pending, "")
        self.assertEqual(line, '{"tick_index": 1, "run_id": "run-1"}\n')

    def test_follow_jsonl_flushes_final_line_at_stop_eof(self) -> None:
        pending, line = _next_complete_jsonl_line(io.StringIO('{"tick_index": 1}'), "", flush_pending=False)

        self.assertEqual(pending, '{"tick_index": 1}')
        self.assertIsNone(line)

        pending, line = _next_complete_jsonl_line(io.StringIO(""), pending, flush_pending=True)

        self.assertEqual(pending, "")
        self.assertEqual(line, '{"tick_index": 1}')



if __name__ == "__main__":
    unittest.main()
