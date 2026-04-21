from __future__ import annotations

import io
import shutil
import tempfile
import unittest
from pathlib import Path

from bridge.common.schema import TickSnapshot
from bridge.writer.cli import _next_complete_jsonl_line
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