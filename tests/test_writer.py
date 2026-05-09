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
    RealTrafficStateReader,
    _current_event_tick,
    _merge_real_traffic_state,
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

    def test_merge_real_traffic_state_overrides_metrics_from_exact_real_traffic_tick(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-real-traffic-"))
        try:
            state_file = root / "real-traffic.jsonl"
            state_file.write_text(
                json.dumps(
                    {
                        "tick_index": 0,
                        "sim_time_ms": 100,
                        "flows": [
                            {
                                "flow_id": "flow-1",
                                "ue_name": "ue-1",
                                "session_ref": "session-1",
                                "container": "ue-container",
                                "interface": "uesimtun0",
                                "ue_ip": "10.60.0.1",
                                "dl_container": "upf-1",
                                "source_port": 15000,
                                "destination_port": 5000,
                                "packet_size_bytes": 1000,
                                "ul_packets_sent": 10,
                                "dl_packets_sent": 5,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            payload = build_payload()
            payload["flows"][0]["loss_rate"] = 0.2
            payload["flows"][0]["throughput_ul_mbps"] = 9.9
            payload["flows"][0]["throughput_dl_mbps"] = 8.8
            payload["flows"][0]["telemetry"] = {"loss_rate": 0.2, "packet_sent": 999, "packet_received": 12}
            payload["flows"][0]["sla"] = {"loss_rate": 0.02}
            snapshot = TickSnapshot.from_dict(payload)

            merged = _merge_real_traffic_state(
                snapshot,
                Namespace(
                    real_traffic_state_file=str(state_file),
                    real_traffic_timeout_seconds=0.1,
                    tick_ms=100,
                ),
            )

            flow = merged.flows[0]
            self.assertEqual(flow.traffic["direction"], "bidirectional")
            self.assertEqual(flow.traffic["five_tuple"]["source_ip"], "10.60.0.1")
            self.assertAlmostEqual(flow.throughput_ul_mbps, 0.8)
            self.assertAlmostEqual(flow.throughput_dl_mbps, 0.4)
            self.assertAlmostEqual(flow.loss_rate, 0.2)
            self.assertEqual(flow.telemetry["packet_sent"], 15)
            self.assertEqual(flow.telemetry["packet_received"], 15)
            self.assertAlmostEqual(flow.telemetry["throughput_ul"], 0.8)
            self.assertAlmostEqual(flow.telemetry["throughput_dl"], 0.4)
            self.assertEqual(merged.ues[0].ip_address, "10.60.0.1")
            self.assertAlmostEqual(merged.kpis["active_flows"], 1.0)
            self.assertAlmostEqual(merged.kpis["mean_loss_rate"], flow.loss_rate)
            self.assertAlmostEqual(merged.kpis["throughput_ul_mbps_total"], 0.8)
            self.assertAlmostEqual(merged.kpis["throughput_dl_mbps_total"], 0.4)
            self.assertAlmostEqual(merged.reward_inputs["loss_penalty"], flow.loss_rate)
            self.assertAlmostEqual(merged.reward_inputs["throughput_score"], 1.2)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_real_traffic_state_reader_tracks_appended_ticks_incrementally(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-real-reader-"))
        try:
            state_file = root / "real-traffic.jsonl"
            state_file.write_text(
                "\n".join(
                    [
                        json.dumps({"tick_index": 0, "sim_time_ms": 100, "flows": []}, ensure_ascii=False),
                        json.dumps({"tick_index": 1, "sim_time_ms": 200, "flows": []}, ensure_ascii=False),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            reader = RealTrafficStateReader(str(state_file), timeout_seconds=0.01)
            self.assertEqual(reader.payload_for_tick(0)["tick_index"], 0)
            self.assertEqual(reader.payload_for_tick(1)["tick_index"], 1)

            with state_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"tick_index": 2, "sim_time_ms": 300, "flows": []}, ensure_ascii=False) + "\n")

            self.assertEqual(reader.payload_for_tick(2)["tick_index"], 2)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_real_traffic_state_reader_returns_none_when_exact_tick_is_missing(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-real-reader-stale-"))
        try:
            state_file = root / "real-traffic.jsonl"
            state_file.write_text(
                json.dumps({"tick_index": 0, "sim_time_ms": 100, "flows": []}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            reader = RealTrafficStateReader(str(state_file), timeout_seconds=0.01)
            self.assertEqual(reader.payload_for_tick(0)["tick_index"], 0)
            self.assertIsNone(reader.payload_for_tick(1))
            self.assertEqual(reader.payload_status_for_tick(1)[0], "pending")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_real_traffic_state_reader_reports_skipped_when_source_has_advanced_past_target_tick(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-real-reader-skipped-"))
        try:
            state_file = root / "real-traffic.jsonl"
            state_file.write_text(
                "\n".join(
                    [
                        json.dumps({"tick_index": 1, "sim_time_ms": 200, "flows": []}, ensure_ascii=False),
                        json.dumps({"tick_index": 17, "sim_time_ms": 1800, "flows": []}, ensure_ascii=False),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            reader = RealTrafficStateReader(str(state_file), timeout_seconds=0.01)
            self.assertIsNone(reader.payload_for_tick(8))
            self.assertEqual(reader.payload_status_for_tick(8)[0], "skipped")
            self.assertEqual(reader.payload_for_tick(17)["tick_index"], 17)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_merge_real_traffic_state_reuses_incremental_reader_for_multiple_ticks(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="writer-real-traffic-incremental-"))
        try:
            state_file = root / "real-traffic.jsonl"
            state_file.write_text(
                json.dumps(
                    {
                        "tick_index": 0,
                        "sim_time_ms": 100,
                        "flows": [
                            {
                                "flow_id": "flow-1",
                                "ue_name": "ue-1",
                                "session_ref": "session-1",
                                "container": "ue-container",
                                "interface": "uesimtun0",
                                "ue_ip": "10.60.0.1",
                                "dl_container": "upf-1",
                                "source_port": 15000,
                                "destination_port": 5000,
                                "packet_size_bytes": 1000,
                                "ul_packets_sent": 10,
                                "dl_packets_sent": 5,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            args = Namespace(
                real_traffic_state_file=str(state_file),
                real_traffic_timeout_seconds=0.01,
                tick_ms=100,
            )

            payload = build_payload()
            payload["tick_index"] = 0
            payload["sim_time_ms"] = 100
            payload["flows"][0]["sla"] = {"loss_rate": 0.02}
            first = _merge_real_traffic_state(TickSnapshot.from_dict(payload), args)
            self.assertAlmostEqual(first.flows[0].throughput_ul_mbps, 0.8)

            with state_file.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "tick_index": 1,
                            "sim_time_ms": 200,
                            "flows": [
                                {
                                    "flow_id": "flow-1",
                                    "ue_name": "ue-1",
                                    "session_ref": "session-1",
                                    "container": "ue-container",
                                    "interface": "uesimtun0",
                                    "ue_ip": "10.60.0.1",
                                    "dl_container": "upf-1",
                                    "source_port": 15000,
                                    "destination_port": 5000,
                                    "packet_size_bytes": 1000,
                                    "ul_packets_sent": 20,
                                    "dl_packets_sent": 10,
                                }
                            ],
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            payload["tick_index"] = 1
            payload["sim_time_ms"] = 200
            second = _merge_real_traffic_state(TickSnapshot.from_dict(payload), args)
            self.assertAlmostEqual(second.flows[0].throughput_ul_mbps, 1.6)
            self.assertIsInstance(getattr(args, "_real_traffic_reader", None), RealTrafficStateReader)
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
