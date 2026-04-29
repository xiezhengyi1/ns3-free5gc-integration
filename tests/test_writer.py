from __future__ import annotations

import io
import json
import shutil
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

from bridge.common.schema import TickSnapshot
from bridge.writer.cli import RealTrafficStateReader, _merge_real_traffic_state, _next_complete_jsonl_line
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

    def test_merge_real_traffic_state_preserves_snapshot_loss_metrics(self) -> None:
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
            self.assertEqual(flow.telemetry["packet_received"], 12)
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
