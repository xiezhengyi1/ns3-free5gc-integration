from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

from bridge.orchestrator.metrics import (
    MetricsValidationError,
    load_gated_user_plane_metrics,
)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


class GatedUserPlaneMetricsTest(unittest.TestCase):
    def test_rejects_empty_logs_instead_of_reporting_a_valid_zero_packet_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(event_log, [])
            _write_jsonl(kpi_log, [])

            with self.assertRaisesRegex(MetricsValidationError, "no packet events"):
                load_gated_user_plane_metrics(event_log, kpi_log)

    def test_rejects_coercible_but_invalid_metric_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(
                event_log,
                [
                    {
                        "packet_id": 1.5,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "uplink",
                        "enqueue_ns3_us": 0,
                        "terminal_kind": "delivered",
                        "terminal_ns3_us": 100,
                        "delay_us": 100,
                        "ipdv_us": None,
                        "size_bytes": 125,
                        "warmup": "false",
                    }
                ],
            )
            _write_jsonl(kpi_log, [])

            with self.assertRaisesRegex(MetricsValidationError, "packet_id must be an integer"):
                load_gated_user_plane_metrics(event_log, kpi_log)

    def test_loads_packet_event_metrics_without_synthetic_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(
                event_log,
                [
                    {
                        "packet_id": 1,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "uplink",
                        "enqueue_ns3_us": 1000,
                        "terminal_kind": "delivered",
                        "terminal_ns3_us": 1200,
                        "delay_us": 200,
                        "ipdv_us": None,
                        "size_bytes": 1000,
                        "warmup": False,
                    },
                    {
                        "packet_id": 2,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "uplink",
                        "enqueue_ns3_us": 1100,
                        "terminal_kind": "dropped",
                        "terminal_ns3_us": 1300,
                        "reason": "rlc",
                        "delay_us": None,
                        "ipdv_us": None,
                        "size_bytes": 1000,
                        "warmup": False,
                    },
                ],
            )
            _write_jsonl(
                kpi_log,
                [
                    {
                        "epoch_id": 1,
                        "start_ns3_us": 1000,
                        "end_ns3_us": 2000,
                        "submitted_packets": 2,
                        "delivered_packets": 1,
                        "dropped_packets": 1,
                        "delivered_bytes": 1000,
                        "throughput_mbps": 8.0,
                        "delay_p50_us": 200,
                        "delay_p95_us": 200,
                        "delay_p99_us": 200,
                        "ipdv_p50_us": None,
                        "ipdv_p95_us": None,
                        "ipdv_p99_us": None,
                    },
                ],
            )

            report = load_gated_user_plane_metrics(event_log, kpi_log)

            self.assertEqual(report.total_submitted_packets, 2)
            self.assertEqual(report.total_delivered_packets, 1)
            self.assertEqual(report.total_dropped_packets, 1)
            self.assertEqual(report.epochs[0].throughput_mbps, 8.0)

    def test_rejects_kpi_values_that_do_not_match_packet_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(
                event_log,
                [
                    {
                        "packet_id": 1,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "downlink",
                        "enqueue_ns3_us": 10,
                        "terminal_kind": "delivered",
                        "terminal_ns3_us": 60,
                        "delay_us": 50,
                        "ipdv_us": None,
                        "size_bytes": 500,
                        "warmup": False,
                    }
                ],
            )
            _write_jsonl(
                kpi_log,
                [
                    {
                        "epoch_id": 1,
                        "start_ns3_us": 0,
                        "end_ns3_us": 100,
                        "submitted_packets": 1,
                        "delivered_packets": 1,
                        "dropped_packets": 0,
                        "delivered_bytes": 500,
                        "throughput_mbps": 40.0,
                        "delay_p50_us": 999,
                        "delay_p95_us": 999,
                        "delay_p99_us": 999,
                        "ipdv_p50_us": None,
                        "ipdv_p95_us": None,
                        "ipdv_p99_us": None,
                    }
                ],
            )

            with self.assertRaisesRegex(MetricsValidationError, "delay_p50_us"):
                load_gated_user_plane_metrics(event_log, kpi_log)

    def test_rejects_non_terminal_or_non_monotonic_packet_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(
                event_log,
                [
                    {
                        "packet_id": 1,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "uplink",
                        "enqueue_ns3_us": 100,
                        "terminal_kind": "delivered",
                        "terminal_ns3_us": 99,
                        "delay_us": -1,
                        "ipdv_us": None,
                        "size_bytes": 100,
                        "warmup": False,
                    }
                ],
            )
            _write_jsonl(kpi_log, [])

            with self.assertRaisesRegex(MetricsValidationError, "precedes enqueue"):
                load_gated_user_plane_metrics(event_log, kpi_log)

    def test_module_cli_reports_validated_packet_totals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_log = tmp_path / "packet-events.jsonl"
            kpi_log = tmp_path / "packet-kpis.jsonl"
            _write_jsonl(
                event_log,
                [
                    {
                        "packet_id": 1,
                        "epoch_id": 1,
                        "flow_id": "flow-1",
                        "direction": "uplink",
                        "enqueue_ns3_us": 0,
                        "terminal_kind": "delivered",
                        "terminal_ns3_us": 100,
                        "delay_us": 100,
                        "ipdv_us": None,
                        "size_bytes": 125,
                        "warmup": False,
                    }
                ],
            )
            _write_jsonl(
                kpi_log,
                [
                    {
                        "epoch_id": 1,
                        "start_ns3_us": 0,
                        "end_ns3_us": 1000,
                        "submitted_packets": 1,
                        "delivered_packets": 1,
                        "dropped_packets": 0,
                        "delivered_bytes": 125,
                        "throughput_mbps": 1.0,
                        "delay_p50_us": 100,
                        "delay_p95_us": 100,
                        "delay_p99_us": 100,
                        "ipdv_p50_us": None,
                        "ipdv_p95_us": None,
                        "ipdv_p99_us": None,
                    }
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "bridge.orchestrator.metrics",
                    "--event-log",
                    str(event_log),
                    "--kpi-log",
                    str(kpi_log),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            summary = json.loads(result.stdout)
            self.assertEqual(summary["submitted_packets"], 1)
            self.assertEqual(summary["delivered_packets"], 1)
            self.assertEqual(summary["dropped_packets"], 0)


if __name__ == "__main__":
    unittest.main()
