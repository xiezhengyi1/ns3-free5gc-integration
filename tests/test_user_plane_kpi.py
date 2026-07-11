from __future__ import annotations

import unittest

from bridge.user_plane.kpi import KpiError, PacketKpiCollector


class PacketKpiCollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.collector = PacketKpiCollector()

    def _submit(
        self,
        packet_id: int,
        enqueue_us: int,
        *,
        size: int = 1000,
        warmup: bool = False,
    ) -> None:
        self.collector.submitted(
            packet_id=packet_id,
            epoch_id=1,
            flow_id="flow-1",
            direction="uplink",
            enqueue_ns3_us=enqueue_us,
            size_bytes=size,
            warmup=warmup,
        )

    def test_computes_virtual_delay_and_ipdv(self) -> None:
        self._submit(1, 1000)
        self._submit(2, 2000)
        first = self.collector.delivered(packet_id=1, deliver_ns3_us=1120)
        second = self.collector.delivered(packet_id=2, deliver_ns3_us=2180)

        self.assertEqual(first.delay_us, 120)
        self.assertIsNone(first.ipdv_us)
        self.assertEqual(second.delay_us, 180)
        self.assertEqual(second.ipdv_us, 60)

    def test_summarizes_tick_percentiles_throughput_and_drops(self) -> None:
        for packet_id, enqueue_us in enumerate((1000, 2000, 3000), start=1):
            self._submit(packet_id, enqueue_us)
        self.collector.delivered(packet_id=1, deliver_ns3_us=1100)
        self.collector.delivered(packet_id=2, deliver_ns3_us=2200)
        self.collector.dropped(packet_id=3, drop_ns3_us=3300, reason="rlc")

        summary = self.collector.complete_epoch(
            epoch_id=1, start_ns3_us=0, end_ns3_us=10_000
        )

        self.assertEqual(summary.submitted_packets, 3)
        self.assertEqual(summary.delivered_packets, 2)
        self.assertEqual(summary.dropped_packets, 1)
        self.assertEqual(summary.delivered_bytes, 2000)
        self.assertEqual(summary.throughput_mbps, 1.6)
        self.assertEqual(summary.delay_p50_us, 100)
        self.assertEqual(summary.delay_p95_us, 200)
        self.assertEqual(summary.delay_p99_us, 200)
        self.assertEqual(summary.ipdv_p50_us, 100)

    def test_warmup_packets_do_not_fabricate_metric_samples(self) -> None:
        self._submit(1, 1000, warmup=True)
        self.collector.delivered(packet_id=1, deliver_ns3_us=1500)

        summary = self.collector.complete_epoch(
            epoch_id=1, start_ns3_us=0, end_ns3_us=2000
        )

        self.assertEqual(summary.delivered_packets, 1)
        self.assertIsNone(summary.delay_p50_us)
        self.assertIsNone(summary.ipdv_p50_us)

    def test_rejects_non_monotonic_or_duplicate_terminal_events(self) -> None:
        self._submit(1, 1000)
        with self.assertRaisesRegex(KpiError, "precedes"):
            self.collector.delivered(packet_id=1, deliver_ns3_us=999)
        self.collector.delivered(packet_id=1, deliver_ns3_us=1100)
        with self.assertRaisesRegex(KpiError, "terminal"):
            self.collector.dropped(packet_id=1, drop_ns3_us=1200, reason="late")

    def test_epoch_completion_enforces_packet_conservation(self) -> None:
        self._submit(1, 1000)

        with self.assertRaisesRegex(KpiError, "not terminal"):
            self.collector.complete_epoch(epoch_id=1, start_ns3_us=0, end_ns3_us=2000)


if __name__ == "__main__":
    unittest.main()
