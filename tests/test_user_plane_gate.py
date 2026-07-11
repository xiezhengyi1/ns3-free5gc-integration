from __future__ import annotations

import ipaddress
import unittest

from bridge.user_plane.coordinator import PacketCoordinator
from bridge.user_plane.frame_port import MemoryFramePort
from bridge.user_plane.gate import FrameGate
from bridge.user_plane.gtpu import (
    Classification,
    DecisionKind,
    Direction,
    GtpuPacket,
    FlowBinding,
    FlowClassifier,
)
from bridge.user_plane.kpi import PacketKpiCollector
from bridge.user_plane.protocol import Message, MessageType
from bridge.user_plane.routing import EndpointLink, EndpointRouter
from tests.test_gtpu import _frame


_PACKET = GtpuPacket(
    teid=7,
    qfi=9,
    outer_src=ipaddress.IPv4Address("10.0.0.2"),
    outer_dst=ipaddress.IPv4Address("10.0.0.3"),
    inner_src=ipaddress.IPv4Address("10.60.0.1"),
    inner_dst=ipaddress.IPv4Address("192.0.2.1"),
    inner_protocol=17,
    inner_src_port=4000,
    inner_dst_port=5000,
)


def _two_port_route(ingress_port: str, _frame: bytes) -> tuple[str, ...]:
    return {"gnb": ("upf",), "upf": ("gnb",)}[ingress_port]


class _Classifier:
    def classify(self, frame: bytes) -> Classification:
        if frame.startswith(b"control"):
            return Classification(DecisionKind.CONTROL_BYPASS)
        if frame.startswith(b"managed"):
            return Classification(
                DecisionKind.MANAGED,
                packet=_PACKET,
                flow_id="flow-1",
                direction=Direction.UPLINK,
            )
        if frame.startswith(b"malformed"):
            return Classification(DecisionKind.MALFORMED, reason="bad frame")
        return Classification(DecisionKind.UNMAPPED, reason="no binding")


class UserPlaneGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.gnb = MemoryFramePort()
        self.upf = MemoryFramePort()
        self.messages: list[Message] = []
        self.coordinator = PacketCoordinator(
            max_pending_packets=8,
            max_pending_bytes=1024,
        )
        self.kpi = PacketKpiCollector()
        self.gate = FrameGate(
            classifier=_Classifier(),
            coordinator=self.coordinator,
            kpi=self.kpi,
            ports={"gnb": self.gnb, "upf": self.upf},
            peer_send=self.messages.append,
            virtual_expiry_us_by_flow={"flow-1": 1000},
            egress_resolver=_two_port_route,
        )
        self.epoch = self.coordinator.begin_epoch(start_ns3_us=100)

    def _capture_managed(self, frame: bytes = b"managed-packet") -> int:
        packet_id = self.gate.capture(
            frame=frame,
            ingress_port="gnb",
            epoch_id=self.epoch,
            ns3_time_us=100,
        )
        assert packet_id is not None
        return packet_id

    def test_control_frame_passes_immediately(self) -> None:
        result = self.gate.capture(
            frame=b"control-arp",
            ingress_port="gnb",
            epoch_id=self.epoch,
            ns3_time_us=100,
        )

        self.assertIsNone(result)
        self.assertEqual(self.upf.written_frames, [b"control-arp"])
        self.assertEqual(self.messages, [])

    def test_pre_epoch_mode_bypasses_only_control_frames(self) -> None:
        self.gate.capture_pre_epoch(frame=b"control-arp", ingress_port="gnb")
        self.gate.capture_pre_epoch(frame=b"managed-packet", ingress_port="gnb")

        self.assertEqual(self.upf.written_frames, [b"control-arp"])
        self.assertEqual(self.gate.stats.bypassed, 1)
        self.assertEqual(self.gate.stats.dropped, 1)

    def test_managed_frame_waits_for_delivery_result(self) -> None:
        packet_id = self._capture_managed()

        self.assertEqual(self.upf.written_frames, [])
        self.assertEqual(self.messages[0].message_type, MessageType.PACKET_ENQUEUE)
        self.assertEqual(self.messages[0].payload["packet_id"], packet_id)

        self.gate.handle_peer_message(
            Message(
                MessageType.PACKET_DELIVER,
                sequence=2,
                payload={
                    "packet_id": packet_id,
                    "epoch_id": self.epoch,
                    "ns3_time_us": 250,
                },
            )
        )

        self.assertEqual(self.upf.written_frames, [b"managed-packet"])
        self.assertEqual(self.coordinator.pending_packets, 0)

    def test_drop_discards_original_frame(self) -> None:
        packet_id = self._capture_managed()

        self.gate.handle_peer_message(
            Message(
                MessageType.PACKET_DROP,
                sequence=3,
                payload={
                    "packet_id": packet_id,
                    "epoch_id": self.epoch,
                    "ns3_time_us": 220,
                    "reason": "rlc-discard",
                },
            )
        )

        self.assertEqual(self.upf.written_frames, [])
        self.assertEqual(self.gate.stats.dropped, 1)

    def test_unmapped_and_malformed_gpdu_fail_closed(self) -> None:
        for frame in (b"unmapped", b"malformed"):
            self.gate.capture(
                frame=frame,
                ingress_port="gnb",
                epoch_id=self.epoch,
                ns3_time_us=100,
            )

        self.assertEqual(self.upf.written_frames, [])
        self.assertEqual(self.gate.stats.unmapped, 1)
        self.assertEqual(self.gate.stats.malformed, 1)

    def test_capacity_overflow_discards_new_frame(self) -> None:
        coordinator = PacketCoordinator(max_pending_packets=1, max_pending_bytes=1024)
        epoch = coordinator.begin_epoch(start_ns3_us=100)
        gate = FrameGate(
            classifier=_Classifier(),
            coordinator=coordinator,
            kpi=PacketKpiCollector(),
            ports={"gnb": self.gnb, "upf": self.upf},
            peer_send=self.messages.append,
            virtual_expiry_us_by_flow={"flow-1": 1000},
            egress_resolver=_two_port_route,
        )
        gate.capture(
            frame=b"managed-first",
            ingress_port="gnb",
            epoch_id=epoch,
            ns3_time_us=100,
        )

        result = gate.capture(
            frame=b"managed-second",
            ingress_port="gnb",
            epoch_id=epoch,
            ns3_time_us=100,
        )

        self.assertIsNone(result)
        self.assertEqual(gate.stats.capacity_drops, 1)

    def test_peer_disconnect_discards_all_pending_frames(self) -> None:
        self._capture_managed(b"managed-one")
        self._capture_managed(b"managed-two")

        self.gate.peer_disconnected(ns3_time_us=150)

        self.assertEqual(self.upf.written_frames, [])
        self.assertEqual(self.coordinator.pending_packets, 0)
        self.assertEqual(self.gate.stats.dropped, 2)

    def test_shared_upf_releases_downlink_to_the_selected_gnb(self) -> None:
        ports = {
            "gnb:1": MemoryFramePort(),
            "gnb:2": MemoryFramePort(),
            "upf:a": MemoryFramePort(),
        }
        coordinator = PacketCoordinator(max_pending_packets=8, max_pending_bytes=4096)
        epoch = coordinator.begin_epoch(start_ns3_us=100)
        router = EndpointRouter(
            [
                EndpointLink("gnb:1", "upf:a", "10.0.0.2", "10.0.0.5"),
                EndpointLink("gnb:2", "upf:a", "10.0.0.3", "10.0.0.5"),
            ]
        )
        gate = FrameGate(
            classifier=FlowClassifier(
                [
                    FlowBinding(
                        "flow-2",
                        ue_ip="10.60.0.2",
                        gnb_ip="10.0.0.3",
                        upf_ip="10.0.0.5",
                        qfi=9,
                    )
                ],
                gnb_ips=["10.0.0.2", "10.0.0.3"],
                upf_ips=["10.0.0.5"],
            ),
            coordinator=coordinator,
            kpi=PacketKpiCollector(),
            ports=ports,
            peer_send=self.messages.append,
            virtual_expiry_us_by_flow={"flow-2": 1000},
            egress_resolver=router.route,
        )
        frame = _frame(
            outer_src="10.0.0.5",
            outer_dst="10.0.0.3",
            inner_src="192.0.2.1",
            inner_dst="10.60.0.2",
            src_port=5000,
            dst_port=4000,
        )

        packet_id = gate.capture(
            frame=frame,
            ingress_port="upf:a",
            epoch_id=epoch,
            ns3_time_us=100,
        )
        assert packet_id is not None
        gate.handle_peer_message(
            Message(
                MessageType.PACKET_DELIVER,
                sequence=2,
                payload={"packet_id": packet_id, "epoch_id": epoch, "ns3_time_us": 200},
            )
        )

        self.assertEqual(ports["gnb:1"].written_frames, [])
        self.assertEqual(ports["gnb:2"].written_frames, [frame])


if __name__ == "__main__":
    unittest.main()
