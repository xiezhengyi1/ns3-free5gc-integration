#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import asdict
import ipaddress
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bridge.user_plane.coordinator import PacketCoordinator
from bridge.user_plane.frame_port import MemoryFramePort
from bridge.user_plane.gate import FrameGate
from bridge.user_plane.gtpu import (
    Classification,
    DecisionKind,
    Direction,
    GtpuPacket,
)
from bridge.user_plane.kpi import PacketKpiCollector
from bridge.user_plane.protocol import Message, MessageType


class _DemoClassifier:
    def classify(self, frame: bytes) -> Classification:
        packet = GtpuPacket(
            teid=7,
            qfi=9,
            outer_src=ipaddress.IPv4Address("10.201.1.2"),
            outer_dst=ipaddress.IPv4Address("10.201.1.3"),
            inner_src=ipaddress.IPv4Address("10.60.0.1"),
            inner_dst=ipaddress.IPv4Address("192.0.2.1"),
            inner_protocol=17,
            inner_src_port=15000,
            inner_dst_port=5000,
            inner_size_bytes=len(frame),
            inner_payload_size_bytes=len(frame),
        )
        return Classification(
            DecisionKind.MANAGED,
            packet=packet,
            flow_id="flow-1",
            direction=Direction.UPLINK,
        )


def run_demo() -> dict[str, object]:
    gnb = MemoryFramePort()
    upf = MemoryFramePort()
    outbound: list[Message] = []
    coordinator = PacketCoordinator(
        max_pending_packets=16,
        max_pending_bytes=4096,
    )
    kpi = PacketKpiCollector()
    gate = FrameGate(
        classifier=_DemoClassifier(),
        coordinator=coordinator,
        kpi=kpi,
        ports={"gnb": gnb, "upf": upf},
        peer_send=outbound.append,
        virtual_expiry_us_by_flow={"flow-1": 1000},
    )
    epoch_id = coordinator.begin_epoch(start_ns3_us=1000)
    first = gate.capture(
        frame=b"managed-one",
        ingress_port="gnb",
        epoch_id=epoch_id,
        ns3_time_us=1000,
    )
    second = gate.capture(
        frame=b"managed-two",
        ingress_port="gnb",
        epoch_id=epoch_id,
        ns3_time_us=1000,
    )
    assert first is not None and second is not None

    gate.handle_peer_message(
        Message(
            MessageType.PACKET_DELIVER,
            sequence=10,
            payload={
                "packet_id": first,
                "epoch_id": epoch_id,
                "ns3_time_us": 1200,
            },
        )
    )
    gate.handle_peer_message(
        Message(
            MessageType.PACKET_DROP,
            sequence=11,
            payload={
                "packet_id": second,
                "epoch_id": epoch_id,
                "ns3_time_us": 1500,
                "reason": "demo-radio-drop",
            },
        )
    )
    summary = kpi.complete_epoch(
        epoch_id=epoch_id,
        start_ns3_us=1000,
        end_ns3_us=2000,
    )
    return {
        "released_frames": [frame.decode("ascii") for frame in upf.written_frames],
        "enqueue_messages": [message.payload for message in outbound],
        "stats": asdict(gate.stats),
        "kpi": asdict(summary),
        "pending_packets": coordinator.pending_packets,
    }


def main() -> int:
    print(json.dumps(run_demo(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
