from __future__ import annotations

import ipaddress
import struct
import unittest

from bridge.user_plane.gtpu import (
    DecisionKind,
    Direction,
    FlowBinding,
    FlowClassifier,
    parse_gtpu_frame,
)


def _ipv4_header(src: str, dst: str, protocol: int, payload_length: int) -> bytes:
    total_length = 20 + payload_length
    return struct.pack(
        "!BBHHHBBH4s4s",
        0x45,
        0,
        total_length,
        1,
        0,
        64,
        protocol,
        0,
        ipaddress.IPv4Address(src).packed,
        ipaddress.IPv4Address(dst).packed,
    )


def _udp(src_port: int, dst_port: int, payload: bytes) -> bytes:
    return struct.pack("!HHHH", src_port, dst_port, 8 + len(payload), 0) + payload


def _inner_udp(src: str, dst: str, src_port: int, dst_port: int, payload: bytes = b"x") -> bytes:
    udp = _udp(src_port, dst_port, payload)
    return _ipv4_header(src, dst, 17, len(udp)) + udp


def _gtpu(inner: bytes, *, teid: int = 7, qfi: int | None = None, message_type: int = 0xFF) -> bytes:
    if qfi is None:
        body = inner
        flags = 0x30
    else:
        flags = 0x34
        optional = struct.pack("!HBB", 0, 0, 0x85)
        pdu_session_container = bytes((1, 0x10, qfi & 0x3F, 0))
        body = optional + pdu_session_container + inner
    return struct.pack("!BBHI", flags, message_type, len(body), teid) + body


def _ethernet_ipv4(payload: bytes, src: str, dst: str, *, vlan: bool = False) -> bytes:
    ip_packet = _ipv4_header(src, dst, 17, len(payload)) + payload
    addresses = bytes.fromhex("00112233445566778899aabb")
    if vlan:
        return addresses + struct.pack("!HHH", 0x8100, 1, 0x0800) + ip_packet
    return addresses + struct.pack("!H", 0x0800) + ip_packet


def _frame(
    *,
    outer_src: str = "10.0.0.2",
    outer_dst: str = "10.0.0.3",
    inner_src: str = "10.60.0.1",
    inner_dst: str = "192.0.2.1",
    src_port: int = 4000,
    dst_port: int = 5000,
    qfi: int | None = 9,
    message_type: int = 0xFF,
    vlan: bool = False,
) -> bytes:
    inner = _inner_udp(inner_src, inner_dst, src_port, dst_port)
    gtpu = _gtpu(inner, qfi=qfi, message_type=message_type)
    return _ethernet_ipv4(_udp(2152, 2152, gtpu), outer_src, outer_dst, vlan=vlan)


class GtpuParserTest(unittest.TestCase):
    def test_parses_vlan_gtpu_extension_and_inner_udp_tuple(self) -> None:
        parsed = parse_gtpu_frame(_frame(vlan=True, qfi=37))

        self.assertEqual(parsed.teid, 7)
        self.assertEqual(parsed.qfi, 37)
        self.assertEqual(str(parsed.outer_src), "10.0.0.2")
        self.assertEqual(str(parsed.inner_src), "10.60.0.1")
        self.assertEqual(str(parsed.inner_dst), "192.0.2.1")
        self.assertEqual(parsed.inner_protocol, 17)
        self.assertEqual(parsed.inner_src_port, 4000)
        self.assertEqual(parsed.inner_dst_port, 5000)
        self.assertGreater(parsed.inner_size_bytes, 20)
        self.assertEqual(parsed.inner_payload_size_bytes, 1)

    def test_classifier_maps_qfi_before_tuple_fallback(self) -> None:
        classifier = FlowClassifier(
            bindings=[
                FlowBinding("qfi-flow", qfi=9, ue_ip="10.60.0.1"),
                FlowBinding(
                    "tuple-flow",
                    ue_ip="10.60.0.1",
                    inner_protocol=17,
                    ue_port=4000,
                    remote_port=5000,
                ),
            ],
            gnb_ips=["10.0.0.2"],
            upf_ips=["10.0.0.3"],
        )

        decision = classifier.classify(_frame(qfi=9))

        self.assertEqual(decision.kind, DecisionKind.MANAGED)
        self.assertEqual(decision.flow_id, "qfi-flow")
        self.assertEqual(decision.direction, Direction.UPLINK)

    def test_classifier_uses_tuple_fallback_without_qfi(self) -> None:
        classifier = FlowClassifier(
            bindings=[
                FlowBinding(
                    "tuple-flow",
                    ue_ip="10.60.0.1",
                    inner_protocol=17,
                    ue_port=4000,
                    remote_port=5000,
                )
            ],
            gnb_ips=["10.0.0.2"],
            upf_ips=["10.0.0.3"],
        )

        decision = classifier.classify(_frame(qfi=None))

        self.assertEqual(decision.kind, DecisionKind.MANAGED)
        self.assertEqual(decision.flow_id, "tuple-flow")

    def test_classifier_maps_downlink_ports_relative_to_ue(self) -> None:
        classifier = FlowClassifier(
            bindings=[
                FlowBinding(
                    "downlink",
                    ue_ip="10.60.0.1",
                    inner_protocol=17,
                    ue_port=4000,
                    remote_port=5000,
                )
            ],
            gnb_ips=["10.0.0.2"],
            upf_ips=["10.0.0.3"],
        )

        decision = classifier.classify(
            _frame(
                outer_src="10.0.0.3",
                outer_dst="10.0.0.2",
                inner_src="192.0.2.1",
                inner_dst="10.60.0.1",
                src_port=5000,
                dst_port=4000,
                qfi=None,
            )
        )

        self.assertEqual(decision.kind, DecisionKind.MANAGED)
        self.assertEqual(decision.direction, Direction.DOWNLINK)

    def test_non_gtpu_and_non_gpdu_are_control_bypass(self) -> None:
        classifier = FlowClassifier([], gnb_ips=["10.0.0.2"], upf_ips=["10.0.0.3"])
        arp = bytes.fromhex("00112233445566778899aabb0806") + b"\x00" * 28

        self.assertEqual(classifier.classify(arp).kind, DecisionKind.CONTROL_BYPASS)
        self.assertEqual(
            classifier.classify(_frame(message_type=1)).kind,
            DecisionKind.CONTROL_BYPASS,
        )

    def test_unmapped_gpdu_fails_closed(self) -> None:
        classifier = FlowClassifier([], gnb_ips=["10.0.0.2"], upf_ips=["10.0.0.3"])

        self.assertEqual(classifier.classify(_frame()).kind, DecisionKind.UNMAPPED)

    def test_malformed_gpdu_fails_closed(self) -> None:
        classifier = FlowClassifier([], gnb_ips=["10.0.0.2"], upf_ips=["10.0.0.3"])
        truncated = _frame()[:-8]

        self.assertEqual(classifier.classify(truncated).kind, DecisionKind.MALFORMED)


if __name__ == "__main__":
    unittest.main()
