from __future__ import annotations

import ipaddress
import struct
import unittest

from bridge.user_plane.routing import EndpointLink, EndpointRouter


def _ipv4_frame(source_ip: str, destination_ip: str) -> bytes:
    ethernet = bytes.fromhex("0200000000020200000000010800")
    header = bytearray(20)
    header[0] = 0x45
    struct.pack_into("!H", header, 2, 20)
    header[8] = 64
    header[9] = 17
    header[12:16] = ipaddress.IPv4Address(source_ip).packed
    header[16:20] = ipaddress.IPv4Address(destination_ip).packed
    return ethernet + bytes(header)


def _arp_request(source_ip: str, target_ip: str) -> bytes:
    ethernet = bytes.fromhex("ffffffffffff0200000000010806")
    arp = struct.pack(
        "!HHBBH6s4s6s4s",
        1,
        0x0800,
        6,
        4,
        1,
        bytes.fromhex("020000000001"),
        ipaddress.IPv4Address(source_ip).packed,
        bytes(6),
        ipaddress.IPv4Address(target_ip).packed,
    )
    return ethernet + arp


class EndpointRouterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.router = EndpointRouter(
            [
                EndpointLink("gnb:1", "upf:a", "10.0.0.2", "10.0.0.5"),
                EndpointLink("gnb:2", "upf:a", "10.0.0.3", "10.0.0.5"),
                EndpointLink("gnb:3", "upf:b", "10.0.0.4", "10.0.0.6"),
            ]
        )

    def test_routes_shared_upf_downlink_to_selected_gnb(self) -> None:
        frame = _ipv4_frame("10.0.0.5", "10.0.0.3")

        self.assertEqual(self.router.route("upf:a", frame), ("gnb:2",))

    def test_routes_arp_to_target_neighbor(self) -> None:
        frame = _arp_request("10.0.0.5", "10.0.0.2")

        self.assertEqual(self.router.route("upf:a", frame), ("gnb:1",))

    def test_rejects_known_endpoint_without_configured_link(self) -> None:
        frame = _ipv4_frame("10.0.0.5", "10.0.0.4")

        self.assertEqual(self.router.route("upf:a", frame), ())

    def test_floods_unknown_control_destination_only_to_neighbors(self) -> None:
        frame = bytes.fromhex("ffffffffffff02000000000188cc") + b"control"

        self.assertEqual(
            self.router.route("upf:a", frame),
            ("gnb:1", "gnb:2"),
        )


if __name__ == "__main__":
    unittest.main()
