from __future__ import annotations

import ipaddress
import struct
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EndpointLink:
    gnb_port: str
    upf_port: str
    gnb_ip: ipaddress.IPv4Address | str
    upf_ip: ipaddress.IPv4Address | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "gnb_ip", ipaddress.IPv4Address(self.gnb_ip))
        object.__setattr__(self, "upf_ip", ipaddress.IPv4Address(self.upf_ip))


class EndpointRouter:
    """Route Ethernet frames only across configured gNB-UPF adjacencies."""

    def __init__(self, links: Iterable[EndpointLink]) -> None:
        self._neighbors: dict[str, set[str]] = defaultdict(set)
        self._port_by_ip: dict[ipaddress.IPv4Address, str] = {}
        self._port_by_mac: dict[bytes, str] = {}
        for link in links:
            self._neighbors[link.gnb_port].add(link.upf_port)
            self._neighbors[link.upf_port].add(link.gnb_port)
            self._register_ip(link.gnb_ip, link.gnb_port)
            self._register_ip(link.upf_ip, link.upf_port)
        if not self._neighbors:
            raise ValueError("endpoint router requires at least one N3 link")

    def route(self, ingress_port: str, frame: bytes) -> tuple[str, ...]:
        neighbors = self._neighbors.get(ingress_port)
        if neighbors is None:
            raise ValueError(f"unknown ingress port {ingress_port}")
        if len(frame) < 14:
            return ()

        destination_mac = frame[0:6]
        source_mac = frame[6:12]
        if not source_mac[0] & 1:
            self._port_by_mac[source_mac] = ingress_port

        target_ip = self._target_ipv4(frame)
        if target_ip is not None:
            target_port = self._port_by_ip.get(target_ip)
            if target_port is not None:
                return (target_port,) if target_port in neighbors else ()

        learned_port = self._port_by_mac.get(destination_mac)
        if learned_port is not None:
            return (learned_port,) if learned_port in neighbors else ()
        return tuple(sorted(neighbors))

    def _register_ip(self, address: ipaddress.IPv4Address, port: str) -> None:
        previous = self._port_by_ip.get(address)
        if previous is not None and previous != port:
            raise ValueError(f"N3 endpoint IP {address} maps to multiple ports")
        self._port_by_ip[address] = port

    @staticmethod
    def _target_ipv4(frame: bytes) -> ipaddress.IPv4Address | None:
        ether_type = struct.unpack_from("!H", frame, 12)[0]
        offset = 14
        if ether_type in {0x8100, 0x88A8}:
            if len(frame) < 18:
                return None
            ether_type = struct.unpack_from("!H", frame, 16)[0]
            offset = 18
        if ether_type == 0x0800 and len(frame) >= offset + 20:
            return ipaddress.IPv4Address(frame[offset + 16 : offset + 20])
        if ether_type != 0x0806 or len(frame) < offset + 28:
            return None
        hardware_type, protocol_type, hardware_len, protocol_len = struct.unpack_from(
            "!HHBB", frame, offset
        )
        if (hardware_type, protocol_type, hardware_len, protocol_len) != (1, 0x0800, 6, 4):
            return None
        return ipaddress.IPv4Address(frame[offset + 24 : offset + 28])
