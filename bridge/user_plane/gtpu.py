from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import ipaddress
import struct
from typing import Iterable


GTPU_PORT = 2152
GTPU_G_PDU = 0xFF
PDU_SESSION_CONTAINER = 0x85


class GtpuParseError(ValueError):
    pass


class NotGtpuFrame(GtpuParseError):
    pass


class NonGpduFrame(GtpuParseError):
    pass


class DecisionKind(str, Enum):
    CONTROL_BYPASS = "control_bypass"
    MANAGED = "managed"
    UNMAPPED = "unmapped"
    MALFORMED = "malformed"


class Direction(str, Enum):
    UPLINK = "uplink"
    DOWNLINK = "downlink"


@dataclass(frozen=True, slots=True)
class GtpuPacket:
    teid: int
    qfi: int | None
    outer_src: ipaddress.IPv4Address
    outer_dst: ipaddress.IPv4Address
    inner_src: ipaddress.IPv4Address
    inner_dst: ipaddress.IPv4Address
    inner_protocol: int
    inner_src_port: int | None
    inner_dst_port: int | None
    inner_size_bytes: int = 0
    inner_payload_size_bytes: int = 0


@dataclass(frozen=True, slots=True)
class FlowBinding:
    flow_id: str
    ue_ip: ipaddress.IPv4Address | str
    gnb_ip: ipaddress.IPv4Address | str
    upf_ip: ipaddress.IPv4Address | str
    qfi: int | None = None
    inner_protocol: int | None = None
    ue_port: int | None = None
    remote_port: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ue_ip", ipaddress.IPv4Address(self.ue_ip))
        object.__setattr__(self, "gnb_ip", ipaddress.IPv4Address(self.gnb_ip))
        object.__setattr__(self, "upf_ip", ipaddress.IPv4Address(self.upf_ip))
        if self.qfi is not None and not 0 <= self.qfi <= 63:
            raise ValueError("qfi must be within [0, 63]")


@dataclass(frozen=True, slots=True)
class Classification:
    kind: DecisionKind
    packet: GtpuPacket | None = None
    flow_id: str | None = None
    direction: Direction | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Ipv4View:
    src: ipaddress.IPv4Address
    dst: ipaddress.IPv4Address
    protocol: int
    payload_offset: int
    end: int


def _parse_ipv4(data: bytes, offset: int, limit: int) -> _Ipv4View:
    if offset + 20 > limit:
        raise GtpuParseError("truncated IPv4 header")
    version_ihl = data[offset]
    if version_ihl >> 4 != 4:
        raise GtpuParseError("encapsulated packet is not IPv4")
    header_length = (version_ihl & 0x0F) * 4
    if header_length < 20 or offset + header_length > limit:
        raise GtpuParseError("invalid IPv4 header length")
    total_length = struct.unpack_from("!H", data, offset + 2)[0]
    if total_length < header_length or offset + total_length > limit:
        raise GtpuParseError("invalid IPv4 total length")
    return _Ipv4View(
        src=ipaddress.IPv4Address(data[offset + 12:offset + 16]),
        dst=ipaddress.IPv4Address(data[offset + 16:offset + 20]),
        protocol=data[offset + 9],
        payload_offset=offset + header_length,
        end=offset + total_length,
    )


def _parse_udp(data: bytes, offset: int, limit: int) -> tuple[int, int, int, int]:
    if offset + 8 > limit:
        raise GtpuParseError("truncated UDP header")
    src_port, dst_port, length, _checksum = struct.unpack_from("!HHHH", data, offset)
    if length < 8 or offset + length > limit:
        raise GtpuParseError("invalid UDP length")
    return src_port, dst_port, offset + 8, offset + length


def parse_gtpu_frame(frame: bytes) -> GtpuPacket:
    if len(frame) < 14:
        raise NotGtpuFrame("truncated Ethernet frame")
    ether_type = struct.unpack_from("!H", frame, 12)[0]
    network_offset = 14
    if ether_type in {0x8100, 0x88A8}:
        if len(frame) < 18:
            raise GtpuParseError("truncated VLAN header")
        ether_type = struct.unpack_from("!H", frame, 16)[0]
        network_offset = 18
    if ether_type != 0x0800:
        raise NotGtpuFrame("Ethernet payload is not IPv4")

    outer = _parse_ipv4(frame, network_offset, len(frame))
    if outer.protocol != 17:
        raise NotGtpuFrame("outer IPv4 payload is not UDP")
    src_port, dst_port, gtpu_offset, udp_end = _parse_udp(
        frame, outer.payload_offset, outer.end
    )
    if src_port != GTPU_PORT and dst_port != GTPU_PORT:
        raise NotGtpuFrame("UDP packet is not GTP-U")
    if gtpu_offset + 8 > udp_end:
        raise GtpuParseError("truncated GTP-U header")

    flags, message_type, payload_length, teid = struct.unpack_from(
        "!BBHI", frame, gtpu_offset
    )
    if (flags >> 5) != 1 or not flags & 0x10:
        raise GtpuParseError("unsupported GTP-U version or protocol type")
    gtpu_end = gtpu_offset + 8 + payload_length
    if gtpu_end > udp_end:
        raise GtpuParseError("GTP-U payload exceeds UDP payload")
    if message_type != GTPU_G_PDU:
        raise NonGpduFrame(f"GTP-U message type {message_type} is not a G-PDU")

    cursor = gtpu_offset + 8
    qfi: int | None = None
    has_optional_fields = bool(flags & 0x07)
    next_extension = 0
    if has_optional_fields:
        if cursor + 4 > gtpu_end:
            raise GtpuParseError("truncated GTP-U optional fields")
        next_extension = frame[cursor + 3]
        cursor += 4
    if flags & 0x04:
        while next_extension:
            extension_type = next_extension
            if cursor >= gtpu_end:
                raise GtpuParseError("truncated GTP-U extension header")
            extension_length = frame[cursor] * 4
            if extension_length < 4 or cursor + extension_length > gtpu_end:
                raise GtpuParseError("invalid GTP-U extension header length")
            content = frame[cursor + 1:cursor + extension_length - 1]
            next_extension = frame[cursor + extension_length - 1]
            if extension_type == PDU_SESSION_CONTAINER:
                if len(content) < 2:
                    raise GtpuParseError("truncated PDU Session Container")
                qfi = content[-1] & 0x3F
            cursor += extension_length

    inner = _parse_ipv4(frame, cursor, gtpu_end)
    inner_src_port: int | None = None
    inner_dst_port: int | None = None
    if inner.protocol in {6, 17}:
        if inner.payload_offset + 4 > inner.end:
            raise GtpuParseError("truncated inner transport header")
        inner_src_port, inner_dst_port = struct.unpack_from(
            "!HH", frame, inner.payload_offset
        )
    inner_payload_size = inner.end - inner.payload_offset
    if inner.protocol == 17:
        inner_payload_size = max(0, inner_payload_size - 8)
    return GtpuPacket(
        teid=teid,
        qfi=qfi,
        outer_src=outer.src,
        outer_dst=outer.dst,
        inner_src=inner.src,
        inner_dst=inner.dst,
        inner_protocol=inner.protocol,
        inner_src_port=inner_src_port,
        inner_dst_port=inner_dst_port,
        inner_size_bytes=inner.end - cursor,
        inner_payload_size_bytes=inner_payload_size,
    )


class FlowClassifier:
    def __init__(
        self,
        bindings: Iterable[FlowBinding],
        *,
        gnb_ips: Iterable[str | ipaddress.IPv4Address],
        upf_ips: Iterable[str | ipaddress.IPv4Address],
    ) -> None:
        self._bindings = tuple(bindings)
        self._gnb_ips = {ipaddress.IPv4Address(value) for value in gnb_ips}
        self._upf_ips = {ipaddress.IPv4Address(value) for value in upf_ips}

    def classify(self, frame: bytes) -> Classification:
        try:
            packet = parse_gtpu_frame(frame)
        except (NotGtpuFrame, NonGpduFrame) as exc:
            return Classification(DecisionKind.CONTROL_BYPASS, reason=str(exc))
        except GtpuParseError as exc:
            return Classification(DecisionKind.MALFORMED, reason=str(exc))

        direction = self._direction(packet)
        if direction is None:
            return Classification(
                DecisionKind.UNMAPPED,
                packet=packet,
                reason="outer GTP-U endpoints do not identify gNB/UPF direction",
            )
        ue_ip = packet.inner_src if direction is Direction.UPLINK else packet.inner_dst

        if packet.qfi is not None:
            for binding in self._bindings:
                if (
                    binding.ue_ip == ue_ip
                    and binding.qfi == packet.qfi
                    and self._matches_endpoints(binding, packet, direction)
                ):
                    return Classification(
                        DecisionKind.MANAGED,
                        packet=packet,
                        flow_id=binding.flow_id,
                        direction=direction,
                    )
        for binding in self._bindings:
            if self._matches_tuple(binding, packet, direction, ue_ip):
                return Classification(
                    DecisionKind.MANAGED,
                    packet=packet,
                    flow_id=binding.flow_id,
                    direction=direction,
                )
        return Classification(
            DecisionKind.UNMAPPED,
            packet=packet,
            direction=direction,
            reason="no QFI or inner tuple binding matched",
        )

    def _direction(self, packet: GtpuPacket) -> Direction | None:
        if packet.outer_src in self._gnb_ips and packet.outer_dst in self._upf_ips:
            return Direction.UPLINK
        if packet.outer_src in self._upf_ips and packet.outer_dst in self._gnb_ips:
            return Direction.DOWNLINK
        return None

    @staticmethod
    def _matches_tuple(
        binding: FlowBinding,
        packet: GtpuPacket,
        direction: Direction,
        ue_ip: ipaddress.IPv4Address,
    ) -> bool:
        if binding.ue_ip != ue_ip or not FlowClassifier._matches_endpoints(
            binding, packet, direction
        ):
            return False
        if binding.inner_protocol is not None and binding.inner_protocol != packet.inner_protocol:
            return False
        ue_port = (
            packet.inner_src_port
            if direction is Direction.UPLINK
            else packet.inner_dst_port
        )
        remote_port = (
            packet.inner_dst_port
            if direction is Direction.UPLINK
            else packet.inner_src_port
        )
        if binding.ue_port is not None and binding.ue_port != ue_port:
            return False
        if binding.remote_port is not None and binding.remote_port != remote_port:
            return False
        return any(
            value is not None
            for value in (binding.inner_protocol, binding.ue_port, binding.remote_port)
        )

    @staticmethod
    def _matches_endpoints(
        binding: FlowBinding,
        packet: GtpuPacket,
        direction: Direction,
    ) -> bool:
        if direction is Direction.UPLINK:
            return packet.outer_src == binding.gnb_ip and packet.outer_dst == binding.upf_ip
        return packet.outer_src == binding.upf_ip and packet.outer_dst == binding.gnb_ip
