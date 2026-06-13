from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Protocol


DATAGRAM_MAGIC = b"N6UD"
DATAGRAM_VERSION = 1
DATAGRAM_HEADER = struct.Struct("!4sBHQQI")


class DatagramSocket(Protocol):
    def sendto(self, data: bytes, destination: tuple[str, int]) -> int:
        ...


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class ExperimentHeader:
    flow_id: str
    epoch_id: int
    application_sequence: int
    payload_length: int


def encode_experiment_datagram(
    header: ExperimentHeader, *, total_size: int
) -> bytes:
    flow_id = header.flow_id.encode("utf-8")
    fixed_size = DATAGRAM_HEADER.size + len(flow_id)
    if len(flow_id) > 65535:
        raise ValueError("flow_id is too long")
    if total_size < fixed_size:
        raise ValueError(
            f"payload_size must be at least {fixed_size} bytes for the experiment header"
        )
    payload_length = total_size - fixed_size
    fixed = DATAGRAM_HEADER.pack(
        DATAGRAM_MAGIC,
        DATAGRAM_VERSION,
        len(flow_id),
        header.epoch_id,
        header.application_sequence,
        payload_length,
    )
    return fixed + flow_id + bytes(payload_length)


def decode_experiment_datagram(
    datagram: bytes,
) -> tuple[ExperimentHeader, bytes]:
    if len(datagram) < DATAGRAM_HEADER.size:
        raise ValueError("truncated experiment datagram header")
    magic, version, flow_length, epoch_id, sequence, payload_length = (
        DATAGRAM_HEADER.unpack_from(datagram)
    )
    if magic != DATAGRAM_MAGIC:
        raise ValueError("invalid experiment datagram magic")
    if version != DATAGRAM_VERSION:
        raise ValueError(f"unsupported experiment datagram version {version}")
    header_end = DATAGRAM_HEADER.size + flow_length
    if header_end + payload_length != len(datagram):
        raise ValueError("experiment datagram length mismatch")
    try:
        flow_id = datagram[DATAGRAM_HEADER.size:header_end].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("flow_id is not valid UTF-8") from exc
    payload = datagram[header_end:]
    return (
        ExperimentHeader(
            flow_id=flow_id,
            epoch_id=epoch_id,
            application_sequence=sequence,
            payload_length=payload_length,
        ),
        payload,
    )


class ControlledUdpAgent:
    def __init__(
        self,
        *,
        flow_id: str,
        uplink_socket: DatagramSocket,
        downlink_socket: DatagramSocket,
        uplink_destination: Endpoint,
        downlink_destination: Endpoint,
    ) -> None:
        self.flow_id = flow_id
        self._sockets = {
            "uplink": uplink_socket,
            "downlink": downlink_socket,
        }
        self._destinations = {
            "uplink": uplink_destination,
            "downlink": downlink_destination,
        }
        self._authorization_ids: set[int] = set()
        self._next_application_sequence = 1

    def authorize(
        self,
        *,
        authorization_id: int,
        epoch_id: int,
        direction: str,
        payload_size: int,
    ) -> bool:
        if authorization_id in self._authorization_ids:
            return False
        if direction not in self._sockets:
            raise ValueError("direction must be 'uplink' or 'downlink'")
        sequence = self._next_application_sequence
        datagram = encode_experiment_datagram(
            ExperimentHeader(
                flow_id=self.flow_id,
                epoch_id=epoch_id,
                application_sequence=sequence,
                payload_length=0,
            ),
            total_size=payload_size,
        )
        endpoint = self._destinations[direction]
        sent = self._sockets[direction].sendto(
            datagram, (endpoint.host, endpoint.port)
        )
        if sent != len(datagram):
            raise OSError(
                f"short UDP send: expected {len(datagram)} bytes, sent {sent}"
            )
        self._authorization_ids.add(authorization_id)
        self._next_application_sequence += 1
        return True
