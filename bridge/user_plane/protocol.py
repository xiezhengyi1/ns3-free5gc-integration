from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
import json
import struct
from typing import Any


MAGIC = b"N6AI"
VERSION = 1
HEADER = struct.Struct("!4sBBHIQ")
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024


class ProtocolError(ValueError):
    pass


class MessageType(IntEnum):
    HELLO = 1
    PACKET_ENQUEUE = 2
    PACKET_DELIVER = 3
    PACKET_DROP = 4
    TICK_COMPLETE = 5
    AUTHORIZE_SEND = 6
    EPOCH_START = 7


@dataclass(frozen=True, slots=True)
class Message:
    message_type: MessageType
    sequence: int
    payload: dict[str, Any] = field(default_factory=dict)
    flags: int = 0


def _validate_header(
    magic: bytes,
    version: int,
    raw_type: int,
    payload_length: int,
) -> MessageType:
    if magic != MAGIC:
        raise ProtocolError("invalid protocol magic")
    if version != VERSION:
        raise ProtocolError(f"unsupported protocol version {version}")
    if payload_length > MAX_PAYLOAD_BYTES:
        raise ProtocolError("payload length exceeds protocol limit")
    try:
        return MessageType(raw_type)
    except ValueError as exc:
        raise ProtocolError(f"unknown message type {raw_type}") from exc


def encode_message(message: Message) -> bytes:
    if message.sequence < 0:
        raise ProtocolError("sequence must not be negative")
    payload = json.dumps(
        message.payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ProtocolError("payload length exceeds protocol limit")
    return HEADER.pack(
        MAGIC,
        VERSION,
        int(message.message_type),
        message.flags,
        len(payload),
        message.sequence,
    ) + payload


def decode_message(frame: bytes) -> Message:
    if len(frame) < HEADER.size:
        raise ProtocolError("truncated protocol header")
    magic, version, raw_type, flags, payload_length, sequence = HEADER.unpack_from(frame)
    message_type = _validate_header(magic, version, raw_type, payload_length)
    expected_length = HEADER.size + payload_length
    if len(frame) != expected_length:
        raise ProtocolError(
            f"frame length mismatch: expected {expected_length}, received {len(frame)}"
        )
    try:
        payload = json.loads(frame[HEADER.size:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("protocol payload must be a JSON object")
    return Message(
        message_type=message_type,
        sequence=sequence,
        payload=payload,
        flags=flags,
    )


class StreamDecoder:
    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[Message]:
        self._buffer.extend(data)
        messages: list[Message] = []
        while len(self._buffer) >= HEADER.size:
            magic, version, raw_type, _flags, payload_length, _sequence = HEADER.unpack_from(
                self._buffer
            )
            _validate_header(magic, version, raw_type, payload_length)
            frame_length = HEADER.size + payload_length
            if len(self._buffer) < frame_length:
                break
            frame = bytes(self._buffer[:frame_length])
            del self._buffer[:frame_length]
            messages.append(decode_message(frame))
        return messages
