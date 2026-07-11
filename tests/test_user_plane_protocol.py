from __future__ import annotations

import unittest

from bridge.user_plane.protocol import (
    HEADER,
    MAGIC,
    VERSION,
    Message,
    MessageType,
    ProtocolError,
    StreamDecoder,
    decode_message,
    encode_message,
)


class UserPlaneProtocolTest(unittest.TestCase):
    def test_round_trips_all_message_types(self) -> None:
        for index, message_type in enumerate(MessageType, start=1):
            expected = Message(
                message_type=message_type,
                sequence=index,
                flags=index % 2,
                payload={"flow_id": "flow-1", "value": index},
            )

            self.assertEqual(decode_message(encode_message(expected)), expected)

    def test_rejects_bad_magic(self) -> None:
        frame = bytearray(
            encode_message(Message(MessageType.HELLO, sequence=1, payload={}))
        )
        frame[:4] = b"BAD!"

        with self.assertRaisesRegex(ProtocolError, "magic"):
            decode_message(bytes(frame))

    def test_rejects_unsupported_version(self) -> None:
        payload = b"{}"
        frame = HEADER.pack(MAGIC, VERSION + 1, MessageType.HELLO, 0, len(payload), 1) + payload

        with self.assertRaisesRegex(ProtocolError, "version"):
            decode_message(frame)

    def test_rejects_truncated_header(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "header"):
            decode_message(b"short")

    def test_rejects_payload_length_mismatch(self) -> None:
        frame = encode_message(Message(MessageType.HELLO, sequence=1, payload={"ok": True}))

        with self.assertRaisesRegex(ProtocolError, "length"):
            decode_message(frame[:-1])

    def test_stream_decoder_handles_fragmented_and_multiple_frames(self) -> None:
        first = encode_message(
            Message(MessageType.PACKET_ENQUEUE, sequence=11, payload={"packet_id": 7})
        )
        second = encode_message(
            Message(MessageType.TICK_COMPLETE, sequence=12, payload={"epoch_id": 3})
        )
        decoder = StreamDecoder()

        self.assertEqual(decoder.feed(first[:5]), [])
        self.assertEqual(decoder.feed(first[5:] + second[:8]), [
            Message(MessageType.PACKET_ENQUEUE, sequence=11, payload={"packet_id": 7})
        ])
        self.assertEqual(decoder.feed(second[8:]), [
            Message(MessageType.TICK_COMPLETE, sequence=12, payload={"epoch_id": 3})
        ])


if __name__ == "__main__":
    unittest.main()
