from __future__ import annotations

import unittest

from bridge.user_plane.udp_agent import (
    ControlledUdpAgent,
    Endpoint,
    decode_experiment_datagram,
)


class _FakeSocket:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data: bytes, destination: tuple[str, int]) -> int:
        self.calls.append((data, destination))
        return len(data)


class ControlledUdpAgentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.uplink = _FakeSocket()
        self.downlink = _FakeSocket()
        self.agent = ControlledUdpAgent(
            flow_id="flow-1",
            uplink_socket=self.uplink,
            downlink_socket=self.downlink,
            uplink_destination=Endpoint("192.0.2.10", 5000),
            downlink_destination=Endpoint("10.60.0.1", 15000),
        )

    def test_does_not_send_before_authorization(self) -> None:
        self.assertEqual(self.uplink.calls, [])
        self.assertEqual(self.downlink.calls, [])

    def test_sends_exactly_one_datagram_per_authorization(self) -> None:
        sent = self.agent.authorize(
            authorization_id=1,
            epoch_id=7,
            direction="uplink",
            payload_size=128,
        )

        self.assertTrue(sent)
        self.assertEqual(len(self.uplink.calls), 1)
        datagram, destination = self.uplink.calls[0]
        header, payload = decode_experiment_datagram(datagram)
        self.assertEqual(destination, ("192.0.2.10", 5000))
        self.assertEqual(header.flow_id, "flow-1")
        self.assertEqual(header.epoch_id, 7)
        self.assertEqual(header.application_sequence, 1)
        self.assertEqual(header.payload_length, len(payload))
        self.assertEqual(len(datagram), 128)

    def test_duplicate_authorization_is_idempotent(self) -> None:
        self.assertTrue(
            self.agent.authorize(
                authorization_id=5,
                epoch_id=1,
                direction="uplink",
                payload_size=96,
            )
        )
        self.assertFalse(
            self.agent.authorize(
                authorization_id=5,
                epoch_id=1,
                direction="uplink",
                payload_size=96,
            )
        )
        self.assertEqual(len(self.uplink.calls), 1)

    def test_downlink_uses_downlink_socket_and_destination(self) -> None:
        self.agent.authorize(
            authorization_id=8,
            epoch_id=2,
            direction="downlink",
            payload_size=96,
        )

        self.assertEqual(len(self.downlink.calls), 1)
        self.assertEqual(self.downlink.calls[0][1], ("10.60.0.1", 15000))

    def test_rejects_payload_smaller_than_experiment_header(self) -> None:
        with self.assertRaisesRegex(ValueError, "payload_size"):
            self.agent.authorize(
                authorization_id=9,
                epoch_id=2,
                direction="uplink",
                payload_size=1,
            )


if __name__ == "__main__":
    unittest.main()
