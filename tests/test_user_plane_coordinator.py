from __future__ import annotations

import unittest

from bridge.user_plane.coordinator import (
    ActionKind,
    CapacityError,
    CoordinatorError,
    PacketCoordinator,
    PacketState,
)


class PacketCoordinatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = PacketCoordinator(max_pending_packets=2, max_pending_bytes=8)
        self.epoch = self.coordinator.begin_epoch(start_ns3_us=1000)

    def _capture(self, frame: bytes = b"abcd", expiry_us: int = 500):
        return self.coordinator.capture(
            frame=frame,
            flow_id="flow-1",
            direction="uplink",
            epoch_id=self.epoch,
            enqueue_ns3_us=1000,
            virtual_expiry_us=expiry_us,
        )

    def test_allocates_monotonic_epoch_and_packet_ids(self) -> None:
        second_epoch = self.coordinator.begin_epoch(start_ns3_us=2000)
        first = self._capture()
        second = self.coordinator.capture(
            frame=b"x",
            flow_id="flow-1",
            direction="uplink",
            epoch_id=second_epoch,
            enqueue_ns3_us=2000,
            virtual_expiry_us=500,
        )

        self.assertEqual((self.epoch, second_epoch), (1, 2))
        self.assertEqual((first.packet_id, second.packet_id), (1, 2))
        self.assertEqual(self.coordinator.epoch(self.epoch).start_ns3_us, 1000)

    def test_delivery_requires_submission_and_explicit_release_completion(self) -> None:
        packet = self._capture()
        self.coordinator.mark_submitted(packet.packet_id)

        action = self.coordinator.mark_delivered(
            packet.packet_id, epoch_id=self.epoch, ns3_time_us=1200
        )

        self.assertEqual(action.kind, ActionKind.RELEASE)
        self.assertEqual(action.frame, b"abcd")
        self.assertEqual(self.coordinator.packet(packet.packet_id).state, PacketState.DELIVERED)
        self.assertTrue(self.coordinator.is_epoch_complete(self.epoch))

        self.coordinator.complete_action(packet.packet_id)
        self.assertEqual(self.coordinator.packet(packet.packet_id).state, PacketState.RELEASED)
        self.assertEqual(self.coordinator.pending_bytes, 0)

    def test_drop_discards_packet(self) -> None:
        packet = self._capture()
        self.coordinator.mark_submitted(packet.packet_id)

        action = self.coordinator.mark_dropped(
            packet.packet_id,
            epoch_id=self.epoch,
            ns3_time_us=1100,
            reason="rlc-discard",
        )
        self.assertEqual(action.kind, ActionKind.DISCARD)
        self.coordinator.complete_action(packet.packet_id)
        self.assertEqual(self.coordinator.packet(packet.packet_id).state, PacketState.DISCARDED)

    def test_rejects_duplicate_unknown_and_cross_epoch_results(self) -> None:
        packet = self._capture()
        self.coordinator.mark_submitted(packet.packet_id)
        with self.assertRaisesRegex(CoordinatorError, "epoch"):
            self.coordinator.mark_delivered(packet.packet_id, epoch_id=99, ns3_time_us=1200)
        with self.assertRaisesRegex(CoordinatorError, "unknown"):
            self.coordinator.mark_delivered(999, epoch_id=self.epoch, ns3_time_us=1200)

        self.coordinator.mark_delivered(packet.packet_id, epoch_id=self.epoch, ns3_time_us=1200)
        with self.assertRaisesRegex(CoordinatorError, "state"):
            self.coordinator.mark_delivered(packet.packet_id, epoch_id=self.epoch, ns3_time_us=1300)

    def test_epoch_waits_for_every_submitted_packet(self) -> None:
        first = self._capture(b"aa")
        second = self._capture(b"bb")
        self.coordinator.mark_submitted(first.packet_id)
        self.coordinator.mark_submitted(second.packet_id)

        self.coordinator.mark_delivered(first.packet_id, epoch_id=self.epoch, ns3_time_us=1200)
        self.assertFalse(self.coordinator.is_epoch_complete(self.epoch))
        self.coordinator.mark_dropped(
            second.packet_id, epoch_id=self.epoch, ns3_time_us=1300, reason="phy"
        )
        self.assertTrue(self.coordinator.is_epoch_complete(self.epoch))

    def test_enforces_packet_and_byte_capacity(self) -> None:
        self._capture(b"1234")
        self._capture(b"5678")

        with self.assertRaises(CapacityError):
            self._capture(b"x")

    def test_expiry_uses_virtual_time(self) -> None:
        packet = self._capture(expiry_us=500)
        self.coordinator.mark_submitted(packet.packet_id)

        self.assertEqual(self.coordinator.expire(ns3_now_us=1499), [])
        actions = self.coordinator.expire(ns3_now_us=1500)

        self.assertEqual([action.packet_id for action in actions], [packet.packet_id])
        self.assertEqual(
            self.coordinator.packet(packet.packet_id).terminal_reason,
            "virtual-expiry",
        )

    def test_peer_disconnect_fails_closed(self) -> None:
        first = self._capture(b"aa")
        second = self._capture(b"bb")
        self.coordinator.mark_submitted(first.packet_id)

        actions = self.coordinator.peer_disconnected(ns3_time_us=1050)

        self.assertEqual({action.packet_id for action in actions}, {first.packet_id, second.packet_id})
        self.assertTrue(all(action.kind is ActionKind.DISCARD for action in actions))


if __name__ == "__main__":
    unittest.main()
