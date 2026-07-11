from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from bridge.user_plane.cli import AuthorizationRelay, GateRuntimeConfig, _reset_output_log
from bridge.user_plane.protocol import Message, MessageType, StreamDecoder


class _AgentSocket:
    def __init__(self) -> None:
        self.data = bytearray()
        self.closed = False

    def sendall(self, data: bytes) -> None:
        self.data.extend(data)

    def close(self) -> None:
        self.closed = True


class UserPlaneCliTest(unittest.TestCase):
    def test_reset_output_log_truncates_previous_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "events.jsonl"
            output.parent.mkdir()
            output.write_text("stale\n", encoding="utf-8")

            _reset_output_log(str(output))

            self.assertEqual(output.read_text(encoding="utf-8"), "")

    def test_loads_runtime_config_and_flow_bindings(self) -> None:
        config = GateRuntimeConfig.from_dict(
            {
                "links": [
                    {
                        "link_id": "n3-1",
                        "gnb_name": "gNB-1",
                        "upf_name": "upf-1",
                        "gnb_tap": "tgnb1",
                        "upf_tap": "tupf1",
                        "gnb_ip": "10.0.0.2",
                        "upf_ip": "10.0.0.3",
                    }
                ],
                "socket_path": "/tmp/gate.sock",
                "authorization_socket": "/tmp/gate.sock.agents",
                "max_pending_packets": 16,
                "max_pending_bytes": 4096,
                "flows": [
                    {
                        "flow_id": "flow-1",
                        "ue_ip": "10.60.0.1",
                        "gnb_ip": "10.0.0.2",
                        "upf_ip": "10.0.0.3",
                        "qfi": 9,
                        "inner_protocol": 17,
                        "ue_port": 4000,
                        "remote_port": 5000,
                        "virtual_expiry_us": 5000,
                    }
                ],
            }
        )

        self.assertEqual(config.links[0].gnb_tap, "tgnb1")
        self.assertEqual(config.authorization_socket, "/tmp/gate.sock.agents")
        self.assertEqual(config.bindings[0].flow_id, "flow-1")
        self.assertEqual(config.bindings[0].qfi, 9)
        self.assertEqual(config.virtual_expiry_us_by_flow, {"flow-1": 5000})

    def test_loads_multiple_links_with_a_shared_upf_tap(self) -> None:
        config = GateRuntimeConfig.from_dict(
            {
                "links": [
                    {
                        "link_id": "n3-1",
                        "gnb_name": "gNB-1",
                        "upf_name": "upf-a",
                        "gnb_tap": "tgnb1",
                        "upf_tap": "tupf1",
                        "gnb_ip": "10.0.0.2",
                        "upf_ip": "10.0.0.5",
                    },
                    {
                        "link_id": "n3-2",
                        "gnb_name": "gNB-2",
                        "upf_name": "upf-a",
                        "gnb_tap": "tgnb2",
                        "upf_tap": "tupf1",
                        "gnb_ip": "10.0.0.3",
                        "upf_ip": "10.0.0.5",
                    },
                ],
                "socket_path": "/tmp/gate.sock",
                "flows": [],
            }
        )

        self.assertEqual(len(config.links), 2)
        self.assertEqual(config.gnb_ips, ("10.0.0.2", "10.0.0.3"))
        self.assertEqual(config.upf_ips, ("10.0.0.5",))

    def test_rejects_removed_fail_closed_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "fail_closed was removed"):
            GateRuntimeConfig.from_dict({"fail_closed": True})

    def test_rejects_conflicting_shared_endpoint_definition(self) -> None:
        payload = {
            "links": [
                {
                    "gnb_name": "gNB-1",
                    "upf_name": "upf-a",
                    "gnb_tap": "tgnb1",
                    "upf_tap": "tupf1",
                    "gnb_ip": "10.0.0.2",
                    "upf_ip": "10.0.0.5",
                },
                {
                    "gnb_name": "gNB-2",
                    "upf_name": "upf-a",
                    "gnb_tap": "tgnb2",
                    "upf_tap": "tupf2",
                    "gnb_ip": "10.0.0.3",
                    "upf_ip": "10.0.0.5",
                },
            ],
            "socket_path": "/tmp/gate.sock",
            "flows": [],
        }

        with self.assertRaisesRegex(ValueError, "conflicting"):
            GateRuntimeConfig.from_dict(payload)

    def test_rejects_invalid_link_ip_and_duplicate_edge(self) -> None:
        link = {
            "gnb_name": "gNB-1",
            "upf_name": "upf-a",
            "gnb_tap": "tgnb1",
            "upf_tap": "tupf1",
            "gnb_ip": "not-an-ip",
            "upf_ip": "10.0.0.5",
        }
        with self.assertRaisesRegex(ValueError, "valid IPv4"):
            GateRuntimeConfig.from_dict(
                {"links": [link], "socket_path": "/tmp/gate.sock", "flows": []}
            )

        link["gnb_ip"] = "10.0.0.2"
        with self.assertRaisesRegex(ValueError, "duplicate N3 link"):
            GateRuntimeConfig.from_dict(
                {
                    "links": [link, {**link, "link_id": "n3-copy"}],
                    "socket_path": "/tmp/gate.sock",
                    "flows": [],
                }
            )

    def test_rejects_duplicate_flow_ids(self) -> None:
        flow = {
            "flow_id": "flow-1",
            "ue_ip": "10.60.0.1",
            "gnb_ip": "10.0.0.2",
            "upf_ip": "10.0.0.3",
            "virtual_expiry_us": 5000,
        }
        payload = {
            "links": [
                {
                    "gnb_name": "gNB-1",
                    "upf_name": "upf-1",
                    "gnb_tap": "tgnb1",
                    "upf_tap": "tupf1",
                    "gnb_ip": "10.0.0.2",
                    "upf_ip": "10.0.0.3",
                }
            ],
            "socket_path": "/tmp/gate.sock",
            "flows": [flow, dict(flow)],
        }

        with self.assertRaisesRegex(ValueError, "duplicate"):
            GateRuntimeConfig.from_dict(payload)

    def test_rejects_removed_legacy_two_tap_config(self) -> None:
        with self.assertRaisesRegex(ValueError, "legacy top-level gate fields were removed"):
            GateRuntimeConfig.from_dict(
                {
                    "gnb_tap": "tgnb1",
                    "upf_tap": "tupf1",
                    "gnb_ips": ["10.0.0.2"],
                    "upf_ips": ["10.0.0.3"],
                    "socket_path": "/tmp/gate.sock",
                    "flows": [],
                }
            )

    def test_protocol_defines_explicit_epoch_start(self) -> None:
        self.assertIsInstance(MessageType.EPOCH_START, MessageType)

    def test_authorization_relay_replays_current_epoch_to_late_agent(self) -> None:
        relay = AuthorizationRelay()
        authorization = Message(
            MessageType.AUTHORIZE_SEND,
            sequence=4,
            payload={
                "authorization_id": 17,
                "epoch_id": 3,
                "flow_id": "flow-1",
                "direction": "uplink",
                "payload_size": 128,
            },
        )

        relay.publish(authorization)
        agent = _AgentSocket()
        relay.add_peer(agent)

        decoded = StreamDecoder().feed(bytes(agent.data))
        self.assertEqual(decoded, [authorization])

    def test_authorization_relay_drops_completed_epoch_from_replay(self) -> None:
        relay = AuthorizationRelay()
        relay.publish(
            Message(
                MessageType.AUTHORIZE_SEND,
                sequence=4,
                payload={"authorization_id": 17, "epoch_id": 3},
            )
        )

        relay.complete_epoch(3)
        agent = _AgentSocket()
        relay.add_peer(agent)

        self.assertEqual(agent.data, b"")


if __name__ == "__main__":
    unittest.main()
