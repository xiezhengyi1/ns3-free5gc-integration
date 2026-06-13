from __future__ import annotations

import unittest

from bridge.user_plane.cli import GateRuntimeConfig
from bridge.user_plane.protocol import MessageType


class UserPlaneCliTest(unittest.TestCase):
    def test_loads_runtime_config_and_flow_bindings(self) -> None:
        config = GateRuntimeConfig.from_dict(
            {
                "gnb_tap": "tgnb1",
                "upf_tap": "tupf1",
                "socket_path": "/tmp/gate.sock",
                "authorization_socket": "/tmp/gate.sock.agents",
                "max_pending_packets": 16,
                "max_pending_bytes": 4096,
                "fail_closed": True,
                "gnb_ips": ["10.0.0.2"],
                "upf_ips": ["10.0.0.3"],
                "flows": [
                    {
                        "flow_id": "flow-1",
                        "ue_ip": "10.60.0.1",
                        "qfi": 9,
                        "inner_protocol": 17,
                        "ue_port": 4000,
                        "remote_port": 5000,
                        "virtual_expiry_us": 5000,
                    }
                ],
            }
        )

        self.assertEqual(config.gnb_tap, "tgnb1")
        self.assertEqual(config.authorization_socket, "/tmp/gate.sock.agents")
        self.assertEqual(config.bindings[0].flow_id, "flow-1")
        self.assertEqual(config.bindings[0].qfi, 9)
        self.assertEqual(config.virtual_expiry_us_by_flow, {"flow-1": 5000})

    def test_rejects_duplicate_flow_ids(self) -> None:
        flow = {
            "flow_id": "flow-1",
            "ue_ip": "10.60.0.1",
            "virtual_expiry_us": 5000,
        }
        payload = {
            "gnb_tap": "tgnb1",
            "upf_tap": "tupf1",
            "socket_path": "/tmp/gate.sock",
            "gnb_ips": ["10.0.0.2"],
            "upf_ips": ["10.0.0.3"],
            "flows": [flow, dict(flow)],
        }

        with self.assertRaisesRegex(ValueError, "duplicate"):
            GateRuntimeConfig.from_dict(payload)

    def test_protocol_defines_explicit_epoch_start(self) -> None:
        self.assertIsInstance(MessageType.EPOCH_START, MessageType)


if __name__ == "__main__":
    unittest.main()
