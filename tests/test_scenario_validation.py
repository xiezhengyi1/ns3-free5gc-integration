from __future__ import annotations

import unittest
from pathlib import Path

from bridge.common.scenario import ScenarioConfig, load_scenario


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _base_payload() -> dict[str, object]:
    return {
        "name": "validation-smoke",
        "scenario_id": "validation-smoke",
        "tick_ms": 100,
        "seed": 1,
        "slices": [
            {
                "sst": 2,
                "sd": "000002",
                "label": "slice-2-000002",
                "resource": {
                    "capacity_dl_mbps": 30.0,
                    "capacity_ul_mbps": 14.0,
                    "guaranteed_dl_mbps": 16.0,
                    "guaranteed_ul_mbps": 8.0,
                },
                "qos": {
                    "latency_ms": 6.0,
                    "jitter_ms": 2.5,
                    "loss_rate": 0.0008,
                    "processing_delay_ms": 0.8,
                },
            }
        ],
        "upfs": [{"name": "i-upf-b", "role": "branching-upf", "dnn": "internet"}],
        "gnbs": [
            {
                "name": "gNB-3",
                "alias": "gnb3.free5gc.org",
                "slices": ["slice-2-000002"],
                "backhaul_upf": "i-upf-b",
            }
        ],
        "ues": [
            {
                "name": "ue8",
                "supi": "imsi-208930000000008",
                "gnb": "gNB-3",
                "key": "8baf473f2f8fd09487cccbd7097c6862",
                "op": "8e27b6af0e692e750f32667a3b14605d",
                "op_type": "OPC",
                "amf": "8000",
                "sessions": [
                    {
                        "slice_ref": "slice-2-000002",
                        "session_ref": "imsi-208930000000008:app-9231:slice-2-000002:internet",
                        "apn": "internet",
                        "type": "IPv4",
                        "five_qi": 7,
                        "app_id": "app-9231",
                    }
                ],
            }
        ],
        "apps": [
            {
                "app_id": "app-9231",
                "name": "Telemedicine",
                "supi": "imsi-208930000000008",
                "ue_name": "ue8",
                "flow_ids": ["flow-3521"],
            }
        ],
        "flows": [
            {
                "flow_id": "flow-3521",
                "name": "Telemedicine_video_1",
                "supi": "imsi-208930000000008",
                "app_id": "app-9231",
                "slice_ref": "slice-2-000002",
                "session_ref": "imsi-208930000000008:app-9231:slice-2-000002:internet",
                "ue_name": "ue8",
                "app_name": "Telemedicine",
                "five_qi": 7,
                "service_type": "URLLC",
                "service_type_id": 2,
                "dl_packet_size_bytes": 1000.0,
                "ul_packet_size_bytes": 1000.0,
                "dl_arrival_rate_pps": 375.0,
                "ul_arrival_rate_pps": 475.0,
                "current_slice_snssai": "02000002",
                "sla_target": {
                    "latency_ms": 15.0,
                    "jitter_ms": 2.5,
                    "loss_rate": 0.0008,
                    "bandwidth_dl_mbps": 3.0,
                    "bandwidth_ul_mbps": 3.8,
                    "guaranteed_bandwidth_dl_mbps": 2.4,
                    "guaranteed_bandwidth_ul_mbps": 3.0,
                    "priority": 1,
                },
            }
        ],
        "free5gc": {
            "compose_file": str(PROJECT_ROOT / "README.md"),
            "config_root": str(PROJECT_ROOT),
        },
        "ns3": {
            "ns3_root": str(PROJECT_ROOT),
            "slice_isolation": True,
        },
    }


class ScenarioValidationTest(unittest.TestCase):
    def test_s3_high_complexity_passes_validation(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s3_high_complexity.yaml")
        self.assertEqual(scenario.name, "high-complexity-ulcl-4upf")

    def test_rejects_flow_sla_stricter_than_slice_qos(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["sla_target"]["jitter_ms"] = 2.0

        with self.assertRaisesRegex(ValueError, "jitter target"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_offered_load_above_bandwidth_target(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["sla_target"]["bandwidth_dl_mbps"] = 2.0

        with self.assertRaisesRegex(ValueError, "offered DL load"):
            ScenarioConfig.from_dict(payload)

    def test_loads_gated_user_plane_defaults(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {})["user_plane_gate"] = {"enabled": True}

        scenario = ScenarioConfig.from_dict(payload)

        self.assertEqual(scenario.flows[0].rlc_mode, "UM")
        self.assertEqual(scenario.flows[0].virtual_expiry_ms, 1000.0)
        self.assertEqual(scenario.ns3.rng_run, 1)
        self.assertEqual(scenario.ns3.virtual_epoch_us, 100_000)
        self.assertEqual(scenario.ns3.channel_update_ms, 10.0)
        self.assertTrue(scenario.ns3.shadowing_enabled)
        self.assertTrue(scenario.bridge.user_plane_gate.enabled)
        self.assertTrue(scenario.bridge.user_plane_gate.fail_closed)
        self.assertEqual(scenario.bridge.user_plane_gate.max_pending_packets, 8192)

    def test_rejects_invalid_rlc_mode(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["rlc_mode"] = "TM"

        with self.assertRaisesRegex(ValueError, "rlc_mode"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_non_positive_virtual_expiry(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["virtual_expiry_ms"] = 0

        with self.assertRaisesRegex(ValueError, "virtual_expiry_ms"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_invalid_gate_capacity(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {})["user_plane_gate"] = {
            "enabled": True,
            "max_pending_packets": 0,
        }

        with self.assertRaisesRegex(ValueError, "max_pending_packets"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_non_positive_ns3_virtual_time_parameters(self) -> None:
        payload = _base_payload()
        payload["ns3"]["virtual_epoch_us"] = 0

        with self.assertRaisesRegex(ValueError, "virtual_epoch_us"):
            ScenarioConfig.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
