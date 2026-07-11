from __future__ import annotations

import unittest
from pathlib import Path

from bridge.common.scenario import ScenarioConfig, load_scenario
from bridge.common.topology import resolve_scenario_topology


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
                "backhaul_upfs": ["i-upf-b"],
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
    def test_rejects_removed_singular_backhaul_upf(self) -> None:
        payload = _base_payload()
        payload["gnbs"][0].pop("backhaul_upfs")
        payload["gnbs"][0]["backhaul_upf"] = "i-upf-b"

        with self.assertRaisesRegex(ValueError, "backhaul_upf was removed"):
            ScenarioConfig.from_dict(payload)

    def test_s3_high_complexity_passes_validation(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s3_high_complexity.yaml")
        self.assertEqual(scenario.name, "high-complexity-ulcl-4upf")

    def test_rejects_flow_sla_stricter_than_slice_qos(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["sla_target"]["jitter_ms"] = 2.0

        with self.assertRaisesRegex(ValueError, "jitter target"):
            ScenarioConfig.from_dict(payload)

    def test_allows_offered_load_above_bandwidth_target_for_congestion_scenarios(self) -> None:
        payload = _base_payload()
        payload["flows"][0]["sla_target"]["bandwidth_dl_mbps"] = 2.0

        scenario = ScenarioConfig.from_dict(payload)

        self.assertEqual(scenario.flows[0].sla_target.bandwidth_dl_mbps, 2.0)

    def test_loads_gated_user_plane_defaults(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {}).update(
            {
                "enable_inline_harness": True,
                "n3_network_cidr": "10.201.1.0/29",
                "user_plane_gate": {"enabled": True},
            }
        )
        payload["flows"][0].update({"ue_ip": "10.60.0.1", "qfi": 9})
        payload["ns3"]["simulator"] = "DefaultSimulatorImpl"

        scenario = ScenarioConfig.from_dict(payload)

        self.assertEqual(scenario.flows[0].rlc_mode, "UM")
        self.assertEqual(scenario.flows[0].virtual_expiry_ms, 1000.0)
        self.assertEqual(scenario.ns3.rng_run, 1)
        self.assertEqual(scenario.ns3.virtual_epoch_us, 100_000)
        self.assertEqual(scenario.ns3.channel_update_ms, 10.0)
        self.assertTrue(scenario.ns3.shadowing_enabled)
        self.assertTrue(scenario.bridge.user_plane_gate.enabled)
        self.assertEqual(scenario.bridge.user_plane_gate.max_pending_packets, 8192)

    def test_gate_accepts_multiple_gnb_upf_links(self) -> None:
        payload = _base_payload()
        payload["upfs"].append(
            {"name": "i-upf-c", "role": "branching-upf", "dnn": "internet"}
        )
        payload["gnbs"].append(
            {
                "name": "gNB-4",
                "alias": "gnb4.free5gc.org",
                "slices": ["slice-2-000002"],
                "backhaul_upfs": ["i-upf-c"],
            }
        )
        payload["gnbs"][0]["backhaul_upfs"] = ["i-upf-b", "i-upf-c"]
        payload["flows"][0]["upf_ref"] = "i-upf-c"
        payload.setdefault("bridge", {}).update(
            {
                "enable_inline_harness": True,
                "n3_network_cidr": "10.201.1.0/29",
                "user_plane_gate": {"enabled": True},
            }
        )
        payload["flows"][0].update({"ue_ip": "10.60.0.1", "qfi": 9})
        payload["ns3"]["simulator"] = "DefaultSimulatorImpl"

        scenario = ScenarioConfig.from_dict(payload)
        topology = resolve_scenario_topology(scenario)

        self.assertEqual(len(scenario.gnbs), 2)
        self.assertEqual(len(scenario.upfs), 2)
        self.assertEqual(
            topology.gnb_to_upfs,
            {
                "gNB-3": ("i-upf-b", "i-upf-c"),
                "gNB-4": ("i-upf-c",),
            },
        )
        self.assertEqual(scenario.resolve_flow_upf(scenario.flows[0]), "i-upf-c")

    def test_rejects_flow_upf_without_n3_link(self) -> None:
        payload = _base_payload()
        payload["upfs"].append(
            {"name": "i-upf-c", "role": "branching-upf", "dnn": "internet"}
        )
        payload["flows"][0]["upf_ref"] = "i-upf-c"

        with self.assertRaisesRegex(ValueError, "not linked"):
            ScenarioConfig.from_dict(payload)

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

    def test_gate_requires_reproducible_flow_binding(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {})["enable_inline_harness"] = True
        payload["bridge"]["n3_network_cidr"] = "10.201.1.0/29"
        payload["bridge"]["user_plane_gate"] = {"enabled": True}
        payload["ns3"]["simulator"] = "DefaultSimulatorImpl"

        with self.assertRaisesRegex(ValueError, "ue_ip"):
            ScenarioConfig.from_dict(payload)

        payload["flows"][0].update(
            {
                "ue_ip": "10.60.0.1",
                "qfi": 9,
                "inner_protocol": 17,
                "ue_port": 15000,
                "remote_port": 5000,
            }
        )
        scenario = ScenarioConfig.from_dict(payload)
        self.assertEqual(scenario.flows[0].ue_ip, "10.60.0.1")
        self.assertEqual(scenario.flows[0].qfi, 9)

    def test_rejects_removed_fail_closed_policy_field(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {}).update(
            {
                "enable_inline_harness": True,
                "n3_network_cidr": "10.201.1.0/29",
                "user_plane_gate": {"enabled": True, "fail_closed": False},
            }
        )
        payload["flows"][0].update({"ue_ip": "10.60.0.1", "qfi": 9})
        payload["ns3"]["simulator"] = "DefaultSimulatorImpl"

        with self.assertRaisesRegex(ValueError, "fail_closed"):
            ScenarioConfig.from_dict(payload)

    def test_gate_rejects_realtime_scheduler(self) -> None:
        payload = _base_payload()
        payload.setdefault("bridge", {}).update(
            {
                "enable_inline_harness": True,
                "n3_network_cidr": "10.201.1.0/29",
                "user_plane_gate": {"enabled": True},
            }
        )
        payload["flows"][0].update({"ue_ip": "10.60.0.1", "qfi": 9})
        payload["ns3"]["simulator"] = "RealtimeSimulatorImpl"

        with self.assertRaisesRegex(ValueError, "virtual-time"):
            ScenarioConfig.from_dict(payload)

    def test_gate_rejects_unsupported_or_conflicting_5qi_rlc_mapping(self) -> None:
        for five_qi, rlc_mode, message in (
            (200, "UM", "unsupported by 5G-LENA"),
            (7, "AM", "conflicts with 5G-LENA"),
        ):
            with self.subTest(five_qi=five_qi, rlc_mode=rlc_mode):
                payload = _base_payload()
                payload.setdefault("bridge", {}).update(
                    {
                        "enable_inline_harness": True,
                        "n3_network_cidr": "10.201.1.0/29",
                        "user_plane_gate": {"enabled": True},
                    }
                )
                payload["flows"][0].update(
                    {
                        "ue_ip": "10.60.0.1",
                        "qfi": 9,
                        "five_qi": five_qi,
                        "rlc_mode": rlc_mode,
                    }
                )
                payload["ns3"]["simulator"] = "DefaultSimulatorImpl"

                with self.assertRaisesRegex(ValueError, message):
                    ScenarioConfig.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
