from __future__ import annotations

import unittest
from pathlib import Path

from bridge.common.scenario import ScenarioConfig, load_scenario
from bridge.common.topology import resolve_scenario_topology


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, object]:
    return {
        "name": "validation-smoke",
        "scenario_id": "validation-smoke",
        "tick_ms": 100,
        "seed": 1,
        "slices": [{
            "sst": 2,
            "sd": "000002",
            "label": "slice-2-000002",
            "resource": {
                "capacity_dl_mbps": 30.0,
                "capacity_ul_mbps": 14.0,
                "guaranteed_dl_mbps": 16.0,
                "guaranteed_ul_mbps": 8.0,
            },
            "qos": {"latency_ms": 6.0, "jitter_ms": 2.5, "loss_rate": 0.0008},
        }],
        "upfs": [{"name": "upf-a", "role": "branching-upf", "dnn": "internet"}],
        "gnbs": [{
            "name": "gNB-1",
            "alias": "gnb1.free5gc.org",
            "slices": ["slice-2-000002"],
            "backhaul_upfs": ["upf-a"],
        }],
        "ues": [{
            "name": "ue1",
            "supi": "imsi-208930000000001",
            "gnb": "gNB-1",
            "key": "8baf473f2f8fd09487cccbd7097c6862",
            "op": "8e27b6af0e692e750f32667a3b14605d",
            "sessions": [{
                "slice_ref": "slice-2-000002",
                "session_ref": "ue1:internet",
                "apn": "internet",
                "app_id": "app-1",
            }],
        }],
        "apps": [{"app_id": "app-1", "name": "App", "supi": "imsi-208930000000001"}],
        "flows": [{
            "flow_id": "flow-1",
            "name": "Flow",
            "supi": "imsi-208930000000001",
            "ue_name": "ue1",
            "app_id": "app-1",
            "slice_ref": "slice-2-000002",
            "session_ref": "ue1:internet",
            "five_qi": 7,
            "sla_target": {"latency_ms": 15.0, "jitter_ms": 2.5, "loss_rate": 0.0008},
        }],
        "free5gc": {"compose_file": str(PROJECT_ROOT / "README.md"), "config_root": str(PROJECT_ROOT)},
        "ns3": {"ns3_root": str(PROJECT_ROOT), "slice_isolation": True},
        "n3_network": {"name": "n3net", "cidr": "10.201.1.0/29"},
    }


class ScenarioValidationTest(unittest.TestCase):
    def test_all_shipped_base_scenarios_are_valid(self) -> None:
        for name in ("s1_basic_single_slice.yaml", "s2_medium_complexity.yaml", "s3_high_complexity.yaml"):
            with self.subTest(name=name):
                self.assertTrue(load_scenario(PROJECT_ROOT / "scenarios" / name).flows)

    def test_rejects_removed_singular_backhaul_upf(self) -> None:
        payload = _payload()
        payload["gnbs"][0].pop("backhaul_upfs")
        payload["gnbs"][0]["backhaul_upf"] = "upf-a"
        with self.assertRaisesRegex(ValueError, "backhaul_upf was removed"):
            ScenarioConfig.from_dict(payload)

    def test_preserves_multi_n3_links_and_flow_selection(self) -> None:
        payload = _payload()
        payload["upfs"].append({"name": "upf-b", "role": "branching-upf", "dnn": "internet"})
        payload["gnbs"][0]["backhaul_upfs"] = ["upf-a", "upf-b"]
        payload["flows"][0]["upf_ref"] = "upf-b"
        scenario = ScenarioConfig.from_dict(payload)
        topology = resolve_scenario_topology(scenario)
        self.assertEqual(topology.gnb_to_upfs["gNB-1"], ("upf-a", "upf-b"))
        self.assertEqual(scenario.resolve_flow_upf(scenario.flows[0]), "upf-b")

    def test_rejects_flow_upf_without_n3_link(self) -> None:
        payload = _payload()
        payload["upfs"].append({"name": "upf-b", "role": "branching-upf", "dnn": "internet"})
        payload["flows"][0]["upf_ref"] = "upf-b"
        with self.assertRaisesRegex(ValueError, "not linked"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_flow_sla_stricter_than_slice_qos(self) -> None:
        payload = _payload()
        payload["flows"][0]["sla_target"]["jitter_ms"] = 2.0
        with self.assertRaisesRegex(ValueError, "jitter target"):
            ScenarioConfig.from_dict(payload)

    def test_rejects_too_small_n3_subnet(self) -> None:
        payload = _payload()
        payload["n3_network"]["cidr"] = "10.201.1.0/30"
        with self.assertRaisesRegex(ValueError, "enough host addresses"):
            ScenarioConfig.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
