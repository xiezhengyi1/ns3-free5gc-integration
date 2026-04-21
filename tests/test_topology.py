from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from bridge.common.scenario import load_scenario
from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import resolve_scenario_topology


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_semantic_graph_summary() -> dict[str, object]:
    return {
        "snapshot_id": "graph-current-1",
        "trigger_event": "Manual-Reset",
        "nodes": [
            {
                "node_key": "ue:imsi-208930000000001",
                "node_type": "ue",
                "label": "imsi-208930000000001",
                "properties": {"supi": "imsi-208930000000001"},
            },
            {
                "node_key": "app:imsi-208930000000001:app-6757",
                "node_type": "app",
                "label": "Remote_Drive",
                "properties": {
                    "id": "app-6757",
                    "name": "Remote_Drive",
                    "supi": "imsi-208930000000001",
                },
            },
            {
                "node_key": "flow:imsi-208930000000001:app-6757:flow-7743",
                "node_type": "flow",
                "label": "Remote_Drive_video_1",
                "properties": {
                    "id": "flow-7743",
                    "name": "Remote_Drive_video_1",
                    "supi": "imsi-208930000000001",
                    "app_id": "app-6757",
                    "app_name": "Remote_Drive",
                    "service": {
                        "service_type": "eMBB",
                        "service_type_id": 1,
                    },
                    "traffic": {
                        "packet_size": 12000.0,
                        "arrival_rate": 500.0,
                    },
                    "sla": {
                        "latency": 12.0,
                        "jitter": 2.0,
                        "priority": 1,
                        "loss_rate": 0.0005,
                        "bandwidth_dl": 18.0,
                        "bandwidth_ul": 16.0,
                        "guaranteed_bandwidth_dl": 14.0,
                        "guaranteed_bandwidth_ul": 12.0,
                    },
                    "allocation": {
                        "optimize_requested": False,
                        "current_slice_snssai": "01000001",
                        "allocated_bandwidth_dl": 18.0,
                        "allocated_bandwidth_ul": 16.0,
                    },
                    "telemetry": {},
                },
            },
            {
                "node_key": "slice:01000001",
                "node_type": "slice",
                "label": "S2_Silver",
                "properties": {
                    "name": "S2_Silver",
                    "snssai": "01000001",
                    "sst": 1,
                    "sd": "000001",
                },
            },
            {
                "node_key": "ran_node:AN_gNB_0",
                "node_type": "ran_node",
                "label": "AN_gNB_0",
                "properties": {
                    "name": "AN_gNB_0",
                    "node_type": "AN",
                    "hosted_slice_snssais": ["01000001"],
                    "capacity": {},
                    "telemetry": {},
                },
            },
            {
                "node_key": "core_node:UPF_0",
                "node_type": "core_node",
                "label": "UPF_0",
                "properties": {
                    "name": "UPF_0",
                    "node_type": "CN",
                    "hosted_slice_snssais": ["01000001"],
                    "capacity": {},
                    "telemetry": {},
                },
            },
        ],
        "edges": [
            {
                "edge_key": "ue:imsi-208930000000001->app:imsi-208930000000001:app-6757",
                "edge_type": "owns",
                "source_key": "ue:imsi-208930000000001",
                "target_key": "app:imsi-208930000000001:app-6757",
                "properties": {"supi": "imsi-208930000000001"},
            },
            {
                "edge_key": "app:imsi-208930000000001:app-6757->flow:imsi-208930000000001:app-6757:flow-7743",
                "edge_type": "contains_flow",
                "source_key": "app:imsi-208930000000001:app-6757",
                "target_key": "flow:imsi-208930000000001:app-6757:flow-7743",
                "properties": {"app_id": "app-6757"},
            },
            {
                "edge_key": "flow:imsi-208930000000001:app-6757:flow-7743->slice:01000001",
                "edge_type": "served_by_slice",
                "source_key": "flow:imsi-208930000000001:app-6757:flow-7743",
                "target_key": "slice:01000001",
                "properties": {"slice": "01000001"},
            },
            {
                "edge_key": "slice:01000001->ran_node:AN_gNB_0",
                "edge_type": "hosted_on",
                "source_key": "slice:01000001",
                "target_key": "ran_node:AN_gNB_0",
                "properties": {"hosted": True},
            },
            {
                "edge_key": "slice:01000001->core_node:UPF_0",
                "edge_type": "hosted_on",
                "source_key": "slice:01000001",
                "target_key": "core_node:UPF_0",
                "properties": {"hosted": True},
            },
        ],
        "metrics": [],
    }


class TopologyResolutionTest(unittest.TestCase):
    def test_loads_graph_derived_entities(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "policy_graph_multi_gnb.yaml")

        self.assertEqual([item.slice_id for item in scenario.slices], ["slice-1-010203"])
        self.assertEqual(scenario.slices[0].label, "embb")
        self.assertEqual([(item.name, item.role) for item in scenario.upfs], [("upf", "upf")])
        self.assertEqual([item.name for item in scenario.gnbs], ["gnb1", "gnb2"])
        self.assertEqual(scenario.gnbs[1].alias, "gnb2.free5gc.org")
        self.assertEqual(scenario.gnbs[1].slices, ("slice-1-010203",))
        self.assertEqual([item.name for item in scenario.ues], ["ue1", "ue2"])
        self.assertEqual(scenario.ues[0].free5gc_policy.target_gnb, "gnb2")
        self.assertEqual(scenario.ues[0].free5gc_policy.preferred_gnbs, ("gnb2", "gnb1"))
        self.assertEqual(scenario.ues[0].sessions[0].app_id, "policy-flow-1")
        self.assertEqual(scenario.ues[1].gnb, "gnb1")

    def test_policy_overrides_graph_attachment(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "policy_graph_multi_gnb.yaml")
        resolved = resolve_scenario_topology(scenario)

        self.assertEqual(resolved.ue_to_gnb["ue1"], "gnb2")
        self.assertEqual(resolved.ue_to_gnb["ue2"], "gnb1")
        self.assertEqual(resolved.gnb_to_upf["gnb1"], "upf")
        self.assertEqual(resolved.gnb_to_upf["gnb2"], "upf")
        self.assertEqual(resolved.gnb_positions["gnb2"].to_tuple(), (200.0, 0.0, 10.0))
        self.assertEqual(resolved.ue_positions["ue1"].to_tuple(), (190.0, 0.0, 1.5))

    def test_loads_ulcl_graph_derived_entities(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_gnb.yaml")
        resolved = resolve_scenario_topology(scenario)

        self.assertEqual(
            [(item.name, item.role) for item in scenario.upfs],
            [("i-upf", "branching-upf"), ("psa-upf", "anchor-upf")],
        )
        self.assertEqual([item.name for item in scenario.gnbs], ["gnb1", "gnb2"])
        self.assertEqual([item.name for item in scenario.ues], ["ue1", "ue2"])
        self.assertEqual(scenario.ues[0].free5gc_policy.target_gnb, "gnb2")
        self.assertEqual(scenario.ues[0].free5gc_policy.preferred_gnbs, ("gnb2", "gnb1"))
        self.assertEqual(resolved.ue_to_gnb["ue1"], "gnb2")
        self.assertEqual(resolved.ue_to_gnb["ue2"], "gnb1")
        self.assertEqual(resolved.gnb_to_upf["gnb1"], "i-upf")
        self.assertEqual(resolved.gnb_to_upf["gnb2"], "i-upf")
        self.assertEqual(resolved.gnb_positions["gnb2"].to_tuple(), (200.0, 0.0, 10.0))
        self.assertEqual(resolved.ue_positions["ue1"].to_tuple(), (190.0, 0.0, 1.5))

    def test_loads_ulcl_multi_slice_graph_entities(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_slice_multi_gnb.yaml")
        resolved = resolve_scenario_topology(scenario)

        self.assertEqual([item.slice_id for item in scenario.slices], ["slice-1-010203", "slice-1-112233"])
        self.assertEqual([item.name for item in scenario.gnbs], ["gnb1", "gnb2"])
        self.assertEqual([item.name for item in scenario.ues], ["ue1", "ue2"])
        self.assertEqual([session.session_ref for session in scenario.ues[0].sessions], ["ue1-video-session", "ue1-control-session"])
        self.assertEqual([session.slice_ref for session in scenario.ues[0].sessions], ["slice-1-010203", "slice-1-112233"])
        self.assertEqual(scenario.flows[0].session_ref, "ue1-video-session")
        self.assertEqual(scenario.flows[1].session_ref, "ue1-control-session")
        self.assertEqual(resolved.ue_to_gnb["ue1"], "gnb2")
        self.assertEqual(resolved.ue_to_gnb["ue2"], "gnb1")
        self.assertEqual(resolved.gnb_to_upf["gnb1"], "i-upf")
        self.assertEqual(resolved.gnb_to_upf["gnb2"], "i-upf")

    def test_loads_semantic_graph_snapshot_into_scenario_model(self) -> None:
        payload = {
            "name": "semantic-graph-snapshot",
            "scenario_id": "semantic-graph-snapshot",
            "tick_ms": 1000,
            "seed": 1,
            "ues": [
                {
                    "name": "ue1",
                    "supi": "imsi-208930000000001",
                    "key": "8baf473f2f8fd09487cccbd7097c6862",
                    "op": "8e27b6af0e692e750f32667a3b14605d",
                    "op_type": "OPC",
                    "amf": "8000",
                }
            ],
            "free5gc": {
                "compose_file": "/home/xiezhengyi/workspace/free5gc-compose/docker-compose.yaml",
                "config_root": "/home/xiezhengyi/workspace/free5gc-compose/config",
            },
            "ns3": {
                "ns3_root": "/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1",
            },
            "writer": {
                "graph_db_url": "postgresql://postgres:123456@localhost:5433/multiagents_db",
            },
            "topology": {
                "graph_snapshot_id": "graph-current-1",
            },
        }

        with mock.patch(
            "bridge.common.scenario.load_graph_snapshot_payload",
            return_value=build_semantic_graph_summary(),
        ):
            scenario = ScenarioConfig.from_dict(payload)

        self.assertEqual(scenario.topology.graph_snapshot_id, "graph-current-1")
        self.assertEqual(scenario.gnbs[0].name, "AN_gNB_0")
        self.assertEqual(scenario.gnbs[0].backhaul_upf, "UPF_0")
        self.assertEqual(scenario.ues[0].gnb, "AN_gNB_0")
        self.assertEqual(scenario.apps[0].app_id, "app-6757")
        self.assertEqual(scenario.flows[0].flow_id, "flow-7743")
        self.assertEqual(scenario.flows[0].service_type, "eMBB")
        self.assertEqual(scenario.flows[0].packet_size_bytes, 12000.0)
        self.assertEqual(scenario.flows[0].sla_target.bandwidth_dl_mbps, 18.0)
        self.assertEqual(scenario.flows[0].sla_target.guaranteed_bandwidth_ul_mbps, 12.0)


if __name__ == "__main__":
    unittest.main()