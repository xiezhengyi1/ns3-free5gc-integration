from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from adapters.free5gc_ueransim.bridge_setup import BridgeInterfacePlan
from bridge.common.scenario import load_scenario
from bridge.common.topology import resolve_scenario_topology
from bridge.orchestrator.process_plan import build_run_manifest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class GatedProcessPlanTest(unittest.TestCase):
    def test_manifest_starts_gate_before_ns3_and_disables_tapbridge_arguments(self) -> None:
        scenario = load_scenario(
            PROJECT_ROOT / "scenarios" / "free5gc_ueransim_gtpu_nr.yaml"
        )
        topology = resolve_scenario_topology(scenario)
        plan = BridgeInterfacePlan(
            link_index=1,
            segment_index=1,
            gnb_name="gNB-1",
            gnb_service="ueransim",
            upf_name="upf",
            upf_service="free5gc-upf",
            gnb_tap="tgnb1",
            upf_tap="tupf1",
            gnb_n3_ip="10.201.1.2",
            upf_n3_ip="10.201.1.3",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = build_run_manifest(
                project_root=PROJECT_ROOT,
                scenario=scenario,
                run_id="gate-plan",
                run_dir=root,
                compose_file=root / "compose.yaml",
                bridge_script=root / "bridge.sh",
                bridge_probe_script=root / "probe.sh",
                bridge_plans=[plan],
                snapshot_file=root / "ns3" / "tick-snapshots.jsonl",
                clock_file=root / "clock.json",
                flow_profile_file=root / "flows.tsv",
                slice_resource_file=root / "slices.tsv",
                user_plane_gate_file=root / "user-plane-gate.json",
                bearer_map_file=root / "bearer-map.json",
                state_db=root / "state.db",
                archive_dir=root / "archive",
                service_map={
                    "gnb": {"gNB-1": "ueransim"},
                    "ue": {
                        "ue-telemed": "ue-telemed",
                        "ue-ar": "ue-ar",
                    },
                    "upf": {"upf": "free5gc-upf"},
                },
                core_services=["free5gc-smf", "free5gc-upf"],
                ran_services=["ueransim", "ue-telemed", "ue-ar"],
                subscriber_payloads=[],
                free5gc_webui_url="http://127.0.0.1:5000",
                resolved_topology=topology,
            )

        commands = {command.name: command for command in manifest.commands}
        names = [command.name for command in manifest.commands]
        self.assertLess(names.index("bridge-setup"), names.index("user-plane-gate"))
        self.assertEqual(names.index("user-plane-gate"), names.index("bridge-setup") + 1)
        self.assertLess(names.index("user-plane-gate"), names.index("ns3-run"))
        self.assertLess(names.index("ns3-run"), names.index("validate-gated-metrics"))
        self.assertLess(names.index("validate-gated-metrics"), names.index("compose-down"))
        self.assertNotIn("--controlled", commands["real-ue-flows"].argv)
        self.assertIn("--authorization-socket", commands["real-ue-flows"].argv)
        self.assertIn("--user-plane-gate-socket", commands["ns3-run"].argv)
        self.assertEqual(
            commands["validate-gated-metrics"].argv[:3],
            ["python3", "-m", "bridge.orchestrator.metrics"],
        )
        self.assertIn(
            str(root / "ns3" / "packet-events.jsonl"),
            commands["validate-gated-metrics"].argv,
        )
        self.assertIn(
            str(root / "ns3" / "packet-kpis.jsonl"),
            commands["validate-gated-metrics"].argv,
        )
        self.assertIn("--wait-seconds", commands["validate-gated-metrics"].argv)
        self.assertNotIn("--bridge-gnb-taps", commands["ns3-run"].argv)
        self.assertEqual(manifest.user_plane_gate_file, str(root / "user-plane-gate.json"))


if __name__ == "__main__":
    unittest.main()
