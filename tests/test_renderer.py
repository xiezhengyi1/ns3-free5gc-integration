from __future__ import annotations

from dataclasses import replace
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import yaml

from adapters.free5gc_ueransim.compose_override import (
    AMF_CONTROL_IP,
    SMF_CONTROL_IP,
    UPF_CONTROL_IP,
    build_n3_network_plan,
    gnb_service_ip,
    upf_service_ip,
)
from bridge.common.ids import generate_run_id
from bridge.common.scenario import load_scenario
from bridge.orchestrator.config_renderer import (
    _render_ns3_flow_profiles,
    _render_user_plane_assets,
    render_run_assets,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RendererTest(unittest.TestCase):
    def test_flow_profile_renders_rlc_and_virtual_expiry(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s1_basic_single_slice.yaml")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "flow-profiles.tsv"

            _render_ns3_flow_profiles(scenario, output)

            rows = output.read_text(encoding="utf-8").splitlines()
            header = rows[0].split("\t")
            values = rows[1].split("\t")
            rendered = dict(zip(header, values, strict=True))
            self.assertEqual(rendered["rlc_mode"], "UM")
            self.assertEqual(rendered["virtual_expiry_ms"], "1000.0")

    def test_renders_gate_and_bearer_map_assets(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s1_basic_single_slice.yaml")
        flow = replace(
            scenario.flows[0],
            ue_ip="10.60.0.1",
            qfi=9,
            inner_protocol=17,
            ue_port=15000,
            remote_port=5000,
            rlc_mode="UM",
            virtual_expiry_ms=250.0,
        )
        scenario = replace(
            scenario,
            flows=(flow,),
            bridge=replace(
                scenario.bridge,
                user_plane_gate=replace(
                    scenario.bridge.user_plane_gate,
                    enabled=True,
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            generated_dir = Path(directory)
            plans = [
                type(
                    "Plan",
                    (),
                    {
                        "gnb_tap": "tgnb1",
                        "upf_tap": "tupf1",
                        "gnb_n3_ip": "10.201.1.2",
                        "upf_n3_ip": "10.201.1.3",
                    },
                )()
            ]

            gate_file, bearer_file = _render_user_plane_assets(
                scenario, plans, generated_dir
            )

            gate = json.loads(gate_file.read_text(encoding="utf-8"))
            bearer = json.loads(bearer_file.read_text(encoding="utf-8"))
            self.assertEqual(gate["gnb_tap"], "tgnb1")
            self.assertEqual(gate["flows"][0]["ue_ip"], "10.60.0.1")
            self.assertEqual(gate["flows"][0]["virtual_expiry_us"], 250000)
            self.assertEqual(bearer["flows"][0]["rlc_mode"], "UM")
            self.assertEqual(bearer["rng_seed"], scenario.seed)
            self.assertEqual(bearer["rng_run"], scenario.ns3.rng_run)

    def test_inline_harness_requires_explicit_n3_network(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s1_basic_single_slice.yaml")
        invalid = replace(
            scenario,
            bridge=replace(
                scenario.bridge,
                enable_inline_harness=True,
                n3_network_cidr=None,
            ),
        )

        with self.assertRaisesRegex(ValueError, "bridge.n3_network_cidr is required"):
            invalid.validate()

    def test_renders_single_upf_scenario_with_real_n3_network(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s1_basic_single_slice.yaml")
        n3_network_plan = build_n3_network_plan(scenario)
        self.assertIsNotNone(n3_network_plan)
        self.assertEqual(n3_network_plan.gateway_ip, "10.201.1.1")
        self.assertEqual(n3_network_plan.gnb_ips["gNB-1"], "10.201.1.2")
        run_id = generate_run_id("testrender")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            gnb_payload = yaml.safe_load((rendered.config_dir / "gNB-1-gnbcfg.yaml").read_text(encoding="utf-8"))
            ue_payload = yaml.safe_load((rendered.config_dir / "ue-telemed-uecfg.yaml").read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            upf_payload = yaml.safe_load((rendered.config_dir / "upfcfg.yaml").read_text(encoding="utf-8"))
            bridge_script = rendered.bridge_script.read_text(encoding="utf-8")

            self.assertEqual(gnb_payload["ngapIp"], gnb_service_ip(1))
            self.assertEqual(gnb_payload["gtpIp"], n3_network_plan.gnb_ips["gNB-1"])
            self.assertEqual(gnb_payload["amfConfigs"][0]["address"], AMF_CONTROL_IP)
            self.assertEqual(ue_payload["gnbSearchList"], [gnb_service_ip(1)])
            self.assertEqual(
                smf_payload["configuration"]["pfcp"],
                {
                    "nodeID": SMF_CONTROL_IP,
                    "listenAddr": SMF_CONTROL_IP,
                    "externalAddr": SMF_CONTROL_IP,
                },
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB1"]["nodeID"],
                n3_network_plan.gnb_ips["gNB-1"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["UPF"]["nodeID"],
                n3_network_plan.upf_ips["upf"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["UPF"]["interfaces"][0]["endpoints"],
                [n3_network_plan.upf_ips["upf"]],
            )
            self.assertEqual(upf_payload["pfcp"]["addr"], UPF_CONTROL_IP)
            self.assertEqual(upf_payload["gtpu"]["ifList"][0]["addr"], n3_network_plan.upf_ips["upf"])
            self.assertEqual(compose_payload["services"]["ueransim"]["command"], "./nr-gnb -c ./config/gnbcfg.yaml")
            self.assertEqual(
                compose_payload["services"]["free5gc-smf"]["networks"]["privnet"]["ipv4_address"],
                SMF_CONTROL_IP,
            )
            self.assertEqual(compose_payload["networks"]["n3net"]["ipam"]["config"][0]["subnet"], scenario.bridge.n3_network_cidr)
            self.assertEqual(
                compose_payload["services"]["ueransim"]["networks"]["n3net"]["ipv4_address"],
                n3_network_plan.gnb_ips["gNB-1"],
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-upf"]["networks"]["n3net"]["ipv4_address"],
                n3_network_plan.upf_ips["upf"],
            )
            self.assertNotIn("version", compose_payload)
            self.assertEqual(
                compose_payload["networks"]["n3net"]["ipam"]["config"][0]["gateway"],
                n3_network_plan.gateway_ip,
            )
            self.assertNotIn("10.210.", bridge_script)
            self.assertNotIn("n3g1", bridge_script)
            self.assertNotIn("n3u1", bridge_script)
            self.assertEqual(manifest["bridge_links"][0]["gnb_n3_ip"], n3_network_plan.gnb_ips["gNB-1"])
            self.assertEqual(manifest["bridge_links"][0]["upf_n3_ip"], n3_network_plan.upf_ips["upf"])
            self.assertIn("bridge-setup", [item["name"] for item in manifest["commands"]])
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_scenario_with_real_n3_network(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s2_medium_complexity.yaml")
        n3_network_plan = build_n3_network_plan(scenario)
        self.assertIsNotNone(n3_network_plan)
        run_id = generate_run_id("testulcl")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            i_upf_payload = yaml.safe_load((rendered.config_dir / "i-upf-1-upfcfg.yaml").read_text(encoding="utf-8"))
            psa_upf_payload = yaml.safe_load((rendered.config_dir / "psa-upf-1-upfcfg.yaml").read_text(encoding="utf-8"))

            self.assertEqual(len(manifest["bridge_links"]), 3)
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB1"]["nodeID"],
                n3_network_plan.gnb_ips["gNB-1"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB2"]["nodeID"],
                n3_network_plan.gnb_ips["gNB-2"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["I-UPF"]["nodeID"],
                n3_network_plan.upf_ips["i-upf-1"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["PSA-UPF"]["nodeID"],
                n3_network_plan.upf_ips["psa-upf-1"],
            )
            self.assertEqual(i_upf_payload["pfcp"]["addr"], upf_service_ip(1))
            self.assertEqual(psa_upf_payload["pfcp"]["addr"], upf_service_ip(2))
            self.assertEqual(i_upf_payload["gtpu"]["ifList"][0]["addr"], n3_network_plan.upf_ips["i-upf-1"])
            self.assertEqual(psa_upf_payload["gtpu"]["ifList"][0]["addr"], n3_network_plan.upf_ips["psa-upf-1"])
            self.assertEqual(
                compose_payload["services"]["free5gc-i-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(1),
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-psa-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(2),
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-i-upf"]["networks"]["n3net"]["ipv4_address"],
                n3_network_plan.upf_ips["i-upf-1"],
            )
            self.assertEqual(
                compose_payload["services"]["ueransim"]["networks"]["n3net"]["ipv4_address"],
                n3_network_plan.gnb_ips["gNB-1"],
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_inline_bridge_script_from_real_n3_ips(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "s1_basic_single_slice.yaml")
        n3_network_plan = build_n3_network_plan(scenario)
        self.assertIsNotNone(n3_network_plan)
        scenario = replace(
            scenario,
            ns3=replace(
                scenario.ns3,
                bridge_link_rate_mbps=250.0,
                bridge_link_delay_ms=2.0,
                bridge_link_loss_rate=0.05,
            ),
        )
        scenario.validate()
        run_id = generate_run_id("testinline")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            bridge_script = rendered.bridge_script.read_text(encoding="utf-8")
            bridge_probe_script = (rendered.generated_dir / "probe-inline-bridge.sh").read_text(encoding="utf-8")
            command_names = [item["name"] for item in manifest["commands"]]
            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")

            self.assertLess(command_names.index("compose-up-gnb"), command_names.index("bridge-setup"))
            self.assertLess(command_names.index("bridge-setup"), command_names.index("compose-up-smf"))
            self.assertIn("--bridge-gnb-taps", ns3_run["argv"])
            self.assertIn("tgnb1", ns3_run["argv"])
            self.assertIn("--bridge-upf-taps", ns3_run["argv"])
            self.assertIn("tupf1", ns3_run["argv"])
            self.assertIn("resolve_ns_ifname_by_ipv4", bridge_script)
            self.assertIn(n3_network_plan.gnb_ips["gNB-1"], bridge_script)
            self.assertIn(n3_network_plan.upf_ips["upf"], bridge_script)
            self.assertIn("ip link set \"$gnb_host_if_1\" nomaster || true", bridge_script)
            self.assertIn("tcpdump -eni \"$gnb_n3_if_1\"", bridge_probe_script)
            self.assertIn("tcpdump -eni \"$upf_n3_if_1\"", bridge_probe_script)
            self.assertNotIn("10.210.", bridge_script)
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
