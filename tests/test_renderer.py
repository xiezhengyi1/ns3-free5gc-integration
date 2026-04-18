from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import yaml

from adapters.free5gc_ueransim.compose_override import (
    AMF_CONTROL_IP,
    SMF_CONTROL_IP,
    UPF_CONTROL_IP,
    gnb_service_ip,
    upf_service_ip,
)
from bridge.common.ids import generate_run_id
from bridge.common.scenario import load_scenario
from bridge.orchestrator.config_renderer import render_run_assets


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RendererTest(unittest.TestCase):
    def test_renders_baseline_single_upf(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_single_upf.yaml")
        run_id = generate_run_id("testrender")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            self.assertTrue(rendered.compose_file.exists())
            self.assertTrue(rendered.bridge_script.exists())
            self.assertTrue((rendered.run_dir / "run-manifest.json").exists())
            self.assertTrue((rendered.config_dir / "gnb1-gnbcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "ue1-uecfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "amfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "smfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "upfcfg.yaml").exists())
            self.assertTrue((rendered.generated_dir / "subscribers" / "ue1-subscriber.json").exists())
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            amf_payload = yaml.safe_load((rendered.config_dir / "amfcfg.yaml").read_text(encoding="utf-8"))
            gnb_payload = yaml.safe_load((rendered.config_dir / "gnb1-gnbcfg.yaml").read_text(encoding="utf-8"))
            ue_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))
            smf_text = (rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8")
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            upf_payload = yaml.safe_load((rendered.config_dir / "upfcfg.yaml").read_text(encoding="utf-8"))
            self.assertNotIn("n3iwue", compose_payload["services"])
            self.assertNotIn("free5gc-n3iwf", compose_payload["services"])
            self.assertIn("compose-up-core", [item["name"] for item in manifest["commands"]])
            self.assertIn("bootstrap-subscribers", [item["name"] for item in manifest["commands"]])
            self.assertIn("compose-up-ran", [item["name"] for item in manifest["commands"]])
            self.assertIn("writer-follow-free5gc", [item["name"] for item in manifest["commands"]])
            self.assertIn("writer-follow-ueransim", [item["name"] for item in manifest["commands"]])
            writer_command = next(item for item in manifest["commands"] if item["name"] == "writer-follow-ns3")
            self.assertIn("--graph-db-url", writer_command["argv"])
            self.assertIn(
                "postgresql://postgres:123456@localhost:5433/multiagents_db",
                writer_command["argv"],
            )
            bootstrap_command = next(
                item for item in manifest["commands"] if item["name"] == "bootstrap-subscribers"
            )
            self.assertIn("http://127.0.0.1:5000", bootstrap_command["argv"])
            self.assertIn(str(rendered.generated_dir / "subscribers" / "ue1-subscriber.json"), bootstrap_command["argv"])
            compose_up_core = next(item for item in manifest["commands"] if item["name"] == "compose-up-core")
            compose_up_ran = next(item for item in manifest["commands"] if item["name"] == "compose-up-ran")
            self.assertIn("-p", compose_up_core["argv"])
            self.assertIn("nrint", compose_up_core["argv"])
            self.assertIn("ueransim", compose_up_ran["argv"])
            self.assertIn("ue-ue1", compose_up_ran["argv"])
            self.assertEqual(manifest["free5gc_webui_url"], "http://127.0.0.1:5000")
            self.assertEqual(gnb_payload["ngapIp"], gnb_service_ip(1))
            self.assertEqual(gnb_payload["gtpIp"], gnb_service_ip(1))
            self.assertEqual(gnb_payload["amfConfigs"][0]["address"], AMF_CONTROL_IP)
            self.assertEqual(amf_payload["configuration"]["ngapIpList"], [AMF_CONTROL_IP])
            self.assertEqual(amf_payload["configuration"]["supportTaiList"][0]["tac"], "000001")
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
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["UPF"]["nodeID"],
                UPF_CONTROL_IP,
            )
            self.assertIn("sd: 010203", smf_text)
            self.assertIn("sd: 112233", smf_text)
            self.assertEqual(upf_payload["pfcp"]["addr"], UPF_CONTROL_IP)
            self.assertEqual(upf_payload["pfcp"]["nodeID"], UPF_CONTROL_IP)
            self.assertEqual(
                compose_payload["services"]["free5gc-smf"]["networks"]["privnet"]["ipv4_address"],
                SMF_CONTROL_IP,
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-amf"]["volumes"][0],
                f"{rendered.config_dir / 'amfcfg.yaml'}:/free5gc/config/amfcfg.yaml",
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-upf"]["networks"]["privnet"]["ipv4_address"],
                UPF_CONTROL_IP,
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-smf"]["volumes"][0],
                f"{rendered.config_dir / 'smfcfg.yaml'}:/free5gc/config/smfcfg.yaml",
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-upf"]["volumes"][0],
                f"{rendered.config_dir / 'upfcfg.yaml'}:/free5gc/config/upfcfg.yaml",
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_multi_ue_single_upf(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_multi_ue.yaml")
        run_id = generate_run_id("testmultiue")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            self.assertTrue((rendered.config_dir / "ue1-uecfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "ue2-uecfg.yaml").exists())
            self.assertTrue((rendered.generated_dir / "subscribers" / "ue1-subscriber.json").exists())
            self.assertTrue((rendered.generated_dir / "subscribers" / "ue2-subscriber.json").exists())

            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            ue2_payload = yaml.safe_load((rendered.config_dir / "ue2-uecfg.yaml").read_text(encoding="utf-8"))
            ue2_subscriber = json.loads(
                (rendered.generated_dir / "subscribers" / "ue2-subscriber.json").read_text(encoding="utf-8")
            )

            self.assertEqual(manifest["ran_services"], ["ueransim", "ue-ue1", "ue-ue2"])
            self.assertEqual(len(manifest["subscriber_payloads"]), 2)
            self.assertIn("ue-ue2", compose_payload["services"])
            self.assertEqual(ue2_payload["supi"], "imsi-208930000000002")
            self.assertEqual(ue2_payload["gnbSearchList"], [gnb_service_ip(1)])
            self.assertEqual(ue2_subscriber["ueId"], "imsi-208930000000002")

            compose_up_ran = next(item for item in manifest["commands"] if item["name"] == "compose-up-ran")
            self.assertIn("ue-ue1", compose_up_ran["argv"])
            self.assertIn("ue-ue2", compose_up_ran["argv"])
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl.yaml")
        run_id = generate_run_id("testulcl")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            self.assertTrue((rendered.config_dir / "smfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "i-upf-upfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "psa-upf-upfcfg.yaml").exists())

            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            i_upf_payload = yaml.safe_load((rendered.config_dir / "i-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            psa_upf_payload = yaml.safe_load((rendered.config_dir / "psa-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            ue_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))

            self.assertEqual(manifest["core_services"][:2], ["free5gc-i-upf", "free5gc-psa-upf"])
            self.assertEqual(manifest["ran_services"], ["ueransim", "ue-ue1"])
            writer_command = next(item for item in manifest["commands"] if item["name"] == "writer-follow-ns3")
            self.assertIn("postgresql://postgres:123456@localhost:5433/multiagents_db", writer_command["argv"])
            self.assertEqual(ue_payload["gnbSearchList"], [gnb_service_ip(1)])
            self.assertTrue(smf_payload["configuration"]["ulcl"])
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["I-UPF"]["nodeID"],
                upf_service_ip(1),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["PSA-UPF"]["nodeID"],
                upf_service_ip(2),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB1"]["nodeID"],
                gnb_service_ip(1),
            )
            self.assertEqual(i_upf_payload["pfcp"]["addr"], upf_service_ip(1))
            self.assertEqual(i_upf_payload["pfcp"]["nodeID"], upf_service_ip(1))
            self.assertEqual(psa_upf_payload["pfcp"]["addr"], upf_service_ip(2))
            self.assertEqual(psa_upf_payload["pfcp"]["nodeID"], upf_service_ip(2))
            self.assertEqual(
                compose_payload["services"]["free5gc-amf"]["networks"]["privnet"]["ipv4_address"],
                AMF_CONTROL_IP,
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-i-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(1),
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-psa-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(2),
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-i-upf"]["volumes"][0],
                f"{rendered.config_dir / 'i-upf-upfcfg.yaml'}:/free5gc/config/upfcfg.yaml",
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-psa-upf"]["volumes"][0],
                f"{rendered.config_dir / 'psa-upf-upfcfg.yaml'}:/free5gc/config/upfcfg.yaml",
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_multi_ue_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_ue.yaml")
        run_id = generate_run_id("testulclmultiue")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))

            self.assertEqual(manifest["ran_services"], ["ueransim", "ue-ue1", "ue-ue2"])
            self.assertEqual(len(manifest["subscriber_payloads"]), 2)
            self.assertIn("ue-ue2", compose_payload["services"])
            self.assertEqual(
                compose_payload["services"]["free5gc-i-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(1),
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-psa-upf"]["networks"]["privnet"]["ipv4_address"],
                upf_service_ip(2),
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_policy_and_graph_seeded_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "policy_graph_multi_gnb.yaml")
        run_id = generate_run_id("testpolicygraph")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            amf_payload = yaml.safe_load((rendered.config_dir / "amfcfg.yaml").read_text(encoding="utf-8"))
            ue1_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))
            ue2_payload = yaml.safe_load((rendered.config_dir / "ue2-uecfg.yaml").read_text(encoding="utf-8"))
            ue1_subscriber = json.loads(
                (rendered.generated_dir / "subscribers" / "ue1-subscriber.json").read_text(encoding="utf-8")
            )
            initial_topology = json.loads(
                (rendered.generated_dir / "initial-topology.json").read_text(encoding="utf-8")
            )

            self.assertEqual(
                manifest["ran_services"],
                ["ueransim", "ueransim-gnb2", "ue-ue1", "ue-ue2"],
            )
            self.assertIn("ueransim-gnb2", compose_payload["services"])
            self.assertEqual(ue1_payload["gnbSearchList"], [gnb_service_ip(2), gnb_service_ip(1)])
            self.assertEqual(ue2_payload["gnbSearchList"], [gnb_service_ip(1)])
            self.assertEqual(ue1_payload["key"], "8baf473f2f8fd09487cccbd7097c6862")
            self.assertEqual(ue1_payload["sessions"][0]["apn"], "internet")
            self.assertEqual(
                [item["tac"] for item in amf_payload["configuration"]["supportTaiList"]],
                ["000001", "000002"],
            )

            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")
            self.assertIn("--ue-gnb-map", ns3_run["argv"])
            self.assertIn("2,1", ns3_run["argv"])
            self.assertIn("--gnb-upf-map", ns3_run["argv"])
            self.assertIn("1,1", ns3_run["argv"])
            self.assertIn("--gnb-positions", ns3_run["argv"])
            self.assertIn("0.0:0.0:10.0;200.0:0.0:10.0", ns3_run["argv"])
            self.assertIn("--ue-positions", ns3_run["argv"])
            self.assertIn("190.0:0.0:1.5;10.0:0.0:1.5", ns3_run["argv"])

            self.assertEqual(initial_topology["ue_to_gnb"], {"ue1": "gnb2", "ue2": "gnb1"})
            self.assertEqual(initial_topology["gnb_to_upf"], {"gnb1": "upf", "gnb2": "upf"})
            self.assertEqual(
                ue1_subscriber["LocalPolicyData"]["free5gcRanPolicy"]["resolvedTargetGnb"],
                "gnb2",
            )
            self.assertEqual(
                ue1_subscriber["LocalPolicyData"]["free5gcRanPolicy"]["preferredGnbs"],
                ["gnb2", "gnb1"],
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()