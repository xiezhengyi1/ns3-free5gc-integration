from __future__ import annotations

from dataclasses import replace
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
    def test_renders_inline_bridge_single_upf(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_single_upf.yaml")
        scenario = replace(
            scenario,
            bridge=replace(scenario.bridge, enable_inline_harness=True),
            ns3=replace(
                scenario.ns3,
                bridge_link_rate_mbps=250.0,
                bridge_link_delay_ms=2.0,
            ),
        )
        run_id = generate_run_id("testinline")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            gnb_payload = yaml.safe_load((rendered.config_dir / "gnb1-gnbcfg.yaml").read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            upf_payload = yaml.safe_load((rendered.config_dir / "upfcfg.yaml").read_text(encoding="utf-8"))
            bridge_script = rendered.bridge_script.read_text(encoding="utf-8")

            command_names = [item["name"] for item in manifest["commands"]]
            self.assertLess(command_names.index("compose-up-ran"), command_names.index("bridge-setup"))

            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")
            self.assertIn("--bridge-gnb-taps", ns3_run["argv"])
            self.assertIn("tgnb1", ns3_run["argv"])
            self.assertIn("--bridge-upf-taps", ns3_run["argv"])
            self.assertIn("tupf1", ns3_run["argv"])
            self.assertIn("--bridge-link-rate-mbps", ns3_run["argv"])
            self.assertIn("250.0", ns3_run["argv"])
            self.assertIn("--bridge-link-delay-ms", ns3_run["argv"])
            self.assertIn("2.0", ns3_run["argv"])

            self.assertEqual(gnb_payload["gtpIp"], gnb_service_ip(1))
            self.assertEqual(gnb_payload["linkIp"], gnb_service_ip(1))
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["UPF"]["interfaces"][0]["endpoints"],
                [UPF_CONTROL_IP],
            )
            self.assertEqual(upf_payload["gtpu"]["ifList"][0]["addr"], UPF_CONTROL_IP)
            self.assertEqual(manifest["bridge_links"][0]["upf_tap"], "tupf1")
            self.assertIn("docker inspect --format '{{ .State.Pid }}' upf", bridge_script)
            self.assertIn(f"ip route replace {UPF_CONTROL_IP}/32 dev esg1", bridge_script)
            self.assertIn(f"ip route replace {gnb_service_ip(1)}/32 dev esu1", bridge_script)
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

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
            self.assertTrue((rendered.config_dir / "nssfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "smfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "upfcfg.yaml").exists())
            self.assertTrue((rendered.generated_dir / "subscribers" / "ue1-subscriber.json").exists())
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            amf_payload = yaml.safe_load((rendered.config_dir / "amfcfg.yaml").read_text(encoding="utf-8"))
            nssf_payload = yaml.safe_load((rendered.config_dir / "nssfcfg.yaml").read_text(encoding="utf-8"))
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
            self.assertEqual(manifest["clock_file"], str(rendered.generated_dir / "ns3" / "ns3-clock.json"))
            free5gc_follow = next(item for item in manifest["commands"] if item["name"] == "writer-follow-free5gc")
            self.assertIn("--clock-file", free5gc_follow["argv"])
            self.assertIn(manifest["clock_file"], free5gc_follow["argv"])
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
            self.assertEqual(
                [item["tai"]["tac"] for item in nssf_payload["configuration"]["taList"]],
                ["000001"],
            )
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
            self.assertEqual(
                smf_payload["configuration"]["snssaiInfos"],
                [
                    {
                        "sNssai": {"sst": 1, "sd": "010203"},
                        "dnnInfos": [
                            {
                                "dnn": "internet",
                                "dns": {
                                    "ipv4": "8.8.8.8",
                                    "ipv6": "2001:4860:4860::8888",
                                },
                            }
                        ],
                    }
                ],
            )
            self.assertEqual(
                list(smf_payload["configuration"]["userplaneInformation"]["upNodes"].keys()),
                ["gNB1", "UPF"],
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["links"],
                [{"A": "gNB1", "B": "UPF"}],
            )
            self.assertEqual(compose_payload["services"]["ue-ue1"]["command"], "./nr-ue -c ./config/uecfg.yaml")
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
                compose_payload["services"]["free5gc-nssf"]["volumes"][0],
                f"{rendered.config_dir / 'nssfcfg.yaml'}:/free5gc/config/nssfcfg.yaml",
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
            self.assertEqual(compose_payload["services"]["ue-ue1"]["command"], "./nr-ue -c ./config/uecfg.yaml")
            self.assertEqual(
                compose_payload["services"]["ue-ue2"]["command"],
                "sh -c 'sleep 1 && ./nr-ue -c ./config/uecfg.yaml'",
            )
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
            self.assertTrue((rendered.config_dir / "uerouting.yaml").exists())
            self.assertTrue((rendered.config_dir / "i-upf-upfcfg.yaml").exists())
            self.assertTrue((rendered.config_dir / "psa-upf-upfcfg.yaml").exists())

            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            uerouting_payload = yaml.safe_load((rendered.config_dir / "uerouting.yaml").read_text(encoding="utf-8"))
            i_upf_payload = yaml.safe_load((rendered.config_dir / "i-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            psa_upf_payload = yaml.safe_load((rendered.config_dir / "psa-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            ue_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))

            self.assertEqual(manifest["core_services"][:2], ["free5gc-i-upf", "free5gc-psa-upf"])
            self.assertEqual(manifest["ran_services"], ["ueransim", "ue-ue1"])
            writer_command = next(item for item in manifest["commands"] if item["name"] == "writer-follow-ns3")
            self.assertIn("postgresql://postgres:123456@localhost:5433/multiagents_db", writer_command["argv"])
            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")
            self.assertIn("--simulator", ns3_run["argv"])
            self.assertIn("--clock-file", ns3_run["argv"])
            self.assertIn("--policy-reload-ms", ns3_run["argv"])
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
            self.assertIn(
                f"{rendered.config_dir / 'uerouting.yaml'}:/free5gc/config/uerouting.yaml",
                compose_payload["services"]["free5gc-smf"]["volumes"],
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue1"],
                {
                    "members": ["imsi-208930000000001"],
                    "topology": [
                        {"A": "gNB1", "B": "I-UPF"},
                        {"A": "I-UPF", "B": "PSA-UPF"},
                    ],
                    "specificPath": [],
                },
            )
            self.assertEqual(uerouting_payload["pfdDataForApp"], [])
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_multi_ue_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_ue.yaml")
        run_id = generate_run_id("testulclmultiue")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            uerouting_payload = yaml.safe_load((rendered.config_dir / "uerouting.yaml").read_text(encoding="utf-8"))

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
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue1"]["topology"],
                [
                    {"A": "gNB1", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue2"]["topology"],
                [
                    {"A": "gNB1", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_multi_gnb_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_gnb.yaml")
        run_id = generate_run_id("testulclmultignb")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            amf_payload = yaml.safe_load((rendered.config_dir / "amfcfg.yaml").read_text(encoding="utf-8"))
            uerouting_payload = yaml.safe_load((rendered.config_dir / "uerouting.yaml").read_text(encoding="utf-8"))
            ue1_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))
            ue2_payload = yaml.safe_load((rendered.config_dir / "ue2-uecfg.yaml").read_text(encoding="utf-8"))

            self.assertEqual(
                manifest["ran_services"],
                ["ueransim", "ueransim-gnb2", "ue-ue1", "ue-ue2"],
            )
            self.assertIn("ueransim-gnb2", compose_payload["services"])
            self.assertEqual(
                [item["tac"] for item in amf_payload["configuration"]["supportTaiList"]],
                ["000001", "000002"],
            )
            self.assertEqual(ue1_payload["gnbSearchList"], [gnb_service_ip(2), gnb_service_ip(1)])
            self.assertEqual(ue2_payload["gnbSearchList"], [gnb_service_ip(1)])
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB1"]["nodeID"],
                gnb_service_ip(1),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["gNB2"]["nodeID"],
                gnb_service_ip(2),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["I-UPF"]["nodeID"],
                upf_service_ip(1),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["upNodes"]["PSA-UPF"]["nodeID"],
                upf_service_ip(2),
            )
            self.assertEqual(
                smf_payload["configuration"]["userplaneInformation"]["links"],
                [
                    {"A": "gNB1", "B": "I-UPF"},
                    {"A": "gNB2", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )

            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")
            self.assertIn("--upf-names", ns3_run["argv"])
            self.assertIn("i-upf,psa-upf", ns3_run["argv"])
            self.assertIn("--gnb-upf-map", ns3_run["argv"])
            self.assertIn("1,1", ns3_run["argv"])
            self.assertIn(
                f"{rendered.config_dir / 'uerouting.yaml'}:/free5gc/config/uerouting.yaml",
                compose_payload["services"]["free5gc-smf"]["volumes"],
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue1"]["topology"],
                [
                    {"A": "gNB2", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue2"]["topology"],
                [
                    {"A": "gNB1", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_renders_ulcl_multi_slice_multi_gnb_topology(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_slice_multi_gnb.yaml")
        run_id = generate_run_id("testulclmultislice")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            manifest = json.loads((rendered.run_dir / "run-manifest.json").read_text(encoding="utf-8"))
            compose_payload = yaml.safe_load(rendered.compose_file.read_text(encoding="utf-8"))
            amf_payload = yaml.safe_load((rendered.config_dir / "amfcfg.yaml").read_text(encoding="utf-8"))
            nssf_payload = yaml.safe_load((rendered.config_dir / "nssfcfg.yaml").read_text(encoding="utf-8"))
            smf_payload = yaml.safe_load((rendered.config_dir / "smfcfg.yaml").read_text(encoding="utf-8"))
            uerouting_payload = yaml.safe_load((rendered.config_dir / "uerouting.yaml").read_text(encoding="utf-8"))
            i_upf_payload = yaml.safe_load((rendered.config_dir / "i-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            psa_upf_payload = yaml.safe_load((rendered.config_dir / "psa-upf-upfcfg.yaml").read_text(encoding="utf-8"))
            ue1_payload = yaml.safe_load((rendered.config_dir / "ue1-uecfg.yaml").read_text(encoding="utf-8"))
            gnb1_payload = yaml.safe_load((rendered.config_dir / "gnb1-gnbcfg.yaml").read_text(encoding="utf-8"))
            gnb2_payload = yaml.safe_load((rendered.config_dir / "gnb2-gnbcfg.yaml").read_text(encoding="utf-8"))
            flow_profiles = (rendered.generated_dir / "ns3-flow-profiles.tsv").read_text(encoding="utf-8")

            self.assertEqual(
                [item["sd"] for item in amf_payload["configuration"]["plmnSupportList"][0]["snssaiList"]],
                ["010203", "112233"],
            )
            self.assertEqual(
                [item["tai"]["tac"] for item in nssf_payload["configuration"]["taList"]],
                ["000001", "000002"],
            )
            self.assertEqual(
                [item["sd"] for item in nssf_payload["configuration"]["supportedNssaiInPlmnList"][0]["supportedSnssaiList"]],
                ["010203", "112233"],
            )
            self.assertEqual(
                [item["snssai"]["sd"] for item in nssf_payload["configuration"]["nsiList"]],
                ["010203", "112233"],
            )
            self.assertEqual(
                [item["sd"] for item in gnb1_payload["slices"]],
                ["0x010203", "0x112233"],
            )
            self.assertEqual(
                [item["sd"] for item in gnb2_payload["slices"]],
                ["0x010203", "0x112233"],
            )
            self.assertEqual(len(ue1_payload["sessions"]), 2)
            self.assertEqual(
                [item["apn"] for item in ue1_payload["sessions"]],
                ["internet", "enterprise"],
            )
            self.assertEqual(
                [item["sNssai"]["sd"] for item in smf_payload["configuration"]["snssaiInfos"]],
                ["010203", "112233"],
            )
            self.assertEqual(
                smf_payload["configuration"]["snssaiInfos"][0]["dnnInfos"],
                [{"dnn": "internet", "dns": {"ipv4": "8.8.8.8", "ipv6": "2001:4860:4860::8888"}}],
            )
            self.assertEqual(
                smf_payload["configuration"]["snssaiInfos"][1]["dnnInfos"],
                [{"dnn": "enterprise", "dns": {"ipv4": "8.8.8.8", "ipv6": "2001:4860:4860::8888"}}],
            )
            self.assertEqual(
                [item["sNssai"]["sd"] for item in smf_payload["configuration"]["userplaneInformation"]["upNodes"]["PSA-UPF"]["sNssaiUpfInfos"]],
                ["010203", "112233"],
            )
            self.assertEqual(
                [item["dnn"] for item in i_upf_payload["dnnList"]],
                ["internet", "enterprise"],
            )
            self.assertEqual(
                [item["dnn"] for item in psa_upf_payload["dnnList"]],
                ["internet", "enterprise"],
            )
            self.assertIn("session_ref", flow_profiles.splitlines()[0].split("\t"))
            self.assertIn("ue1-video-session", flow_profiles)
            self.assertIn("ue1-control-session", flow_profiles)

            ns3_run = next(item for item in manifest["commands"] if item["name"] == "ns3-run")
            self.assertIn("--ue-gnb-map", ns3_run["argv"])
            self.assertIn("2,1", ns3_run["argv"])
            self.assertIn(
                f"{rendered.config_dir / 'uerouting.yaml'}:/free5gc/config/uerouting.yaml",
                compose_payload["services"]["free5gc-smf"]["volumes"],
            )
            self.assertEqual(
                compose_payload["services"]["free5gc-nssf"]["volumes"][0],
                f"{rendered.config_dir / 'nssfcfg.yaml'}:/free5gc/config/nssfcfg.yaml",
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue1"]["topology"],
                [
                    {"A": "gNB2", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
            self.assertEqual(
                uerouting_payload["ueRoutingInfo"]["ue2"]["topology"],
                [
                    {"A": "gNB1", "B": "I-UPF"},
                    {"A": "I-UPF", "B": "PSA-UPF"},
                ],
            )
            self.assertEqual(uerouting_payload["pfdDataForApp"], [])
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