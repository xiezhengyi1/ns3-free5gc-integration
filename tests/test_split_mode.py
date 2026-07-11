from __future__ import annotations

from dataclasses import replace
import shutil
import tempfile
import unittest
from pathlib import Path

import yaml

from bridge.split_mode.config import load_split_mode_config
from bridge.split_mode.renderer import render_split_run
from bridge.split_mode.session_gate import _apply_event, _build_state_maps, _rewrite_flow_profile
from tests.free5gc_fixture import with_free5gc_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPLIT_SCENARIOS = (
    PROJECT_ROOT / "scenarios" / "split_mode" / "s1_basic_single_slice.yaml",
    PROJECT_ROOT / "scenarios" / "split_mode" / "s2_medium_complexity.yaml",
    PROJECT_ROOT / "scenarios" / "split_mode" / "s3_high_complexity.yaml",
)


def _load_split_mode_config_with_fixture(path: Path):
    config = load_split_mode_config(path)
    return replace(config, base_scenario=with_free5gc_fixture(config.base_scenario))


class SplitModeConfigTest(unittest.TestCase):
    def test_rejects_synthetic_n3_fields(self) -> None:
        temp_dir = Path(tempfile.mkdtemp(prefix="splitcfg"))
        try:
            scenario_path = temp_dir / "invalid.yaml"
            scenario_path.write_text(
                "name: invalid\nscenario_id: invalid\nbase_scenario: ../scenarios/s2_medium_complexity.yaml\nbridge:\n  enable_inline_harness: true\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "synthetic N3 fields"):
                load_split_mode_config(scenario_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_manifest_excludes_inline_bridge_commands(self) -> None:
        for scenario_path in SPLIT_SCENARIOS:
            with self.subTest(scenario=scenario_path.name):
                config = _load_split_mode_config_with_fixture(scenario_path)
                rendered = render_split_run(PROJECT_ROOT, config, run_id=f"split-render-{scenario_path.stem}")
                try:
                    manifest = rendered.manifest.to_dict()
                    names = [item["name"] for item in manifest["commands"]]
                    self.assertNotIn("bridge-setup", names)
                    self.assertNotIn("real-ue-flows", names)
                    self.assertIn("split-gate", names)
                    self.assertIn("writer-follow-split-ns3", names)
                    self.assertIn("ns3-build", names)
                    self.assertIn("wait-for-pfcp-ready", names)
                    self.assertLess(names.index("ns3-build"), names.index("ns3-run"))
                    self.assertLess(names.index("compose-up-smf"), names.index("wait-for-pfcp-ready"))
                    self.assertLess(names.index("wait-for-pfcp-ready"), names.index("compose-up-ue"))
                    compose_up_ue = next(item for item in manifest["commands"] if item["name"] == "compose-up-ue")
                    self.assertIn("bridge.split_mode.ue_startup", compose_up_ue["argv"])
                finally:
                    shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_rendered_smf_uses_upf_control_ip_for_pfcp(self) -> None:
        config = _load_split_mode_config_with_fixture(PROJECT_ROOT / "scenarios" / "split_mode" / "s1_basic_single_slice.yaml")
        rendered = render_split_run(PROJECT_ROOT, config, run_id="split-render-smf-control-ip")
        try:
            smf_payload = yaml.safe_load(
                (rendered.run_dir / "generated" / "config" / "smfcfg.yaml").read_text(encoding="utf-8")
            )
            upf_payload = yaml.safe_load(
                (rendered.run_dir / "generated" / "config" / "upfcfg.yaml").read_text(encoding="utf-8")
            )
            upf_node = smf_payload["configuration"]["userplaneInformation"]["upNodes"]["UPF"]
            upf_pfcp_addr = upf_payload["pfcp"]["addr"]
            upf_n3_addr = upf_payload["gtpu"]["ifList"][0]["addr"]
            self.assertEqual(upf_node["nodeID"], upf_pfcp_addr)
            self.assertEqual(upf_node["addr"], upf_pfcp_addr)
            self.assertEqual(upf_node["interfaces"][0]["endpoints"], [upf_n3_addr])
            self.assertNotEqual(upf_pfcp_addr, upf_n3_addr)
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_loads_all_split_scenarios(self) -> None:
        for scenario_path in SPLIT_SCENARIOS:
            with self.subTest(scenario=scenario_path.name):
                config = _load_split_mode_config_with_fixture(scenario_path)
                self.assertTrue(config.scenario_id.endswith("-split"))
                self.assertEqual(config.ns3.scratch_name, "nr_multignb_multiupf_split")
                self.assertEqual(config.radio.scheduler_type, "pf")
                self.assertEqual(config.radio.resolved_tdd_pattern(), "DL|UL|UL|F|DL|UL|UL|F|")
                self.assertFalse(config.control_plane_scenario.bridge.enable_inline_harness)
                self.assertEqual(
                    config.control_plane_scenario.bridge.n3_network_cidr,
                    config.base_scenario.bridge.n3_network_cidr,
                )

    def test_renderer_passes_explicit_radio_arguments(self) -> None:
        config = _load_split_mode_config_with_fixture(PROJECT_ROOT / "scenarios" / "split_mode" / "s1_basic_single_slice.yaml")
        rendered = render_split_run(PROJECT_ROOT, config, run_id="split-render-radio-args")
        try:
            ns3_run = next(item for item in rendered.manifest.commands if item.name == "ns3-run")
            self.assertIn("--scheduler-type", ns3_run.argv)
            self.assertIn("pf", ns3_run.argv)
            self.assertIn("--tdd-pattern", ns3_run.argv)
            self.assertIn("DL|UL|UL|F|DL|UL|UL|F|", ns3_run.argv)
            self.assertIn("--ue-tx-power-db", ns3_run.argv)
            self.assertIn("23.0", ns3_run.argv)
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)


class SplitModeGateTest(unittest.TestCase):
    def test_gate_activates_and_deactivates_sessions(self) -> None:
        config = _load_split_mode_config_with_fixture(PROJECT_ROOT / "scenarios" / "split_mode" / "s2_medium_complexity.yaml")
        scenario = config.control_plane_scenario
        session_by_ref, supi_to_ue, psi_map = _build_state_maps(scenario)
        first_ue = scenario.ues[0]
        first_session = first_ue.sessions[0]

        changed = _apply_event(
            "ueransim.registration_success",
            {"service": "ue-ue1"},
            first_ue.name,
            1,
            session_by_ref,
            supi_to_ue,
            psi_map,
        )
        self.assertTrue(changed)
        self.assertTrue(session_by_ref[first_session.session_ref].registered)
        self.assertFalse(session_by_ref[first_session.session_ref].active)

        changed = _apply_event(
            "ueransim.pdu_session_established",
            {"psi": 1},
            f"{first_ue.name}:psi-1",
            2,
            session_by_ref,
            supi_to_ue,
            psi_map,
        )
        self.assertTrue(changed)
        self.assertTrue(session_by_ref[first_session.session_ref].active)

        changed = _apply_event(
            "ueransim.registration_failure",
            {"service": "ue-ue1"},
            first_ue.name,
            3,
            session_by_ref,
            supi_to_ue,
            psi_map,
        )
        self.assertTrue(changed)
        self.assertFalse(session_by_ref[first_session.session_ref].active)

    def test_rewrites_enabled_column(self) -> None:
        temp_dir = Path(tempfile.mkdtemp(prefix="splitgate"))
        try:
            config = _load_split_mode_config_with_fixture(PROJECT_ROOT / "scenarios" / "split_mode" / "s2_medium_complexity.yaml")
            rendered = render_split_run(PROJECT_ROOT, config, run_id="split-gate-test")
            session_by_ref, _, _ = _build_state_maps(config.control_plane_scenario)
            first_session = config.control_plane_scenario.ues[0].sessions[0].session_ref
            session_by_ref[first_session].active = True
            _rewrite_flow_profile(Path(rendered.manifest.flow_profile_file), session_by_ref)
            lines = Path(rendered.manifest.flow_profile_file).read_text(encoding="utf-8").splitlines()
            header = lines[0].split("\t")
            enabled_index = header.index("enabled")
            session_index = header.index("session_ref")
            target = next(line.split("\t") for line in lines[1:] if line.split("\t")[session_index] == first_session)
            self.assertEqual(target[enabled_index], "true")
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)
            shutil.rmtree(temp_dir, ignore_errors=True)
