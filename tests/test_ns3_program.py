from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.build_ns3_program import prepare_ns3_program


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Ns3ProgramBuildHelperTest(unittest.TestCase):
    def test_native_multi_n3_smoke_checks_per_flow_upf_selection(self) -> None:
        source = (
            PROJECT_ROOT / "scripts" / "run_native_multi_n3_smoke.py"
        ).read_text(encoding="utf-8")

        self.assertIn("--gNbNum=2", source)
        self.assertIn("--upfNames=upf-a,upf-b", source)
        self.assertIn('"upf_ref": "upf-b"', source)
        self.assertIn("unexpected per-flow UPF selection", source)

    def test_native_gate_smoke_script_drives_main_shadow_bearer(self) -> None:
        source = (
            PROJECT_ROOT / "scripts" / "run_native_gate_smoke.py"
        ).read_text(encoding="utf-8")

        self.assertIn("scratch/nr_multignb_multiupf", source)
        self.assertIn("MessageType.PACKET_ENQUEUE", source)
        self.assertIn("MessageType.TICK_COMPLETE", source)
        self.assertIn("completed_epochs", source)

    def test_shadow_peer_smoke_uses_ns3_compatible_named_callback(self) -> None:
        source = (
            PROJECT_ROOT / "sim" / "ns3" / "gtpu_shadow_peer_test.cc"
        ).read_text(encoding="utf-8")

        self.assertIn("void\nOnShadowPacketRequest", source)
        self.assertIn("MakeBoundCallback(&OnShadowPacketRequest", source)
        self.assertNotIn("MakeCallback([]", source)
        self.assertIn("SendFragmented", source)
        self.assertIn("PACKET_DELIVER", source)
        self.assertIn("PACKET_DROP", source)
        self.assertIn("TICK_COMPLETE", source)
        self.assertIn("requests.size() != 2", source)

    def test_prepare_copies_shadow_peer_sources_and_returns_build_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ns3_root = Path(tmp)
            result = prepare_ns3_program(
                project_root=PROJECT_ROOT,
                ns3_root=ns3_root,
                run_build=False,
            )

            scratch = ns3_root / "scratch"
            self.assertTrue((scratch / "nr_multignb_multiupf.cc").exists())
            self.assertTrue((scratch / "gtpu_shadow_peer.h").exists())
            native_test = scratch / "gtpu_shadow_peer_test"
            self.assertTrue((native_test / "gtpu_shadow_peer.h").exists())
            self.assertTrue((native_test / "gtpu_shadow_peer.cc").exists())
            self.assertTrue((native_test / "gtpu_shadow_peer_test.cc").exists())
            self.assertFalse((scratch / "gtpu_shadow_peer_test.cc").exists())
            self.assertEqual(result.ns3_root, ns3_root.resolve())
            self.assertEqual(
                result.build_command,
                [str(ns3_root.resolve() / "ns3"), "build"],
            )

    def test_cli_dry_run_accepts_scenario_and_prints_copied_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ns3_root = Path(tmp)
            scenario = PROJECT_ROOT / "scenarios" / "free5gc_ueransim_gtpu_nr.yaml"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PROJECT_ROOT / "scripts" / "build_ns3_program.py"),
                    "--project-root",
                    str(PROJECT_ROOT),
                    "--ns3-root",
                    str(ns3_root),
                    "--scenario",
                    str(scenario),
                    "--dry-run",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("copied=", result.stdout)
            self.assertIn("nr_multignb_multiupf.cc", result.stdout)
            self.assertIn("gtpu_shadow_peer.h", result.stdout)
            self.assertIn("gtpu_shadow_peer.cc", result.stdout)
            self.assertIn("gtpu_shadow_peer_test.cc", result.stdout)


if __name__ == "__main__":
    unittest.main()
