from __future__ import annotations

import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Ns3ShadowPeerSourceTest(unittest.TestCase):
    def test_run_script_forwards_gate_and_reproducibility_arguments(self) -> None:
        script = (PROJECT_ROOT / "scripts" / "run_ns3_twin.sh").read_text(
            encoding="utf-8"
        )

        for option in (
            "--user-plane-gate-socket",
            "--bearer-map-file",
            "--rng-seed",
            "--rng-run",
            "--virtual-epoch-us",
            "--channel-update-ms",
            "--shadowing-enabled",
        ):
            self.assertIn(option, script)

    def test_ns3_program_uses_shadow_peer_and_virtual_timestamps(self) -> None:
        source = (
            PROJECT_ROOT / "sim" / "ns3" / "nr_multignb_multiupf.cc"
        ).read_text(encoding="utf-8")
        peer = (PROJECT_ROOT / "sim" / "ns3" / "gtpu_shadow_peer.h").read_text(
            encoding="utf-8"
        )

        self.assertIn('#include "gtpu_shadow_peer.h"', source)
        self.assertIn("RngSeedManager::SetSeed", source)
        self.assertIn("RngSeedManager::SetRun", source)
        self.assertIn('cmd.AddValue("userPlaneGateSocket"', source)
        self.assertIn("Simulator::Now().GetMicroSeconds()", peer)
        self.assertIn("PACKET_DELIVER", peer)
        self.assertIn("PACKET_DROP", peer)
        self.assertIn("AUTHORIZE_SEND", peer)
        self.assertIn("EPOCH_START", peer)

    def test_build_scripts_copy_shadow_peer_header(self) -> None:
        for name in ("build_ns3_twin.sh", "run_ns3_twin.sh"):
            script = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
            self.assertIn("gtpu_shadow_peer.h", script)


if __name__ == "__main__":
    unittest.main()
