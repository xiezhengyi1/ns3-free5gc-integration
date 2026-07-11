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
        main_source = source[source.index("int\nmain(") :]
        declarations = main_source[: main_source.index("CommandLine cmd")]
        for declaration in (
            "std::string userPlaneGateSocket;",
            "std::string bearerMapFile;",
            "uint32_t rngSeed = 1;",
            "uint64_t rngRun = 1;",
            "uint64_t virtualEpochUs = 100000;",
            "double channelUpdateMs = 10.0;",
            "bool shadowingEnabled = true;",
        ):
            self.assertIn(declaration, declarations)
        self.assertIn("LoadBearerMapOverrides", source)
        self.assertIn("ApplyBearerMapOverrides", source)
        self.assertIn("PacketErrorRateBased", source)
        self.assertIn("ResolveShadowBearer", source)
        self.assertIn("profile.fiveQi", source)
        self.assertIn("request.qfi", source)
        self.assertIn("request.enqueueNs3Us", source)
        self.assertIn("m_flowId", source)
        self.assertIn("m_enqueueNs3Us", source)
        self.assertIn("NrRlc/TxDrop", source)
        self.assertIn('"rlc-tx-drop"', source)
        self.assertIn('"nr-device-drop"', source)
        self.assertIn('NS_FATAL_ERROR("gated user plane requires bearerMapFile"', source)
        self.assertIn("activeUpfTaps", source)
        self.assertIn("inline N3 bridge requires at least one UPF TAP", source)
        self.assertIn("segment.Create(gNbNum", source)
        self.assertIn('GetColumnValue(headerIndex, columns, "upf_ref")', source)
        self.assertIn("profile->upfName", source)
        self.assertIn("Simulator::Now().GetMicroSeconds()", peer)
        self.assertIn("PACKET_DELIVER", peer)
        self.assertIn("PACKET_DROP", peer)
        self.assertIn("AUTHORIZE_SEND", peer)
        self.assertIn("EPOCH_START", peer)
        self.assertNotIn("ComputeMetricFactor", source)
        self.assertNotIn("ClampMetricFactor", source)
        self.assertIn("SOCK_NONBLOCK", peer)
        self.assertIn("std::chrono::steady_clock", peer)
        self.assertIn("duplicate packet_id", peer)
        self.assertIn("unknown shadow flow", peer)
        self.assertNotIn("usleep(", peer)
        self.assertNotIn("poll(&descriptor", peer)

    def test_build_scripts_copy_shadow_peer_header(self) -> None:
        for name in ("build_ns3_twin.sh", "run_ns3_twin.sh"):
            script = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
            self.assertIn("gtpu_shadow_peer.h", script)


if __name__ == "__main__":
    unittest.main()
