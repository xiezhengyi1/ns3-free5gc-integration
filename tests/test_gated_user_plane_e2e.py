from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_gate_loopback.py"
SPEC = importlib.util.spec_from_file_location("run_gate_loopback", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class GatedUserPlaneE2eTest(unittest.TestCase):
    def test_deterministic_delivery_drop_and_kpi(self) -> None:
        result = MODULE.run_demo()

        self.assertEqual(result["released_frames"], ["managed-one"])
        self.assertEqual(result["stats"]["captured"], 2)
        self.assertEqual(result["stats"]["released"], 1)
        self.assertEqual(result["stats"]["dropped"], 1)
        self.assertEqual(result["kpi"]["submitted_packets"], 2)
        self.assertEqual(result["kpi"]["delivered_packets"], 1)
        self.assertEqual(result["kpi"]["dropped_packets"], 1)
        self.assertEqual(result["kpi"]["delay_p50_us"], 200)
        self.assertEqual(result["pending_packets"], 0)


if __name__ == "__main__":
    unittest.main()
