from __future__ import annotations

import shutil
import unittest
from pathlib import Path

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
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()