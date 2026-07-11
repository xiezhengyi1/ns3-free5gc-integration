from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from bridge.common.scenario import ScenarioConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures" / "free5gc"


def with_free5gc_fixture(scenario: ScenarioConfig) -> ScenarioConfig:
    compose_name = "docker-compose-ulcl.yaml" if scenario.free5gc.mode == "ulcl" else "docker-compose.yaml"
    return replace(
        scenario,
        free5gc=replace(
            scenario.free5gc,
            compose_file=str(FIXTURE_ROOT / compose_name),
            config_root=str(FIXTURE_ROOT / "config"),
        ),
    )
