"""Render scenario-driven configs and run artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from adapters.free5gc_ueransim.bridge_setup import build_bridge_plan, render_bridge_script
from adapters.free5gc_ueransim.compose_override import render_compose_for_run
from bridge.common.scenario import ScenarioConfig, SliceConfig, load_scenario
from bridge.orchestrator.process_plan import RunManifest, build_run_manifest


def _yaml_load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected YAML mapping at {path}")
    return payload


def _yaml_dump(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def _format_slice_hex(slice_config: SliceConfig) -> str:
    return f"0x{slice_config.sd.lower()}"


def _resolve_output_path(run_dir: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else run_dir / candidate


@dataclass(slots=True)
class RenderedRun:
    run_id: str
    project_root: Path
    run_dir: Path
    generated_dir: Path
    config_dir: Path
    compose_file: Path
    bridge_script: Path
    manifest: RunManifest


def _inline_gnb_ip_map(scenario: ScenarioConfig) -> dict[str, str]:
    if not scenario.bridge.enable_inline_harness:
        return {}
    grouped = scenario.ue_groups()
    overloaded = [gnb for gnb, ues in grouped.items() if len(ues) > 1]
    if overloaded:
        joined = ", ".join(overloaded)
        raise ValueError(
            "inline harness currently supports at most one UE per gNB; "
            f"overloaded gNBs: {joined}"
        )
    return {gnb_name: f"10.210.{index}.1" for index, gnb_name in enumerate(grouped, start=1)}


def _render_gnb_configs(scenario: ScenarioConfig, config_dir: Path) -> None:
    slice_map = scenario.slice_map()
    inline_map = _inline_gnb_ip_map(scenario)
    base_cfg = _yaml_load(Path(scenario.free5gc.config_root) / "gnbcfg.yaml")

    for gnb in scenario.gnbs:
        payload = dict(base_cfg)
        payload["linkIp"] = inline_map.get(gnb.name, gnb.alias)
        payload["ngapIp"] = gnb.alias
        payload["gtpIp"] = gnb.alias
        payload["tac"] = gnb.tac
        payload["nci"] = gnb.nci
        payload["slices"] = [
            {"sst": slice_map[slice_ref].sst, "sd": _format_slice_hex(slice_map[slice_ref])}
            for slice_ref in gnb.slices
        ]
        _yaml_dump(config_dir / f"{gnb.name}-gnbcfg.yaml", payload)


def _render_ue_configs(scenario: ScenarioConfig, config_dir: Path) -> None:
    slice_map = scenario.slice_map()
    gnb_map = scenario.gnb_map()
    inline_map = _inline_gnb_ip_map(scenario)

    base_name = "uecfg-ulcl.yaml" if scenario.free5gc.mode == "ulcl" else "uecfg.yaml"
    base_cfg = _yaml_load(Path(scenario.free5gc.config_root) / base_name)

    for ue in scenario.ues:
        payload = dict(base_cfg)
        payload["supi"] = ue.supi
        payload["key"] = ue.key
        payload["op"] = ue.op
        payload["opType"] = ue.op_type
        payload["amf"] = ue.amf
        payload["gnbSearchList"] = [inline_map.get(ue.gnb, gnb_map[ue.gnb].alias)]
        payload["sessions"] = [
            {
                "type": session.session_type,
                "apn": session.apn,
                "slice": {
                    "sst": slice_map[session.slice_ref].sst,
                    "sd": _format_slice_hex(slice_map[session.slice_ref]),
                },
            }
            for session in ue.sessions
        ]
        payload["configured-nssai"] = [
            {
                "sst": slice_map[session.slice_ref].sst,
                "sd": _format_slice_hex(slice_map[session.slice_ref]),
            }
            for session in ue.sessions
        ]
        first_slice = slice_map[ue.sessions[0].slice_ref]
        payload["default-nssai"] = [
            {"sst": first_slice.sst, "sd": int(first_slice.sd, 16)}
        ]
        _yaml_dump(config_dir / f"{ue.name}-uecfg.yaml", payload)


def render_run_assets(
    project_root: Path,
    scenario: ScenarioConfig,
    run_id: str,
) -> RenderedRun:
    run_dir = project_root / "artifacts" / "runs" / run_id
    generated_dir = run_dir / "generated"
    config_dir = generated_dir / "config"
    ns3_dir = generated_dir / scenario.ns3.output_subdir
    state_dir = run_dir / "state"
    archive_dir = _resolve_output_path(run_dir, scenario.writer.archive_dir)
    state_db = _resolve_output_path(run_dir, scenario.writer.state_db)

    for path in (config_dir, ns3_dir, state_dir, archive_dir, state_db.parent):
        path.mkdir(parents=True, exist_ok=True)

    _render_gnb_configs(scenario, config_dir)
    _render_ue_configs(scenario, config_dir)

    compose_payload, service_map = render_compose_for_run(scenario, config_dir)
    compose_file = generated_dir / "free5gc-compose.generated.yaml"
    _yaml_dump(compose_file, compose_payload)

    bridge_plans = build_bridge_plan(scenario, service_map)
    bridge_script = generated_dir / "setup-inline-bridge.sh"
    render_bridge_script(bridge_plans, bridge_script)

    snapshot_file = ns3_dir / "tick-snapshots.jsonl"
    manifest = build_run_manifest(
        project_root=project_root,
        scenario=scenario,
        run_id=run_id,
        run_dir=run_dir,
        compose_file=compose_file,
        bridge_script=bridge_script,
        snapshot_file=snapshot_file,
        state_db=state_db,
        archive_dir=archive_dir,
        service_map=service_map,
    )

    manifest_path = run_dir / "run-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (run_dir / "resolved-scenario.json").write_text(
        json.dumps(
            {
                "name": scenario.name,
                "scenario_id": scenario.scenario_id,
                "tick_ms": scenario.tick_ms,
                "seed": scenario.seed,
                "gnbs": [gnb.name for gnb in scenario.gnbs],
                "ues": [ue.name for ue in scenario.ues],
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    return RenderedRun(
        run_id=run_id,
        project_root=project_root,
        run_dir=run_dir,
        generated_dir=generated_dir,
        config_dir=config_dir,
        compose_file=compose_file,
        bridge_script=bridge_script,
        manifest=manifest,
    )


def render_run_from_scenario_file(
    project_root: Path,
    scenario_file: Path,
    run_id: str,
) -> RenderedRun:
    scenario = load_scenario(scenario_file)
    return render_run_assets(project_root, scenario, run_id)