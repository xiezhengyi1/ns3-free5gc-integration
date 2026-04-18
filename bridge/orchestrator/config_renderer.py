"""Render scenario-driven configs and run artifacts."""

from __future__ import annotations

from copy import deepcopy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from adapters.free5gc_ueransim.bridge_setup import build_bridge_plan, render_bridge_script
from adapters.free5gc_ueransim.compose_override import (
    AMF_CONTROL_IP,
    SMF_CONTROL_IP,
    UPF_CONTROL_IP,
    gnb_service_ip,
    render_compose_for_run,
    upf_service_ip,
)
from adapters.free5gc_ueransim.subscriber_bootstrap import render_subscriber_bootstrap_assets
from bridge.common.scenario import ScenarioConfig, SliceConfig, load_scenario
from bridge.common.topology import ResolvedScenarioTopology, resolve_scenario_topology
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


def _rewrite_config_text(template_text: str, replacements: dict[str, str]) -> str:
    updated = template_text
    for source, target in replacements.items():
        updated = updated.replace(source, target)
    return updated


def _render_amf_config(scenario: ScenarioConfig, config_dir: Path) -> None:
    payload = _yaml_load(Path(scenario.free5gc.config_root) / "amfcfg.yaml")
    configuration = payload.get("configuration")
    if not isinstance(configuration, dict):
        raise ValueError("amfcfg.yaml must define configuration")

    configuration["ngapIpList"] = [AMF_CONTROL_IP]

    sbi = configuration.get("sbi")
    if isinstance(sbi, dict):
        sbi["registerIPv4"] = AMF_CONTROL_IP
        sbi["bindingIPv4"] = AMF_CONTROL_IP

    metrics = configuration.get("metrics")
    if isinstance(metrics, dict):
        metrics["bindingIPv4"] = AMF_CONTROL_IP

    served_guamis = configuration.get("servedGuamiList")
    if not isinstance(served_guamis, list) or not served_guamis:
        raise ValueError("amfcfg.yaml must define servedGuamiList")
    first_guami = served_guamis[0]
    if not isinstance(first_guami, dict):
        raise ValueError("amfcfg.yaml servedGuamiList entries must be mappings")
    plmn = first_guami.get("plmnId")
    if not isinstance(plmn, dict):
        raise ValueError("amfcfg.yaml servedGuamiList must define plmnId")
    mcc = str(plmn["mcc"])
    mnc = str(plmn["mnc"])

    support_tai_list: list[dict[str, object]] = []
    for gnb in scenario.gnbs:
        support_tai_list.append(
            {
                "plmnId": {
                    "mcc": mcc,
                    "mnc": mnc,
                },
                "tac": f"{gnb.tac:06x}",
            }
        )
    configuration["supportTaiList"] = support_tai_list

    plmn_support_list = configuration.get("plmnSupportList")
    if isinstance(plmn_support_list, list) and plmn_support_list:
        first_entry = plmn_support_list[0]
        if isinstance(first_entry, dict):
            first_entry["plmnId"] = {
                "mcc": mcc,
                "mnc": mnc,
            }
            first_entry["snssaiList"] = [
                {
                    "sst": slice_config.sst,
                    "sd": slice_config.sd.lower(),
                }
                for slice_config in scenario.slices
            ]
            configuration["plmnSupportList"] = [first_entry]

    configuration["supportDnnList"] = sorted(
        {
            session.apn
            for ue in scenario.ues
            for session in ue.sessions
        }
        | {upf.dnn for upf in scenario.upfs}
    )

    _yaml_dump(config_dir / "amfcfg.yaml", payload)


def _render_core_configs(scenario: ScenarioConfig, config_dir: Path) -> None:
    base_root = Path(scenario.free5gc.config_root)
    _render_amf_config(scenario, config_dir)

    if scenario.free5gc.mode == "ulcl":
        ulcl_root = base_root / "ULCL"
        replacements = {
            "smf.free5gc.org": SMF_CONTROL_IP,
            "gnb.free5gc.org": gnb_service_ip(1),
        }
        upf_ip_map = {
            upf.name: upf_service_ip(index)
            for index, upf in enumerate(scenario.upfs, start=1)
        }
        replacements.update(
            {
                f"{upf_name}.free5gc.org": upf_ip
                for upf_name, upf_ip in upf_ip_map.items()
            }
        )

        smf_text = _rewrite_config_text(
            (ulcl_root / "smfcfg.yaml").read_text(encoding="utf-8"),
            replacements,
        )
        (config_dir / "smfcfg.yaml").write_text(smf_text, encoding="utf-8")

        for upf in scenario.upfs:
            template_path = ulcl_root / f"upfcfg-{upf.name}.yaml"
            upf_text = _rewrite_config_text(
                template_path.read_text(encoding="utf-8"),
                replacements,
            )
            (config_dir / f"{upf.name}-upfcfg.yaml").write_text(upf_text, encoding="utf-8")
        return

    smf_text = (base_root / "smfcfg.yaml").read_text(encoding="utf-8")
    smf_text = smf_text.replace("smf.free5gc.org", SMF_CONTROL_IP)
    smf_text = smf_text.replace("upf.free5gc.org", UPF_CONTROL_IP)
    (config_dir / "smfcfg.yaml").write_text(smf_text, encoding="utf-8")

    upf_text = (base_root / "upfcfg.yaml").read_text(encoding="utf-8")
    upf_text = upf_text.replace("upf.free5gc.org", UPF_CONTROL_IP)
    (config_dir / "upfcfg.yaml").write_text(upf_text, encoding="utf-8")


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


def _inline_gnb_ip_map(
    scenario: ScenarioConfig,
    resolved_topology: ResolvedScenarioTopology,
) -> dict[str, str]:
    if not scenario.bridge.enable_inline_harness:
        return {}
    grouped: dict[str, list[str]] = {gnb.name: [] for gnb in scenario.gnbs}
    for ue in scenario.ues:
        grouped.setdefault(resolved_topology.ue_to_gnb[ue.name], []).append(ue.name)
    overloaded = [gnb for gnb, ues in grouped.items() if len(ues) > 1]
    if overloaded:
        joined = ", ".join(overloaded)
        raise ValueError(
            "inline harness currently supports at most one UE per gNB; "
            f"overloaded gNBs: {joined}"
        )
    return {gnb_name: f"10.210.{index}.1" for index, gnb_name in enumerate(grouped, start=1)}


def _render_gnb_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    slice_map = scenario.slice_map()
    inline_map = _inline_gnb_ip_map(scenario, resolved_topology)
    base_cfg = _yaml_load(Path(scenario.free5gc.config_root) / "gnbcfg.yaml")

    for index, gnb in enumerate(scenario.gnbs, start=1):
        payload = deepcopy(base_cfg)
        payload["linkIp"] = inline_map.get(gnb.name, gnb_service_ip(index))
        payload["ngapIp"] = gnb_service_ip(index)
        payload["gtpIp"] = gnb_service_ip(index)
        payload["tac"] = gnb.tac
        payload["nci"] = gnb.nci
        amf_configs = payload.get("amfConfigs")
        if isinstance(amf_configs, list):
            payload["amfConfigs"] = [
                {
                    **entry,
                    "address": AMF_CONTROL_IP,
                }
                for entry in amf_configs
                if isinstance(entry, dict)
            ]
        payload["slices"] = [
            {"sst": slice_map[slice_ref].sst, "sd": _format_slice_hex(slice_map[slice_ref])}
            for slice_ref in gnb.slices
        ]
        _yaml_dump(config_dir / f"{gnb.name}-gnbcfg.yaml", payload)


def _build_gnb_search_list(
    ue_name: str,
    preferred_gnbs: tuple[str, ...],
    gnb_indices: dict[str, int],
    inline_map: dict[str, str],
    resolved_topology: ResolvedScenarioTopology,
) -> list[str]:
    ordered_gnbs = list(preferred_gnbs)
    target_gnb = resolved_topology.ue_to_gnb[ue_name]
    if target_gnb not in ordered_gnbs:
        ordered_gnbs.insert(0, target_gnb)
    if not ordered_gnbs:
        ordered_gnbs = [target_gnb]
    return [inline_map.get(gnb_name, gnb_service_ip(gnb_indices[gnb_name])) for gnb_name in ordered_gnbs]


def _render_ue_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    slice_map = scenario.slice_map()
    inline_map = _inline_gnb_ip_map(scenario, resolved_topology)

    base_name = "uecfg-ulcl.yaml" if scenario.free5gc.mode == "ulcl" else "uecfg.yaml"
    base_cfg = _yaml_load(Path(scenario.free5gc.config_root) / base_name)

    gnb_indices = {gnb.name: index for index, gnb in enumerate(scenario.gnbs, start=1)}

    for ue in scenario.ues:
        payload = deepcopy(base_cfg)
        payload["supi"] = ue.supi
        payload["key"] = ue.key
        payload["op"] = ue.op
        payload["opType"] = ue.op_type
        payload["amf"] = ue.amf
        payload["gnbSearchList"] = _build_gnb_search_list(
            ue.name,
            ue.free5gc_policy.preferred_gnbs,
            gnb_indices,
            inline_map,
            resolved_topology,
        )
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


def _format_tsv_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\n", " ")


def _render_ns3_flow_profiles(scenario: ScenarioConfig, output_path: Path) -> None:
    slice_map = scenario.slice_map()
    app_map = scenario.app_map()
    header = [
        "flow_id",
        "flow_name",
        "ue_name",
        "supi",
        "app_id",
        "app_name",
        "slice_ref",
        "slice_snssai",
        "service_type",
        "service_type_id",
        "five_qi",
        "packet_size_bytes",
        "arrival_rate_pps",
        "latency_ms",
        "jitter_ms",
        "loss_rate",
        "bandwidth_dl_mbps",
        "bandwidth_ul_mbps",
        "guaranteed_bandwidth_dl_mbps",
        "guaranteed_bandwidth_ul_mbps",
        "priority",
        "allocated_bandwidth_dl_mbps",
        "allocated_bandwidth_ul_mbps",
        "optimize_requested",
    ]
    lines = ["\t".join(header)]
    for flow in scenario.flows:
        slice_config = slice_map[flow.slice_ref]
        app_config = app_map.get(flow.app_id)
        app_name = flow.app_name or (app_config.name if app_config is not None else flow.app_id)
        values = [
            flow.flow_id,
            flow.name,
            flow.ue_name,
            flow.supi,
            flow.app_id,
            app_name,
            flow.slice_ref,
            f"{slice_config.sst:02d}{slice_config.sd.lower()}",
            flow.service_type,
            flow.service_type_id,
            flow.five_qi,
            flow.packet_size_bytes,
            flow.arrival_rate_pps,
            flow.sla_target.latency_ms,
            flow.sla_target.jitter_ms,
            flow.sla_target.loss_rate,
            flow.sla_target.bandwidth_dl_mbps,
            flow.sla_target.bandwidth_ul_mbps,
            flow.sla_target.guaranteed_bandwidth_dl_mbps,
            flow.sla_target.guaranteed_bandwidth_ul_mbps,
            flow.sla_target.priority,
            flow.allocated_bandwidth_dl_mbps,
            flow.allocated_bandwidth_ul_mbps,
            flow.optimize_requested,
        ]
        lines.append("\t".join(_format_tsv_value(value) for value in values))
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_run_assets(
    project_root: Path,
    scenario: ScenarioConfig,
    run_id: str,
) -> RenderedRun:
    run_dir = project_root / "artifacts" / "runs" / run_id
    generated_dir = run_dir / "generated"
    config_dir = generated_dir / "config"
    subscriber_dir = generated_dir / "subscribers"
    ns3_dir = generated_dir / scenario.ns3.output_subdir
    flow_profile_file = generated_dir / "ns3-flow-profiles.tsv"
    state_dir = run_dir / "state"
    archive_dir = _resolve_output_path(run_dir, scenario.writer.archive_dir)
    state_db = _resolve_output_path(run_dir, scenario.writer.state_db)
    resolved_topology = resolve_scenario_topology(scenario)
    initial_topology_file = generated_dir / "initial-topology.json"

    for path in (config_dir, subscriber_dir, ns3_dir, state_dir, archive_dir, state_db.parent):
        path.mkdir(parents=True, exist_ok=True)

    initial_topology_file.write_text(
        json.dumps(resolved_topology.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    _render_core_configs(scenario, config_dir)
    _render_gnb_configs(scenario, config_dir, resolved_topology)
    _render_ue_configs(scenario, config_dir, resolved_topology)
    _render_ns3_flow_profiles(scenario, flow_profile_file)

    compose_render = render_compose_for_run(scenario, config_dir, resolved_topology)
    compose_file = generated_dir / "free5gc-compose.generated.yaml"
    _yaml_dump(compose_file, compose_render.compose_payload)

    subscriber_assets = render_subscriber_bootstrap_assets(
        scenario,
        config_dir,
        compose_render.compose_payload,
        subscriber_dir,
        resolved_topology,
    )

    bridge_plans = build_bridge_plan(scenario, compose_render.service_map, resolved_topology)
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
        flow_profile_file=flow_profile_file,
        state_db=state_db,
        archive_dir=archive_dir,
        service_map=compose_render.service_map,
        core_services=compose_render.core_services,
        ran_services=compose_render.ran_services,
        subscriber_payloads=subscriber_assets.payload_files,
        free5gc_webui_url=subscriber_assets.webui_base_url,
        resolved_topology=resolved_topology,
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
                "resolved_topology": resolved_topology.to_dict(),
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