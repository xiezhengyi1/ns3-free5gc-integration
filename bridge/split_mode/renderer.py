"""Asset rendering for split-mode runs."""

from __future__ import annotations

from dataclasses import dataclass, replace
import ipaddress
import json
from pathlib import Path
import shutil
from typing import Any

import yaml

from bridge.common.ids import generate_run_id, safe_name
from bridge.common.topology import resolve_scenario_topology
from adapters.free5gc_ueransim.compose_override import build_n3_network_plan, upf_service_ip
from bridge.orchestrator.config_renderer import render_control_plane_assets

from .config import SplitModeConfig
from .manifest import SplitCommandSpec, SplitRunManifest


@dataclass(slots=True)
class RenderedSplitRun:
    run_id: str
    run_dir: Path
    generated_dir: Path
    manifest: SplitRunManifest
    manifest_path: Path


@dataclass(slots=True, frozen=True)
class InstanceIsolation:
    slot: int
    compose_project_name: str
    pcf_control_ip: str
    mongo_container: str


def _replace_text_values(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        updated = value
        for source, target in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
            updated = updated.replace(source, target)
        return updated
    if isinstance(value, list):
        return [_replace_text_values(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: _replace_text_values(item, replacements) for key, item in value.items()}
    return value


def _instance_network_prefix(*, slot: int, second_octet_base: int) -> str:
    if slot <= 0 or slot > 2047:
        raise ValueError("instance_slot must be between 1 and 2047")
    # Slot zero is the legacy unscoped topology (172.16.0.0/24 and
    # 172.24.0.0/24).  Worker slot one must therefore begin at `.1`, not reuse
    # that topology under a different Compose project name.
    second_octet = second_octet_base + (slot // 256)
    third_octet = slot % 256
    if second_octet > 31:
        raise ValueError("instance_slot exceeds the available isolated Docker network range")
    return f"172.{second_octet}.{third_octet}"


def _rewrite_published_ports(compose_payload: dict[str, Any], *, slot: int) -> None:
    # Preserve 28000-28019 for the legacy unscoped instance.
    next_port = 28000 + (slot * 20)
    if next_port + 19 > 65535:
        raise ValueError("instance_slot exceeds the available host port range")

    for service in (compose_payload.get("services") or {}).values():
        if not isinstance(service, dict):
            continue
        ports = service.get("ports")
        if not isinstance(ports, list):
            continue
        updated_ports: list[Any] = []
        for item in ports:
            if isinstance(item, str):
                fragments = item.split(":")
                if len(fragments) >= 2:
                    fragments[-2] = str(next_port)
                    next_port += 1
                    updated_ports.append(":".join(fragments))
                    continue
            elif isinstance(item, dict) and item.get("published") is not None:
                updated = dict(item)
                updated["published"] = str(next_port)
                next_port += 1
                updated_ports.append(updated)
                continue
            updated_ports.append(item)
        service["ports"] = updated_ports


def _resolve_published_port(service: dict[str, Any], *, target_port: int) -> int | None:
    for item in service.get("ports") or []:
        if isinstance(item, str):
            fragments = item.split(":")
            if len(fragments) >= 2:
                target = fragments[-1].split("/", 1)[0]
                if target == str(target_port):
                    return int(fragments[-2])
        elif isinstance(item, dict) and str(item.get("target") or "") == str(target_port):
            published = item.get("published")
            if published is not None:
                return int(published)
    return None


def _resolve_mongo_container_name(services: dict[str, Any], *, compose_project_name: str) -> str:
    """Return the exact Mongo container name after Compose namespacing."""
    for service_name, service in services.items():
        if not isinstance(service, dict):
            continue
        container_name = str(service.get("container_name") or "").strip()
        image_name = str(service.get("image") or "").strip().lower()
        is_mongo_service = "mongo" in str(service_name).lower() or "mongo" in image_name
        if is_mongo_service:
            return container_name or f"{compose_project_name}-{safe_name(str(service_name))}-1"
    # Some lightweight test fixtures omit MongoDB entirely.  Production
    # free5GC Compose files name it explicitly as `mongodb`, so retain that
    # compatible fallback rather than assuming Compose's `-1` suffix.
    return f"{compose_project_name}-mongodb"


def _apply_instance_isolation(
    rendered: Any,
    scenario: Any,
    *,
    instance_slot: int,
) -> InstanceIsolation:
    """Namespace every host-visible resource required by one simulator worker."""
    control_prefix = _instance_network_prefix(slot=instance_slot, second_octet_base=16)
    n3_prefix = _instance_network_prefix(slot=instance_slot, second_octet_base=24)
    compose_project_name = f"{safe_name(scenario.free5gc.project_name)}-w{instance_slot}"
    replacements = {"10.100.200.": f"{control_prefix}."}

    base_n3_plan = build_n3_network_plan(scenario)
    if base_n3_plan is not None:
        target_network = ipaddress.ip_network(f"{n3_prefix}.0/24", strict=True)
        target_hosts = iter(target_network.hosts())
        replacement_ips = [str(next(target_hosts)) for _ in range(1 + len(scenario.gnbs) + len(scenario.upfs))]
        source_ips = [
            base_n3_plan.gateway_ip,
            *[base_n3_plan.gnb_ips[gnb.name] for gnb in scenario.gnbs],
            *[base_n3_plan.upf_ips[upf.name] for upf in scenario.upfs],
        ]
        replacements.update(dict(zip(source_ips, replacement_ips, strict=True)))
        replacements[base_n3_plan.subnet_cidr] = str(target_network)

    compose_payload = _yaml_load(rendered.compose_file)
    compose_payload = _replace_text_values(compose_payload, replacements)
    compose_payload["name"] = compose_project_name
    networks = compose_payload.setdefault("networks", {})
    privnet = networks.setdefault("privnet", {})
    privnet["driver"] = "bridge"
    privnet["ipam"] = {"config": [{"subnet": f"{control_prefix}.0/24", "gateway": f"{control_prefix}.1"}]}
    if base_n3_plan is not None:
        n3_network = networks.setdefault(base_n3_plan.network_name, {})
        n3_network["driver"] = "bridge"
        n3_network["ipam"] = {"config": [{"subnet": f"{n3_prefix}.0/24", "gateway": f"{n3_prefix}.1"}]}
    for network_index, (network_name, network) in enumerate(networks.items(), start=1):
        if not isinstance(network, dict):
            continue
        if network.get("name"):
            network["name"] = f"{compose_project_name}-{safe_name(str(network_name))}"
        if str(network.get("driver") or "bridge") == "bridge":
            driver_opts = network.setdefault("driver_opts", {})
            if not isinstance(driver_opts, dict):
                raise ValueError(f"network {network_name} driver_opts must be a mapping")
            # Docker bridge interfaces live in the host namespace.  Their names
            # must be unique across workers and stay within Linux's 15-char
            # interface-name limit.
            driver_opts["com.docker.network.bridge.name"] = f"br-f5gc-w{instance_slot}-{network_index}"

    services = compose_payload.get("services") or {}
    if not isinstance(services, dict):
        raise ValueError("generated compose payload must define a services mapping")
    for service_name, service in services.items():
        if isinstance(service, dict) and service.get("container_name"):
            service["container_name"] = f"{compose_project_name}-{safe_name(str(service['container_name']))}"
    _rewrite_published_ports(compose_payload, slot=instance_slot)
    _yaml_dump(rendered.compose_file, compose_payload)

    for path in rendered.config_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            source_text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rewritten_text = _replace_text_values(source_text, replacements)
        if rewritten_text != source_text:
            path.write_text(rewritten_text, encoding="utf-8")

    webui_service = services.get("free5gc-webui")
    if isinstance(webui_service, dict):
        webui_port = _resolve_published_port(webui_service, target_port=5000)
        if webui_port is not None:
            rendered.free5gc_webui_url = f"http://127.0.0.1:{webui_port}"
    return InstanceIsolation(
        slot=instance_slot,
        compose_project_name=compose_project_name,
        pcf_control_ip=f"{control_prefix}.20",
        mongo_container=_resolve_mongo_container_name(services, compose_project_name=compose_project_name),
    )


def _format_tsv_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\n", " ")


def _yaml_load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _yaml_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _rewrite_split_control_plane_configs(config: SplitModeConfig, config_dir: Path) -> None:
    scenario = config.control_plane_scenario
    smf_path = config_dir / "smfcfg.yaml"
    smf_payload = _yaml_load(smf_path)
    configuration = smf_payload.get("configuration")
    if not isinstance(configuration, dict):
        raise ValueError("smfcfg.yaml must define configuration")
    userplane_information = configuration.get("userplaneInformation")
    if not isinstance(userplane_information, dict):
        raise ValueError("smfcfg.yaml must define userplaneInformation")
    up_nodes = userplane_information.get("upNodes")
    if not isinstance(up_nodes, dict):
        raise ValueError("smfcfg.yaml userplaneInformation must define upNodes")

    upf_control_ips = {upf.name: upf_service_ip(index) for index, upf in enumerate(scenario.upfs, start=1)}
    upf_index = 0
    for node_name, node_payload in up_nodes.items():
        if not isinstance(node_payload, dict):
            continue
        if str(node_payload.get("type", "")).upper() != "UPF":
            continue
        if upf_index >= len(scenario.upfs):
            raise ValueError(f"smfcfg.yaml has more UPF nodes than scenario.upfs: {node_name}")
        upf = scenario.upfs[upf_index]
        control_ip = upf_control_ips[upf.name]
        node_payload["nodeID"] = control_ip
        node_payload["addr"] = control_ip
        upf_index += 1
    if upf_index != len(scenario.upfs):
        raise ValueError("smfcfg.yaml UPF node count does not match scenario.upfs")
    _yaml_dump(smf_path, smf_payload)


def _rewrite_split_uerouting_pfds(config: SplitModeConfig, config_dir: Path) -> None:
    scenario = config.control_plane_scenario
    uerouting_path = config_dir / "uerouting.yaml"
    payload = _yaml_load(uerouting_path)
    pfd_entries = payload.get("pfdDataForApp")
    if not isinstance(pfd_entries, list):
        raise ValueError("uerouting.yaml must define pfdDataForApp")

    flow_by_id = {flow.flow_id: flow for flow in scenario.flows}
    flow_index_by_id = {flow.flow_id: index for index, flow in enumerate(scenario.flows)}

    # Split-mode ns-3 traffic does not use the real-UE traffic generator tuple.
    # It sends uplink traffic from UE source port 25000+N to remote host 1.0.0.2:6000+N,
    # and downlink traffic from remote host 1.0.0.2:15000+N to UE port 5000+N.
    remote_host_ip = "1.0.0.2"

    for app_entry in pfd_entries:
        if not isinstance(app_entry, dict):
            continue
        pfds = app_entry.get("pfds")
        if not isinstance(pfds, list):
            continue
        for pfd in pfds:
            if not isinstance(pfd, dict):
                continue
            flow_id = str(pfd.get("pfdID") or "").strip()
            if not flow_id:
                continue
            flow = flow_by_id.get(flow_id)
            if flow is None:
                raise ValueError(f"uerouting.yaml references unknown flow id {flow_id}")
            if flow.policy_filter:
                continue
            flow_index = flow_index_by_id[flow_id]
            downlink_port = 5000 + flow_index
            uplink_port = 6000 + flow_index
            downlink_source_port = 15000 + flow_index
            uplink_source_port = 25000 + flow_index
            pfd["flowDescriptions"] = [
                f"permit out udp from any {uplink_source_port} to {remote_host_ip}/32 {uplink_port}",
                f"permit out udp from {remote_host_ip}/32 {downlink_source_port} to any {downlink_port}",
            ]

    _yaml_dump(uerouting_path, payload)


def _render_live_flow_profiles(config: SplitModeConfig, output_path: Path) -> None:
    scenario = config.control_plane_scenario
    slice_map = scenario.slice_map()
    app_map = scenario.app_map()
    header = [
        "flow_id",
        "flow_name",
        "ue_name",
        "supi",
        "app_id",
        "app_name",
        "session_ref",
        "slice_ref",
        "slice_snssai",
        "dnn",
        "upf_ref",
        "service_type",
        "service_type_id",
        "five_qi",
        "packet_size_bytes",
        "arrival_rate_pps",
        "dl_packet_size_bytes",
        "ul_packet_size_bytes",
        "dl_arrival_rate_pps",
        "ul_arrival_rate_pps",
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
        "policy_filter",
        "precedence",
        "qos_ref",
        "charging_method",
        "quota",
        "unit_cost",
        "enabled",
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
            flow.session_ref,
            flow.slice_ref,
            f"{slice_config.sst:02d}{slice_config.sd.lower()}",
            flow.dnn,
            scenario.resolve_flow_upf(flow),
            flow.service_type,
            flow.service_type_id,
            flow.five_qi,
            flow.packet_size_bytes,
            flow.arrival_rate_pps,
            flow.dl_packet_size_bytes,
            flow.ul_packet_size_bytes,
            flow.dl_arrival_rate_pps,
            flow.ul_arrival_rate_pps,
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
            flow.policy_filter,
            flow.precedence,
            flow.qos_ref,
            flow.charging_method,
            flow.quota,
            flow.unit_cost,
            "false",
        ]
        lines.append("\t".join(_format_tsv_value(value) for value in values))
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_split_run(
    project_root: Path,
    config: SplitModeConfig,
    run_id: str | None = None,
    live_graph_snapshot_id: str | None = None,
    instance_slot: int = 0,
    gateway_port: int = 18080,
    reset_port: int = 18081,
    graph_db_url: str | None = None,
) -> RenderedSplitRun:
    resolved_run_id = run_id or generate_run_id(config.scenario_id)
    if resolved_run_id.startswith("-"):
        raise ValueError(f"invalid run id: {resolved_run_id}")
    run_dir = project_root / "artifacts" / "runs" / resolved_run_id
    if run_dir.exists():
        shutil.rmtree(run_dir)
    if gateway_port <= 0 or gateway_port > 65535 or reset_port <= 0 or reset_port > 65535:
        raise ValueError("gateway_port and reset_port must be valid TCP ports")
    if graph_db_url:
        config = replace(
            config,
            base_scenario=replace(
                config.base_scenario,
                writer=replace(config.base_scenario.writer, graph_db_url=str(graph_db_url)),
            ),
        )
    rendered = render_control_plane_assets(project_root, config.control_plane_scenario, resolved_run_id)
    _rewrite_split_control_plane_configs(config, rendered.config_dir)
    if config.control_plane_scenario.free5gc.mode == "ulcl":
        _rewrite_split_uerouting_pfds(config, rendered.config_dir)
    isolation = (
        _apply_instance_isolation(rendered, config.control_plane_scenario, instance_slot=instance_slot)
        if instance_slot
        else None
    )
    run_dir = rendered.run_dir
    generated_dir = rendered.generated_dir
    split_dir = generated_dir / "split-mode"
    split_dir.mkdir(parents=True, exist_ok=True)
    resolved_topology = resolve_scenario_topology(config.control_plane_scenario)
    resolved_live_graph_snapshot_id = (
        str(live_graph_snapshot_id or "").strip() or f"live-{config.control_plane_scenario.scenario_id}"
    )

    flow_profile_file = generated_dir / config.control_plane_scenario.ns3.output_subdir / "flow-profiles-live.tsv"
    runtime_state_file = run_dir / "state" / "split-runtime-state.json"
    result_file = run_dir / "state" / "split-results.jsonl"
    _render_live_flow_profiles(config, flow_profile_file)

    base_manifest = rendered
    scenario = config.control_plane_scenario
    python_executable = project_root / ".venv" / "bin" / "python3"
    python_command = str(python_executable) if python_executable.exists() else "python3"
    compose_project_name = isolation.compose_project_name if isolation is not None else scenario.free5gc.project_name
    compose_base_argv = [
        "docker",
        "compose",
        "-p",
        compose_project_name,
        "-f",
        str(rendered.compose_file),
    ]
    service_map = base_manifest.service_map
    upf_services = list(service_map["upf"].values())
    gnb_services = list(service_map["gnb"].values())
    ue_services = list(service_map["ue"].values())
    smf_services = [service for service in base_manifest.core_services if service == "free5gc-smf"]
    infra_core_services = [
        service for service in base_manifest.core_services if service not in set(upf_services) and service not in set(smf_services)
    ]

    gnb_index_by_name = {gnb.name: index for index, gnb in enumerate(scenario.gnbs, start=1)}
    upf_index_by_name = {upf.name: index for index, upf in enumerate(scenario.upfs, start=1)}
    ue_gnb_map = ",".join(str(gnb_index_by_name[resolved_topology.ue_to_gnb[ue.name]]) for ue in scenario.ues)
    gnb_upf_links = ";".join(
        f"{gnb_index_by_name[gnb.name]}:{upf_index_by_name[upf_name]}"
        for gnb in scenario.gnbs
        for upf_name in resolved_topology.gnb_to_upfs[gnb.name]
    )
    gnb_positions = ";".join(
        (
            f"{position.x}:{position.y}:{position.z}"
            if (position := resolved_topology.gnb_positions.get(gnb.name)) is not None
            else "auto"
        )
        for gnb in scenario.gnbs
    )
    ue_positions = ";".join(
        (
            f"{position.x}:{position.y}:{position.z}"
            if (position := resolved_topology.ue_positions.get(ue.name)) is not None
            else "auto"
        )
        for ue in scenario.ues
    )

    commands = [
        SplitCommandSpec(
            name="compose-up-core",
            cwd=str(project_root),
            argv=[*compose_base_argv, "up", "-d", "--force-recreate", "--remove-orphans", *infra_core_services],
        ),
        SplitCommandSpec(
            name="bootstrap-subscribers",
            cwd=str(project_root),
            argv=[
                python_command,
                "-m",
                "adapters.free5gc_ueransim.subscriber_bootstrap",
                "--base-url",
                base_manifest.free5gc_webui_url,
                "--timeout-seconds",
                "120",
                "--interval-seconds",
                "2",
                *[str(item) for item in base_manifest.subscriber_payloads],
            ],
        ),
    ]
    if scenario.free5gc.mode == "ulcl":
        commands.append(
            SplitCommandSpec(
                name="bootstrap-app-data",
                cwd=str(project_root),
                argv=[
                    python_command,
                    str(project_root / "scripts" / "bootstrap_app_data.py"),
                    "--uerouting-file",
                    str(run_dir / "generated" / "config" / "uerouting.yaml"),
                    "--mongo-container",
                    isolation.mongo_container if isolation is not None else "mongodb",
                    "--database",
                    "free5gc",
                ],
            )
        )
    commands.extend(
        [
            SplitCommandSpec(
                name="compose-up-upf",
                cwd=str(project_root),
                argv=[*compose_base_argv, "up", "-d", "--force-recreate", "--remove-orphans", *upf_services],
            ),
            SplitCommandSpec(
                name="compose-up-gnb",
                cwd=str(project_root),
                argv=[*compose_base_argv, "up", "-d", "--force-recreate", "--remove-orphans", *gnb_services],
            ),
            SplitCommandSpec(
                name="compose-up-smf",
                cwd=str(project_root),
                argv=[*compose_base_argv, "up", "-d", "--force-recreate", "--remove-orphans", *smf_services],
            ),
            SplitCommandSpec(
                name="writer-follow-free5gc",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.writer.cli",
                    "follow-compose-logs",
                    "--parser",
                    "free5gc",
                    "--compose-file",
                    str(rendered.compose_file),
                    "--project-name",
                    compose_project_name,
                    "--run-id",
                    resolved_run_id,
                    "--scenario-id",
                    scenario.scenario_id,
                    "--tick-ms",
                    str(scenario.tick_ms),
                    "--clock-file",
                    str(base_manifest.clock_file),
                    "--tail",
                    "all",
                    "--state-db",
                    str(base_manifest.state_db),
                    "--archive-dir",
                    str(base_manifest.archive_dir),
                    *[item for service in [*infra_core_services, *upf_services, *smf_services] for item in ("--service", service)],
                ],
                background=True,
            ),
            SplitCommandSpec(
                name="wait-for-pfcp-ready",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.split_mode.pfcp_ready_wait",
                    "--state-db",
                    str(base_manifest.state_db),
                    "--run-id",
                    resolved_run_id,
                    "--expected-upfs",
                    str(len(scenario.upfs)),
                    "--timeout-seconds",
                    "60",
                    "--poll-interval-seconds",
                    "0.5",
                ],
            ),
            SplitCommandSpec(
                name="wait-for-ue-ready",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.split_mode.ue_startup",
                    "--compose-file",
                    str(rendered.compose_file),
                    "--project-name",
                    compose_project_name,
                    "--state-db",
                    str(base_manifest.state_db),
                    "--run-id",
                    resolved_run_id,
                    "--timeout-seconds",
                    "90",
                    "--poll-interval-seconds",
                    "0.5",
                    *[
                        item
                        for ue, service_name in zip(scenario.ues, ue_services, strict=True)
                        for item in ("--ue", f"{service_name},{ue.name},{len(ue.sessions)}")
                    ],
                ],
            ),
            # UERANSIM and free5GC can lose an N1/N2 response when all of the
            # multi-session UEs request PDU sessions at once.  `ue_startup`
            # therefore creates and verifies one UE at a time by inspecting
            # its TUN interfaces.  Once every UE is live, this writer replays
            # the complete Compose history into the event store for the gate.
            SplitCommandSpec(
                name="writer-follow-ueransim",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.writer.cli",
                    "follow-compose-logs",
                    "--parser",
                    "ueransim",
                    "--compose-file",
                    str(rendered.compose_file),
                    "--project-name",
                    compose_project_name,
                    "--run-id",
                    resolved_run_id,
                    "--scenario-id",
                    scenario.scenario_id,
                    "--tick-ms",
                    str(scenario.tick_ms),
                    "--clock-file",
                    str(base_manifest.clock_file),
                    "--tail",
                    "all",
                    "--state-db",
                    str(base_manifest.state_db),
                    "--archive-dir",
                    str(base_manifest.archive_dir),
                    *[item for service in [*gnb_services, *ue_services] for item in ("--service", service)],
                ],
                background=True,
            ),
            SplitCommandSpec(
                name="split-gate",
                cwd=str(project_root),
                argv=[],
                background=True,
            ),
        ]
    )
    commands[-1].argv = [
        python_command,
        "-m",
        "bridge.split_mode.session_gate",
        "--split-scenario",
        str(split_dir / "split-mode.yaml"),
        "--state-db",
        str(base_manifest.state_db),
        "--flow-profile-file",
        str(flow_profile_file),
        "--runtime-state-file",
        str(runtime_state_file),
        "--poll-ms",
        str(config.runtime.state_poll_ms),
    ]
    commands.extend(
        [
            SplitCommandSpec(
                name="writer-follow-split-ns3",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.writer.cli",
                    "follow-jsonl",
                    str(base_manifest.snapshot_file),
                "--state-db",
                str(base_manifest.state_db),
                "--archive-dir",
                str(base_manifest.archive_dir),
                "--tick-ms",
                str(scenario.tick_ms),
                "--from-end",
            ],
            background=True,
        ),
            SplitCommandSpec(
                name="policy-acceptor",
                cwd=str(project_root),
                argv=[
                    "bash",
                    str(project_root / "scripts" / "run_policy_acceptor.sh"),
                    "--host",
                    "0.0.0.0",
                    "--port",
                    str(gateway_port),
                    "--instance-id",
                    f"slot-{instance_slot}" if instance_slot else "default",
                    "--flow-profile-file",
                    str(flow_profile_file),
                    "--latest-snapshot-file",
                    str(Path(base_manifest.archive_dir) / resolved_run_id / "latest.json"),
                    "--state-file",
                    str(run_dir / "state" / "policy-acceptor-state.json"),
                    "--upstream-pcf-host",
                    isolation.pcf_control_ip if isolation is not None else "10.100.200.20",
                    "--upstream-pcf-port",
                    "8000",
                    "--default-timeout-ms",
                    "30000",
                ],
                background=True,
            ),
            SplitCommandSpec(
                name="split-results",
                cwd=str(project_root),
                argv=[
                    python_command,
                    "-m",
                    "bridge.split_mode.result_writer",
                    "--state-db",
                    str(base_manifest.state_db),
                    "--runtime-state-file",
                    str(runtime_state_file),
                    "--output-file",
                    str(result_file),
                    "--run-id",
                    resolved_run_id,
                    "--scenario-id",
                    scenario.scenario_id,
                    "--poll-ms",
                    str(config.runtime.state_poll_ms),
                ],
                background=True,
            ),
            SplitCommandSpec(
                name="ns3-build",
                cwd=str(project_root),
                argv=["bash", str(project_root / "scripts" / "build_ns3_split.sh")],
                env={"NS3_ROOT": scenario.ns3.ns3_root},
            ),
            SplitCommandSpec(
                name="ns3-run",
                cwd=str(project_root),
                argv=[
                    "bash",
                    str(project_root / "scripts" / "run_split_ns3.sh"),
                    "--run-id",
                    resolved_run_id,
                    "--scenario-id",
                    scenario.scenario_id,
                    "--g-nb-num",
                    str(len(scenario.gnbs)),
                    "--ue-num",
                    str(len(scenario.ues)),
                    "--ue-num-per-g-nb",
                    str(max(1, len(scenario.ues))),
                    "--tick-ms",
                    str(scenario.tick_ms),
                    "--sim-time-ms",
                    str(scenario.ns3.sim_time_ms),
                    "--simulator",
                    scenario.ns3.simulator,
                    "--output-file",
                    str(base_manifest.snapshot_file),
                    "--clock-file",
                    str(base_manifest.clock_file),
                    "--flow-profile-file",
                    str(flow_profile_file),
                    "--slice-resource-file",
                    str(base_manifest.ns3_slice_resource_file),
                    "--policy-reload-ms",
                    str(config.ns3.policy_reload_ms),
                    "--upf-names",
                    ",".join(upf.name for upf in scenario.upfs) or "upf",
                    "--slice-sds",
                    ",".join(item.sd for item in scenario.slices) or "010203",
                    "--ue-supis",
                    ",".join(ue.supi for ue in scenario.ues),
                    "--ue-gnb-map",
                    ue_gnb_map,
                    "--gnb-upf-links",
                    gnb_upf_links,
                    "--gnb-positions",
                    gnb_positions,
                    "--ue-positions",
                    ue_positions,
                    "--nr-numerology",
                    str(config.ns3.nr_numerology),
                    "--nr-bandwidth-hz",
                    str(config.ns3.nr_bandwidth_hz),
                    "--nr-central-frequency-hz",
                    str(config.ns3.nr_central_frequency_hz),
                    "--nr-tx-power-db",
                    str(config.radio.gnb_tx_power_dbm),
                    "--scheduler-type",
                    config.radio.scheduler_type,
                    "--tdd-pattern",
                    config.radio.resolved_tdd_pattern(),
                    "--ue-tx-power-db",
                    str(config.radio.ue_tx_power_dbm),
                    "--gnb-noise-figure-db",
                    str(config.radio.gnb_noise_figure_db),
                    "--ue-noise-figure-db",
                    str(config.radio.ue_noise_figure_db),
                    "--enable-uplink-power-control",
                    "true" if config.radio.enable_uplink_power_control else "false",
                ],
                env={"NS3_ROOT": scenario.ns3.ns3_root},
            ),
            SplitCommandSpec(
                name="compose-down",
                cwd=str(project_root),
                argv=[*compose_base_argv, "down"],
            ),
        ]
    )
    if scenario.writer.graph_db_url:
        next(command for command in commands if command.name == "writer-follow-split-ns3").argv.extend(
            [
                "--graph-db-url",
                scenario.writer.graph_db_url,
                "--live-graph-snapshot-id",
                resolved_live_graph_snapshot_id,
            ]
        )

    split_yaml_path = split_dir / "split-mode.yaml"
    split_yaml_path.write_text(
        json.dumps(
            {
                "name": config.name,
                "scenario_id": config.scenario_id,
                "base_scenario": str(config.base_scenario_path),
                "ns3": {
                    "output_subdir": config.ns3.output_subdir,
                    "scratch_name": config.ns3.scratch_name,
                    "policy_reload_ms": config.ns3.policy_reload_ms,
                    "activation_poll_ms": config.ns3.activation_poll_ms,
                    "sim_time_ms": config.ns3.sim_time_ms,
                    "nr_numerology": config.ns3.nr_numerology,
                    "nr_bandwidth_hz": config.ns3.nr_bandwidth_hz,
                    "nr_central_frequency_hz": config.ns3.nr_central_frequency_hz,
                },
                "radio": {
                    "scheduler_type": config.radio.scheduler_type,
                    "tdd_pattern": config.radio.tdd_pattern,
                    "gnb_tx_power_dbm": config.radio.gnb_tx_power_dbm,
                    "ue_tx_power_dbm": config.radio.ue_tx_power_dbm,
                    "enable_uplink_power_control": config.radio.enable_uplink_power_control,
                    "gnb_noise_figure_db": config.radio.gnb_noise_figure_db,
                    "ue_noise_figure_db": config.radio.ue_noise_figure_db,
                },
                "runtime": {
                    "startup_timeout_seconds": config.runtime.startup_timeout_seconds,
                    "state_poll_ms": config.runtime.state_poll_ms,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = SplitRunManifest(
        run_id=resolved_run_id,
        scenario_id=scenario.scenario_id,
        live_graph_snapshot_id=(resolved_live_graph_snapshot_id if scenario.writer.graph_db_url else None),
        run_dir=str(run_dir),
        compose_file=str(rendered.compose_file),
        compose_project_name=compose_project_name,
        snapshot_file=str(base_manifest.snapshot_file),
        clock_file=str(base_manifest.clock_file),
        state_db=str(base_manifest.state_db),
        archive_dir=str(base_manifest.archive_dir),
        flow_profile_file=str(flow_profile_file),
        slice_resource_file=str(base_manifest.ns3_slice_resource_file),
        runtime_state_file=str(runtime_state_file),
        result_file=str(result_file),
        subscriber_payloads=[str(item) for item in base_manifest.subscriber_payloads],
        core_services=list(base_manifest.core_services),
        ran_services=list(base_manifest.ran_services),
        service_map=service_map,
        fast_reset={
            "api_url": f"http://127.0.0.1:{reset_port}",
            "state_file": str(run_dir / "state" / "fast-reset-state.json"),
            "process_registry": str(run_dir / "state" / "fast-reset-processes.json"),
            "serve_argv": [
                python_command,
                "-m",
                "bridge.orchestrator.fast_reset",
                "serve",
                str(run_dir / "run-manifest.split.json"),
                "--port",
                str(reset_port),
            ],
        },
        commands=commands,
    )
    manifest_path = run_dir / "run-manifest.split.json"
    manifest_path.write_text(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return RenderedSplitRun(
        run_id=resolved_run_id,
        run_dir=run_dir,
        generated_dir=generated_dir,
        manifest=manifest,
        manifest_path=manifest_path,
    )
