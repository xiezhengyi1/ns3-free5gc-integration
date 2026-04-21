"""Render scenario-driven configs and run artifacts."""

from __future__ import annotations

from copy import deepcopy
import json
import re
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
from bridge.common.scenario import FlowConfig, ScenarioConfig, SliceConfig, load_scenario
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


def _render_nssf_config(scenario: ScenarioConfig, config_dir: Path) -> None:
    payload = _yaml_load(Path(scenario.free5gc.config_root) / "nssfcfg.yaml")
    configuration = payload.get("configuration")
    if not isinstance(configuration, dict):
        raise ValueError("nssfcfg.yaml must define configuration")

    supported_plmns = configuration.get("supportedPlmnList")
    if not isinstance(supported_plmns, list) or not supported_plmns:
        raise ValueError("nssfcfg.yaml must define supportedPlmnList")
    first_plmn = supported_plmns[0]
    if not isinstance(first_plmn, dict):
        raise ValueError("nssfcfg.yaml supportedPlmnList entries must be mappings")
    if "mcc" not in first_plmn or "mnc" not in first_plmn:
        raise ValueError("nssfcfg.yaml supportedPlmnList entries must define mcc and mnc")

    plmn_id = {
        "mcc": first_plmn["mcc"],
        "mnc": first_plmn["mnc"],
    }
    supported_snssais = [
        {
            "sst": slice_config.sst,
            "sd": slice_config.sd.lower(),
        }
        for slice_config in scenario.slices
    ]

    nrf_uri = configuration.get("nrfUri")
    if not isinstance(nrf_uri, str) or not nrf_uri:
        raise ValueError("nssfcfg.yaml must define nrfUri")
    nrf_nf_instances_uri = f"{nrf_uri.rstrip('/')}/nnrf-nfm/v1/nf-instances"

    def format_tac(value: int) -> str:
        return f"{value:06x}"

    availability_data = [
        {
            "tai": {
                "plmnId": deepcopy(plmn_id),
                "tac": format_tac(gnb.tac),
            },
            "supportedSnssaiList": deepcopy(supported_snssais),
        }
        for gnb in scenario.gnbs
    ]

    configuration["supportedPlmnList"] = [deepcopy(plmn_id)]
    configuration["supportedNssaiInPlmnList"] = [
        {
            "plmnId": deepcopy(plmn_id),
            "supportedSnssaiList": deepcopy(supported_snssais),
        }
    ]
    configuration["nsiList"] = [
        {
            "snssai": {
                "sst": slice_config.sst,
                "sd": slice_config.sd.lower(),
            },
            "nsiInformationList": [
                {
                    "nrfId": nrf_nf_instances_uri,
                    "nsiId": 20 + index,
                }
            ],
        }
        for index, slice_config in enumerate(scenario.slices, start=1)
    ]

    amf_set_list = configuration.get("amfSetList")
    if isinstance(amf_set_list, list):
        for entry in amf_set_list:
            if isinstance(entry, dict):
                entry["supportedNssaiAvailabilityData"] = deepcopy(availability_data)

    amf_list = configuration.get("amfList")
    if isinstance(amf_list, list):
        for entry in amf_list:
            if isinstance(entry, dict):
                entry["supportedNssaiAvailabilityData"] = deepcopy(availability_data)

    configuration["taList"] = [
        {
            "tai": {
                "plmnId": deepcopy(plmn_id),
                "tac": format_tac(gnb.tac),
            },
            "accessType": "3GPP_ACCESS",
            "supportedSnssaiList": deepcopy(supported_snssais),
        }
        for gnb in scenario.gnbs
    ]

    _yaml_dump(config_dir / "nssfcfg.yaml", payload)


def _scenario_apns(scenario: ScenarioConfig) -> list[str]:
    return sorted(
        {
            session.apn
            for ue in scenario.ues
            for session in ue.sessions
        }
        | {upf.dnn for upf in scenario.upfs}
    )


def _scenario_slice_apns(scenario: ScenarioConfig) -> dict[str, list[str]]:
    fallback_apns = _scenario_apns(scenario)
    apns_by_slice = {slice_config.slice_id: [] for slice_config in scenario.slices}

    for ue in scenario.ues:
        for session in ue.sessions:
            slice_apns = apns_by_slice.setdefault(session.slice_ref, [])
            if session.apn not in slice_apns:
                slice_apns.append(session.apn)

    for slice_ref, apns in apns_by_slice.items():
        if apns:
            apns_by_slice[slice_ref] = sorted(apns)
            continue
        apns_by_slice[slice_ref] = list(fallback_apns)

    return apns_by_slice


def _default_dns_payload(configuration: dict[str, Any]) -> dict[str, str]:
    snssai_infos = configuration.get("snssaiInfos")
    if isinstance(snssai_infos, list):
        for entry in snssai_infos:
            if not isinstance(entry, dict):
                continue
            dnn_infos = entry.get("dnnInfos")
            if not isinstance(dnn_infos, list):
                continue
            for dnn_info in dnn_infos:
                if not isinstance(dnn_info, dict):
                    continue
                dns = dnn_info.get("dns")
                if isinstance(dns, dict):
                    return deepcopy(dns)
    return {
        "ipv4": "8.8.8.8",
        "ipv6": "2001:4860:4860::8888",
    }


def _dnn_pool(index: int) -> tuple[str, str]:
    second_octet = 60 + index
    return f"10.{second_octet}.0.0/16", f"10.{second_octet}.100.0/24"


def _ulcl_upnode_name(upf_name: str, role: str, used_names: set[str]) -> str:
    normalized_role = role.lower()
    normalized_name = upf_name.lower()
    candidate = upf_name.replace("-", "_").upper()
    if "branch" in normalized_role or normalized_name in {"i-upf", "iupf"}:
        candidate = "I-UPF"
    elif "anchor" in normalized_role or "psa" in normalized_role or normalized_name in {"psa-upf", "psaupf"}:
        candidate = "PSA-UPF"

    if candidate not in used_names:
        return candidate

    suffix = 2
    while f"{candidate}-{suffix}" in used_names:
        suffix += 1
    return f"{candidate}-{suffix}"


def _compose_inspect_targets(compose_payload: dict[str, Any]) -> dict[str, str]:
    payload: dict[str, str] = {}
    services = compose_payload.get("services")
    if not isinstance(services, dict):
        return payload
    for service_name, service_payload in services.items():
        target = service_name
        if isinstance(service_payload, dict):
            container_name = service_payload.get("container_name")
            if isinstance(container_name, str) and container_name:
                target = container_name
        payload[str(service_name)] = target
    return payload


def _render_ulcl_smf_config(
    scenario: ScenarioConfig,
    config_dir: Path,
    ulcl_root: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    slice_apn_map = _scenario_slice_apns(scenario)
    all_apns = sorted({apn for slice_apns in slice_apn_map.values() for apn in slice_apns})
    slice_pool_map = {
        slice_config.slice_id: _dnn_pool(index)
        for index, slice_config in enumerate(scenario.slices)
    }
    replacements = {
        "smf.free5gc.org": SMF_CONTROL_IP,
        "gnb.free5gc.org": gnb_service_ip(1),
    }
    replacements.update(
        {
            f"{upf.name}.free5gc.org": upf_service_ip(index)
            for index, upf in enumerate(scenario.upfs, start=1)
        }
    )

    smf_text = _rewrite_config_text(
        (ulcl_root / "smfcfg.yaml").read_text(encoding="utf-8"),
        replacements,
    )
    smf_payload = yaml.safe_load(smf_text)
    if not isinstance(smf_payload, dict):
        raise ValueError("ULCL smfcfg.yaml must decode to a mapping")

    smf_configuration = smf_payload.get("configuration")
    if not isinstance(smf_configuration, dict):
        raise ValueError("ULCL smfcfg.yaml must define configuration")

    sbi = smf_configuration.get("sbi")
    if isinstance(sbi, dict):
        sbi["registerIPv4"] = SMF_CONTROL_IP
        sbi["bindingIPv4"] = SMF_CONTROL_IP

    pfcp = smf_configuration.get("pfcp")
    if isinstance(pfcp, dict):
        pfcp["nodeID"] = SMF_CONTROL_IP
        pfcp["listenAddr"] = SMF_CONTROL_IP
        pfcp["externalAddr"] = SMF_CONTROL_IP

    metrics = smf_configuration.get("metrics")
    if isinstance(metrics, dict):
        metrics["bindingIPv4"] = SMF_CONTROL_IP

    dns_payload = _default_dns_payload(smf_configuration)
    smf_configuration["snssaiInfos"] = [
        {
            "sNssai": {
                "sst": slice_config.sst,
                "sd": slice_config.sd.lower(),
            },
            "dnnInfos": [
                {
                    "dnn": apn,
                    "dns": deepcopy(dns_payload),
                }
                for apn in slice_apn_map[slice_config.slice_id]
            ],
        }
        for slice_config in scenario.slices
    ]

    userplane_information = smf_configuration.get("userplaneInformation")
    if not isinstance(userplane_information, dict):
        raise ValueError("ULCL smfcfg.yaml must define userplaneInformation")

    template_up_nodes = userplane_information.get("upNodes")
    if not isinstance(template_up_nodes, dict):
        raise ValueError("ULCL smfcfg.yaml userplaneInformation must define upNodes")

    template_upf_nodes = {
        node_name: deepcopy(node_payload)
        for node_name, node_payload in template_up_nodes.items()
        if isinstance(node_payload, dict) and node_payload.get("type") == "UPF"
    }
    template_links = userplane_information.get("links")
    if not isinstance(template_links, list):
        template_links = []

    rendered_up_nodes: dict[str, dict[str, Any]] = {}
    used_node_names: set[str] = set()
    gnb_node_names: dict[str, str] = {}
    for index, gnb in enumerate(scenario.gnbs, start=1):
        node_name = f"gNB{index}"
        rendered_up_nodes[node_name] = {
            "type": "AN",
            "nodeID": gnb_service_ip(index),
        }
        used_node_names.add(node_name)
        gnb_node_names[gnb.name] = node_name

    upf_node_names: dict[str, str] = {}
    for index, upf in enumerate(scenario.upfs, start=1):
        node_name = _ulcl_upnode_name(upf.name, upf.role, used_node_names)
        template_payload = deepcopy(template_upf_nodes.get(node_name, {}))
        if not template_payload:
            template_payload = {"type": "UPF"}
        template_payload["nodeID"] = upf_service_ip(index)
        template_payload["addr"] = upf_service_ip(index)
        interfaces = template_payload.get("interfaces")
        if isinstance(interfaces, list):
            for interface in interfaces:
                if isinstance(interface, dict):
                    interface["endpoints"] = [upf_service_ip(index)]
                    interface["networkInstances"] = all_apns

        template_snssai_infos = template_payload.get("sNssaiUpfInfos")
        template_dnn_info = None
        if isinstance(template_snssai_infos, list):
            for snssai_info in template_snssai_infos:
                if not isinstance(snssai_info, dict):
                    continue
                dnn_upf_info_list = snssai_info.get("dnnUpfInfoList")
                if not isinstance(dnn_upf_info_list, list):
                    continue
                for dnn_upf_info in dnn_upf_info_list:
                    if isinstance(dnn_upf_info, dict):
                        template_dnn_info = deepcopy(dnn_upf_info)
                        break
                if template_dnn_info is not None:
                    break

        rendered_snssai_infos = []
        is_anchor = "anchor" in upf.role.lower() or "psa" in upf.role.lower()
        for slice_config in scenario.slices:
            dnn_upf_info_list = []
            for apn in slice_apn_map[slice_config.slice_id]:
                dnn_upf_info = deepcopy(template_dnn_info) if template_dnn_info is not None else {}
                dnn_upf_info["dnn"] = apn
                if is_anchor or "pools" in dnn_upf_info:
                    dnn_upf_info["pools"] = [{"cidr": slice_pool_map[slice_config.slice_id][0]}]
                else:
                    dnn_upf_info.pop("pools", None)
                if "staticPools" in dnn_upf_info:
                    dnn_upf_info["staticPools"] = [{"cidr": slice_pool_map[slice_config.slice_id][1]}]
                dnn_upf_info_list.append(dnn_upf_info)
            rendered_snssai_infos.append(
                {
                    "sNssai": {
                        "sst": slice_config.sst,
                        "sd": slice_config.sd.lower(),
                    },
                    "dnnUpfInfoList": dnn_upf_info_list,
                }
            )
        template_payload["sNssaiUpfInfos"] = rendered_snssai_infos
        rendered_up_nodes[node_name] = template_payload
        used_node_names.add(node_name)
        upf_node_names[upf.name] = node_name

    rendered_links: list[dict[str, str]] = []
    seen_links: set[tuple[str, str]] = set()

    def add_link(left: str, right: str) -> None:
        key = (left, right)
        if key in seen_links:
            return
        seen_links.add(key)
        rendered_links.append({"A": left, "B": right})

    for gnb in scenario.gnbs:
        add_link(
            gnb_node_names[gnb.name],
            upf_node_names[resolved_topology.gnb_to_upf[gnb.name]],
        )

    for link in template_links:
        if not isinstance(link, dict):
            continue
        left = link.get("A")
        right = link.get("B")
        if not isinstance(left, str) or not isinstance(right, str):
            continue
        if left in template_upf_nodes and right in template_upf_nodes:
            add_link(left, right)

    userplane_information["upNodes"] = rendered_up_nodes
    userplane_information["links"] = rendered_links
    _yaml_dump(config_dir / "smfcfg.yaml", smf_payload)


def _render_ulcl_upf_config(
    scenario: ScenarioConfig,
    config_dir: Path,
    ulcl_root: Path,
    upf_name: str,
    upf_index: int,
) -> None:
    slice_apn_map = _scenario_slice_apns(scenario)
    slice_pool_map = {
        slice_config.slice_id: _dnn_pool(index)
        for index, slice_config in enumerate(scenario.slices)
    }
    replacements = {
        f"{upf.name}.free5gc.org": upf_service_ip(index)
        for index, upf in enumerate(scenario.upfs, start=1)
    }

    upf_text = _rewrite_config_text(
        (ulcl_root / f"upfcfg-{upf_name}.yaml").read_text(encoding="utf-8"),
        replacements,
    )
    upf_payload = yaml.safe_load(upf_text)
    if not isinstance(upf_payload, dict):
        raise ValueError(f"ULCL UPF config for {upf_name} must decode to a mapping")

    pfcp = upf_payload.get("pfcp")
    if isinstance(pfcp, dict):
        pfcp["addr"] = upf_service_ip(upf_index)
        pfcp["nodeID"] = upf_service_ip(upf_index)

    gtpu = upf_payload.get("gtpu")
    if isinstance(gtpu, dict):
        if_list = gtpu.get("ifList")
        if isinstance(if_list, list):
            for interface in if_list:
                if isinstance(interface, dict):
                    interface["addr"] = upf_service_ip(upf_index)

    template_entry = None
    existing_dnn_entries = upf_payload.get("dnnList")
    if isinstance(existing_dnn_entries, list):
        for entry in existing_dnn_entries:
            if isinstance(entry, dict):
                template_entry = deepcopy(entry)
                break

    upf_payload["dnnList"] = []
    for slice_config in scenario.slices:
        cidr, _ = slice_pool_map[slice_config.slice_id]
        for apn in slice_apn_map[slice_config.slice_id]:
            dnn_entry = deepcopy(template_entry) if template_entry is not None else {}
            dnn_entry["dnn"] = apn
            dnn_entry["cidr"] = cidr
            upf_payload["dnnList"].append(dnn_entry)

    _yaml_dump(config_dir / f"{upf_name}-upfcfg.yaml", upf_payload)


def _ulcl_specific_path_nodes(topology_links: list[dict[str, str]]) -> list[str]:
    path_nodes = [link["B"] for link in topology_links[:-1]]
    return [node for node in path_nodes if isinstance(node, str)]


def _extract_specific_path_destination(flow: FlowConfig) -> str | None:
    if not flow.policy_filter:
        return None
    match = re.search(r"\bfrom\s+(\S+)\s+to\b", flow.policy_filter, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _ulcl_topology_links_for_ue(
    ue_name: str,
    scenario: ScenarioConfig,
    resolved_topology: ResolvedScenarioTopology,
    userplane_links: list[dict[str, str]],
) -> list[dict[str, str]]:
    gnb_index = next(
        index
        for index, gnb in enumerate(scenario.gnbs, start=1)
        if gnb.name == resolved_topology.ue_to_gnb[ue_name]
    )
    start_node = f"gNB{gnb_index}"

    reachable_nodes = {start_node}
    pending_nodes = [start_node]
    selected_links: list[dict[str, str]] = []
    seen_links: set[tuple[str, str]] = set()

    while pending_nodes:
        current_node = pending_nodes.pop(0)
        for link in userplane_links:
            left = link.get("A")
            right = link.get("B")
            if not isinstance(left, str) or not isinstance(right, str) or left != current_node:
                continue
            key = (left, right)
            if key in seen_links:
                continue
            seen_links.add(key)
            selected_links.append({"A": left, "B": right})
            if right not in reachable_nodes:
                reachable_nodes.add(right)
                pending_nodes.append(right)

    return selected_links


def _render_ulcl_uerouting_config(
    scenario: ScenarioConfig,
    config_dir: Path,
    ulcl_root: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    template_payload = _yaml_load(ulcl_root / "uerouting.yaml")
    info_payload = template_payload.get("info")
    if not isinstance(info_payload, dict):
        info_payload = {
            "version": "1.0.7",
            "description": "Routing information for UE",
        }

    smf_payload = _yaml_load(config_dir / "smfcfg.yaml")
    configuration = smf_payload.get("configuration")
    if not isinstance(configuration, dict):
        raise ValueError("generated smfcfg.yaml must define configuration")

    userplane_information = configuration.get("userplaneInformation")
    if not isinstance(userplane_information, dict):
        raise ValueError("generated smfcfg.yaml must define userplaneInformation")

    userplane_links = userplane_information.get("links")
    if not isinstance(userplane_links, list):
        raise ValueError("generated smfcfg.yaml userplaneInformation must define links")

    flows_by_ue: dict[str, list[FlowConfig]] = {}
    for flow in scenario.flows:
        if flow.ue_name is None:
            continue
        flows_by_ue.setdefault(flow.ue_name, []).append(flow)

    ue_routing_info: dict[str, dict[str, Any]] = {}
    pfd_data_by_app: dict[str, dict[str, Any]] = {}

    for ue in scenario.ues:
        topology_links = _ulcl_topology_links_for_ue(ue.name, scenario, resolved_topology, userplane_links)
        specific_path_nodes = _ulcl_specific_path_nodes(topology_links)
        specific_paths: list[dict[str, Any]] = []

        for flow in sorted(flows_by_ue.get(ue.name, []), key=lambda item: item.flow_id):
            destination = _extract_specific_path_destination(flow)
            if destination and specific_path_nodes:
                specific_paths.append({"dest": destination, "path": list(specific_path_nodes)})

            if not flow.policy_filter:
                continue
            app_payload = pfd_data_by_app.setdefault(
                flow.app_id,
                {
                    "applicationId": flow.app_id,
                    "pfds": [],
                },
            )
            app_payload["pfds"].append(
                {
                    "pfdID": flow.flow_id,
                    "flowDescriptions": [flow.policy_filter],
                }
            )

        ue_routing_info[ue.name] = {
            "members": [ue.supi],
            "topology": topology_links,
            "specificPath": specific_paths,
        }

    uerouting_payload = {
        "info": info_payload,
        "ueRoutingInfo": ue_routing_info,
        "pfdDataForApp": list(pfd_data_by_app.values()),
    }
    _yaml_dump(config_dir / "uerouting.yaml", uerouting_payload)


def _render_single_upf_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    base_root: Path,
) -> None:
    slice_apn_map = _scenario_slice_apns(scenario)
    apns = sorted({apn for slice_apns in slice_apn_map.values() for apn in slice_apns})
    slice_pool_map = {
        slice_config.slice_id: _dnn_pool(index)
        for index, slice_config in enumerate(scenario.slices)
    }

    smf_payload = _yaml_load(base_root / "smfcfg.yaml")
    smf_configuration = smf_payload.get("configuration")
    if not isinstance(smf_configuration, dict):
        raise ValueError("smfcfg.yaml must define configuration")

    sbi = smf_configuration.get("sbi")
    if isinstance(sbi, dict):
        sbi["registerIPv4"] = SMF_CONTROL_IP
        sbi["bindingIPv4"] = SMF_CONTROL_IP

    pfcp = smf_configuration.get("pfcp")
    if isinstance(pfcp, dict):
        pfcp["nodeID"] = SMF_CONTROL_IP
        pfcp["listenAddr"] = SMF_CONTROL_IP
        pfcp["externalAddr"] = SMF_CONTROL_IP

    metrics = smf_configuration.get("metrics")
    if isinstance(metrics, dict):
        metrics["bindingIPv4"] = SMF_CONTROL_IP

    dns_payload = _default_dns_payload(smf_configuration)
    smf_configuration["snssaiInfos"] = [
        {
            "sNssai": {
                "sst": slice_config.sst,
                "sd": slice_config.sd.lower(),
            },
            "dnnInfos": [
                {
                    "dnn": apn,
                    "dns": deepcopy(dns_payload),
                }
                for apn in slice_apn_map[slice_config.slice_id]
            ],
        }
        for slice_config in scenario.slices
    ]

    userplane_information = smf_configuration.get("userplaneInformation")
    if not isinstance(userplane_information, dict):
        raise ValueError("smfcfg.yaml must define userplaneInformation")

    up_nodes = userplane_information.get("upNodes")
    if not isinstance(up_nodes, dict):
        raise ValueError("smfcfg.yaml userplaneInformation must define upNodes")
    existing_upf = next(
        (
            deepcopy(node)
            for node in up_nodes.values()
            if isinstance(node, dict) and node.get("type") == "UPF"
        ),
        {"type": "UPF"},
    )
    existing_interfaces = existing_upf.get("interfaces")
    if not isinstance(existing_interfaces, list) or not existing_interfaces:
        existing_interfaces = [{"interfaceType": "N3"}]
    rendered_interfaces = []
    for interface in existing_interfaces:
        if not isinstance(interface, dict):
            continue
        rendered_interface = deepcopy(interface)
        rendered_interface["endpoints"] = [UPF_CONTROL_IP]
        rendered_interface["networkInstances"] = apns
        rendered_interfaces.append(rendered_interface)

    existing_upf["nodeID"] = UPF_CONTROL_IP
    existing_upf["addr"] = UPF_CONTROL_IP
    existing_upf["interfaces"] = rendered_interfaces
    existing_upf["sNssaiUpfInfos"] = [
        {
            "sNssai": {
                "sst": slice_config.sst,
                "sd": slice_config.sd.lower(),
            },
            "dnnUpfInfoList": [
                {
                    "dnn": apn,
                    "pools": [{"cidr": slice_pool_map[slice_config.slice_id][0]}],
                    "staticPools": [{"cidr": slice_pool_map[slice_config.slice_id][1]}],
                }
                for apn in slice_apn_map[slice_config.slice_id]
            ],
        }
        for slice_config in scenario.slices
    ]
    up_nodes_payload = {
        f"gNB{index}": {"type": "AN", "nodeID": gnb_service_ip(index)}
        for index, _ in enumerate(scenario.gnbs, start=1)
    }
    up_nodes_payload["UPF"] = existing_upf
    userplane_information["upNodes"] = up_nodes_payload
    userplane_information["links"] = [
        {"A": f"gNB{index}", "B": "UPF"}
        for index, _ in enumerate(scenario.gnbs, start=1)
    ]
    _yaml_dump(config_dir / "smfcfg.yaml", smf_payload)

    upf_payload = _yaml_load(base_root / "upfcfg.yaml")
    pfcp_payload = upf_payload.get("pfcp")
    if isinstance(pfcp_payload, dict):
        pfcp_payload["addr"] = UPF_CONTROL_IP
        pfcp_payload["nodeID"] = UPF_CONTROL_IP

    gtpu = upf_payload.get("gtpu")
    if isinstance(gtpu, dict):
        if_list = gtpu.get("ifList")
        if isinstance(if_list, list):
            for interface in if_list:
                if isinstance(interface, dict):
                    interface["addr"] = UPF_CONTROL_IP

    existing_dnn_entries = upf_payload.get("dnnList")
    template_entry = None
    if isinstance(existing_dnn_entries, list):
        for entry in existing_dnn_entries:
            if isinstance(entry, dict):
                template_entry = deepcopy(entry)
                break
    upf_payload["dnnList"] = []
    for slice_config in scenario.slices:
        cidr, _ = slice_pool_map[slice_config.slice_id]
        for apn in slice_apn_map[slice_config.slice_id]:
            dnn_entry = deepcopy(template_entry) if template_entry is not None else {}
            dnn_entry["dnn"] = apn
            dnn_entry["cidr"] = cidr
            upf_payload["dnnList"].append(dnn_entry)

    _yaml_dump(config_dir / "upfcfg.yaml", upf_payload)


def _render_core_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    base_root = Path(scenario.free5gc.config_root)
    _render_amf_config(scenario, config_dir)
    _render_nssf_config(scenario, config_dir)

    if scenario.free5gc.mode == "ulcl":
        ulcl_root = base_root / "ULCL"
        _render_ulcl_smf_config(scenario, config_dir, ulcl_root, resolved_topology)

        for index, upf in enumerate(scenario.upfs, start=1):
            _render_ulcl_upf_config(scenario, config_dir, ulcl_root, upf.name, index)
        _render_ulcl_uerouting_config(scenario, config_dir, ulcl_root, resolved_topology)
        return

    _render_single_upf_configs(scenario, config_dir, base_root)


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


def _render_gnb_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    slice_map = scenario.slice_map()
    base_cfg = _yaml_load(Path(scenario.free5gc.config_root) / "gnbcfg.yaml")

    for index, gnb in enumerate(scenario.gnbs, start=1):
        payload = deepcopy(base_cfg)
        payload["linkIp"] = gnb_service_ip(index)
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
    resolved_topology: ResolvedScenarioTopology,
) -> list[str]:
    ordered_gnbs = list(preferred_gnbs)
    target_gnb = resolved_topology.ue_to_gnb[ue_name]
    if target_gnb not in ordered_gnbs:
        ordered_gnbs.insert(0, target_gnb)
    if not ordered_gnbs:
        ordered_gnbs = [target_gnb]
    return [gnb_service_ip(gnb_indices[gnb_name]) for gnb_name in ordered_gnbs]


def _render_ue_configs(
    scenario: ScenarioConfig,
    config_dir: Path,
    resolved_topology: ResolvedScenarioTopology,
) -> None:
    slice_map = scenario.slice_map()

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
        "session_ref",
        "slice_ref",
        "slice_snssai",
        "dnn",
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
        "policy_filter",
        "precedence",
        "qos_ref",
        "charging_method",
        "quota",
        "unit_cost",
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
            flow.policy_filter,
            flow.precedence,
            flow.qos_ref,
            flow.charging_method,
            flow.quota,
            flow.unit_cost,
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
    clock_file = ns3_dir / "ns3-clock.json"

    for path in (config_dir, subscriber_dir, ns3_dir, state_dir, archive_dir, state_db.parent):
        path.mkdir(parents=True, exist_ok=True)

    initial_topology_file.write_text(
        json.dumps(resolved_topology.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    compose_render = render_compose_for_run(scenario, config_dir, resolved_topology)
    bridge_plans = build_bridge_plan(
        scenario,
        compose_render.service_map,
        resolved_topology,
        inspect_targets=_compose_inspect_targets(compose_render.compose_payload),
    )

    _render_core_configs(scenario, config_dir, resolved_topology)
    _render_gnb_configs(scenario, config_dir, resolved_topology)
    _render_ue_configs(scenario, config_dir, resolved_topology)
    _render_ns3_flow_profiles(scenario, flow_profile_file)

    compose_file = generated_dir / "free5gc-compose.generated.yaml"
    _yaml_dump(compose_file, compose_render.compose_payload)

    subscriber_assets = render_subscriber_bootstrap_assets(
        scenario,
        config_dir,
        compose_render.compose_payload,
        subscriber_dir,
        resolved_topology,
    )

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
        bridge_plans=bridge_plans,
        snapshot_file=snapshot_file,
        clock_file=clock_file,
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