"""Compose rendering helpers for generated free5GC and UERANSIM runs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from bridge.common.ids import safe_name
from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import ResolvedScenarioTopology, resolve_scenario_topology


_MODE_EXCLUDED_SERVICES: dict[str, set[str]] = {
    "single_upf": {"n3iwue", "free5gc-n3iwf", "free5gc-tngf"},
}

SMF_CONTROL_IP = "10.100.200.110"
UPF_CONTROL_IP = "10.100.200.15"
AMF_CONTROL_IP = "10.100.200.16"
_GNB_SERVICE_IP_BASE = 101
_UPF_SERVICE_IP_BASE = 15


@dataclass(slots=True)
class ComposeRenderResult:
    compose_payload: dict[str, Any]
    service_map: dict[str, dict[str, str]]
    core_services: list[str]
    ran_services: list[str]


def gnb_service_ip(index: int) -> str:
    return f"10.100.200.{_GNB_SERVICE_IP_BASE + index - 1}"


def upf_service_ip(index: int) -> str:
    host_octet = _UPF_SERVICE_IP_BASE if index == 1 else _UPF_SERVICE_IP_BASE + index
    return f"10.100.200.{host_octet}"


def _absolutize_volume(item: str, compose_dir: Path) -> str:
    if ":" not in item:
        return item
    source, remainder = item.split(":", 1)
    if source.startswith("./") or source.startswith("../"):
        source = str((compose_dir / source).resolve())
    return f"{source}:{remainder}"


def _normalize_compose(payload: dict[str, Any], compose_dir: Path) -> dict[str, Any]:
    normalized = deepcopy(payload)
    for service in normalized.get("services", {}).values():
        volumes = service.get("volumes")
        if isinstance(volumes, list):
            service["volumes"] = [
                _absolutize_volume(item, compose_dir) if isinstance(item, str) else item
                for item in volumes
            ]
    return normalized


def _ensure_network_entry(service: dict[str, Any], network_name: str) -> dict[str, Any]:
    networks = service.setdefault("networks", {})
    entry = networks.get(network_name)
    if not isinstance(entry, dict):
        entry = {}
    networks[network_name] = entry
    return entry


def _replace_volume_source(service: dict[str, Any], container_path: str, host_path: Path) -> None:
    volumes = service.get("volumes")
    if not isinstance(volumes, list):
        return

    updated: list[Any] = []
    for item in volumes:
        if isinstance(item, str) and ":" in item:
            source, remainder = item.split(":", 1)
            if remainder == container_path or remainder.startswith(f"{container_path}:"):
                suffix = remainder[len(container_path) :]
                updated.append(f"{host_path}:{container_path}{suffix}")
                continue
            updated.append(f"{source}:{remainder}")
            continue
        updated.append(item)

    service["volumes"] = updated


def _gnb_service_name(index: int, gnb_name: str) -> str:
    return "ueransim" if index == 1 else f"ueransim-{safe_name(gnb_name)}"


def _ue_service_name(ue_name: str) -> str:
    return f"ue-{safe_name(ue_name)}"


def _upf_service_name(upf_name: str) -> str:
    return f"free5gc-{safe_name(upf_name)}"


def _filter_depends_on(service: dict[str, Any], known_services: set[str]) -> None:
    depends_on = service.get("depends_on")
    if isinstance(depends_on, list):
        filtered = [item for item in depends_on if item in known_services]
        if filtered:
            service["depends_on"] = filtered
        else:
            service.pop("depends_on", None)
        return
    if isinstance(depends_on, dict):
        filtered = {key: value for key, value in depends_on.items() if key in known_services}
        if filtered:
            service["depends_on"] = filtered
        else:
            service.pop("depends_on", None)


def _prune_services_for_mode(services: dict[str, Any], mode: str) -> None:
    for service_name in _MODE_EXCLUDED_SERVICES.get(mode, set()):
        services.pop(service_name, None)

    known_services = set(services)
    for service in services.values():
        _filter_depends_on(service, known_services)


def render_compose_for_run(
    scenario: ScenarioConfig,
    generated_config_dir: Path,
    resolved_topology: ResolvedScenarioTopology | None = None,
) -> ComposeRenderResult:
    resolved_topology = resolved_topology or resolve_scenario_topology(scenario)
    compose_path = Path(scenario.free5gc.compose_file)
    with compose_path.open("r", encoding="utf-8") as handle:
        raw_compose = yaml.safe_load(handle)
    compose = _normalize_compose(raw_compose, compose_path.parent)

    services = compose.setdefault("services", {})
    if "ueransim" not in services:
        raise ValueError("base compose file must define a ueransim service")

    _prune_services_for_mode(services, scenario.free5gc.mode)

    if "free5gc-amf" in services:
        amf_service = services["free5gc-amf"]
        _replace_volume_source(amf_service, "/free5gc/config/amfcfg.yaml", generated_config_dir / "amfcfg.yaml")
        _ensure_network_entry(amf_service, "privnet")["ipv4_address"] = AMF_CONTROL_IP

    if "free5gc-nssf" in services:
        nssf_service = services["free5gc-nssf"]
        _replace_volume_source(nssf_service, "/free5gc/config/nssfcfg.yaml", generated_config_dir / "nssfcfg.yaml")

    if "free5gc-smf" in services:
        smf_service = services["free5gc-smf"]
        _replace_volume_source(smf_service, "/free5gc/config/smfcfg.yaml", generated_config_dir / "smfcfg.yaml")
        if scenario.free5gc.mode == "ulcl":
            _replace_volume_source(
                smf_service,
                "/free5gc/config/uerouting.yaml",
                generated_config_dir / "uerouting.yaml",
            )
        _ensure_network_entry(smf_service, "privnet")["ipv4_address"] = SMF_CONTROL_IP

    if scenario.free5gc.mode == "single_upf" and "free5gc-upf" in services:
        upf_service = services["free5gc-upf"]
        _replace_volume_source(upf_service, "/free5gc/config/upfcfg.yaml", generated_config_dir / "upfcfg.yaml")
        _ensure_network_entry(upf_service, "privnet")["ipv4_address"] = UPF_CONTROL_IP
    elif scenario.free5gc.mode == "ulcl":
        for index, upf in enumerate(scenario.upfs, start=1):
            service_name = _upf_service_name(upf.name)
            if service_name not in services:
                continue
            upf_service = services[service_name]
            _replace_volume_source(
                upf_service,
                "/free5gc/config/upfcfg.yaml",
                generated_config_dir / f"{upf.name}-upfcfg.yaml",
            )
            _ensure_network_entry(upf_service, "privnet")["ipv4_address"] = upf_service_ip(index)

    gnb_template = deepcopy(services["ueransim"])
    gnb_template["volumes"] = []

    service_map = {"gnb": {}, "ue": {}, "upf": {}}

    if scenario.free5gc.mode == "single_upf" and scenario.upfs and "free5gc-upf" in services:
        service_map["upf"][scenario.upfs[0].name] = "free5gc-upf"
    elif scenario.free5gc.mode == "ulcl":
        for upf in scenario.upfs:
            service_name = _upf_service_name(upf.name)
            if service_name in services:
                service_map["upf"][upf.name] = service_name

    for index, gnb in enumerate(scenario.gnbs, start=1):
        service_name = _gnb_service_name(index, gnb.name)
        service = deepcopy(gnb_template)
        service["container_name"] = service_name
        service["command"] = "./nr-gnb -c ./config/gnbcfg.yaml"
        service["restart"] = "unless-stopped"
        service["volumes"] = [
            f"{generated_config_dir / f'{gnb.name}-gnbcfg.yaml'}:/ueransim/config/gnbcfg.yaml"
        ]

        aliases = [gnb.alias]
        if index == 1:
            aliases.append("gnb.free5gc.org")
        service["networks"] = {
            "privnet": {
                "aliases": aliases,
                "ipv4_address": gnb_service_ip(index),
            }
        }

        services[service_name] = service
        service_map["gnb"][gnb.name] = service_name

    if len(scenario.gnbs) > 1 and "ueransim" in services and "ueransim" not in service_map["gnb"].values():
        services.pop("ueransim", None)

    known_services = set(services)
    gnb_depends_on = gnb_template.get("depends_on", [])
    if isinstance(gnb_depends_on, dict):
        gnb_depends_on = list(gnb_depends_on)
    gnb_depends_on = [item for item in gnb_depends_on if item in known_services]

    ue_start_offsets: dict[str, int] = {}

    for index, ue in enumerate(scenario.ues, start=1):
        service_name = _ue_service_name(ue.name)
        target_gnb = resolved_topology.ue_to_gnb[ue.name]
        start_offset = ue_start_offsets.get(target_gnb, 0)
        ue_start_offsets[target_gnb] = start_offset + 1
        aliases = [f"{safe_name(ue.name)}.ue.free5gc.org"]
        if index == 1:
            aliases.append("ue.free5gc.org")
        command = "./nr-ue -c ./config/uecfg.yaml"
        if start_offset > 0:
            command = f"sh -c 'sleep {start_offset} && ./nr-ue -c ./config/uecfg.yaml'"
        service = {
            "container_name": service_name,
            "image": gnb_template["image"],
            "command": command,
            "restart": "unless-stopped",
            "volumes": [
                f"{generated_config_dir / f'{ue.name}-uecfg.yaml'}:/ueransim/config/uecfg.yaml"
            ],
            "cap_add": deepcopy(gnb_template.get("cap_add", ["NET_ADMIN"])),
            "devices": deepcopy(gnb_template.get("devices", ["/dev/net/tun"])),
            "networks": {
                "privnet": {
                    "aliases": aliases,
                }
            },
            "depends_on": [service_map["gnb"][target_gnb], *gnb_depends_on],
        }
        _filter_depends_on(service, set(services) | {service_name})
        services[service_name] = service
        service_map["ue"][ue.name] = service_name

    ran_services = [
        *service_map["gnb"].values(),
        *service_map["ue"].values(),
    ]
    core_services = [name for name in services if name not in set(ran_services)]

    return ComposeRenderResult(
        compose_payload=compose,
        service_map=service_map,
        core_services=core_services,
        ran_services=ran_services,
    )