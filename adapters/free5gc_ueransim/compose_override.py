"""Compose rendering helpers for generated free5GC and UERANSIM runs."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from bridge.common.ids import safe_name
from bridge.common.scenario import ScenarioConfig


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


def _gnb_service_name(index: int, gnb_name: str) -> str:
    return "ueransim" if index == 1 else f"ueransim-{safe_name(gnb_name)}"


def _ue_service_name(ue_name: str) -> str:
    return f"ue-{safe_name(ue_name)}"


def render_compose_for_run(
    scenario: ScenarioConfig,
    generated_config_dir: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, str]]]:
    compose_path = Path(scenario.free5gc.compose_file)
    with compose_path.open("r", encoding="utf-8") as handle:
        raw_compose = yaml.safe_load(handle)
    compose = _normalize_compose(raw_compose, compose_path.parent)

    services = compose.setdefault("services", {})
    if "ueransim" not in services:
        raise ValueError("base compose file must define a ueransim service")

    gnb_template = deepcopy(services["ueransim"])
    gnb_template["volumes"] = []

    service_map = {"gnb": {}, "ue": {}}

    for index, gnb in enumerate(scenario.gnbs, start=1):
        service_name = _gnb_service_name(index, gnb.name)
        service = deepcopy(gnb_template)
        service["container_name"] = service_name
        service["command"] = "./nr-gnb -c ./config/gnbcfg.yaml"
        service["volumes"] = [
            f"{generated_config_dir / f'{gnb.name}-gnbcfg.yaml'}:/ueransim/config/gnbcfg.yaml"
        ]

        aliases = [gnb.alias]
        if index == 1:
            aliases.append("gnb.free5gc.org")
        service.setdefault("networks", {})["privnet"] = {"aliases": aliases}

        services[service_name] = service
        service_map["gnb"][gnb.name] = service_name

    if len(scenario.gnbs) > 1 and "ueransim" in services and "ueransim" not in service_map["gnb"].values():
        services.pop("ueransim", None)

    for ue in scenario.ues:
        service_name = _ue_service_name(ue.name)
        service = {
            "container_name": service_name,
            "image": gnb_template["image"],
            "command": "./nr-ue -c ./config/uecfg.yaml",
            "volumes": [
                f"{generated_config_dir / f'{ue.name}-uecfg.yaml'}:/ueransim/config/uecfg.yaml"
            ],
            "cap_add": deepcopy(gnb_template.get("cap_add", ["NET_ADMIN"])),
            "devices": deepcopy(gnb_template.get("devices", ["/dev/net/tun"])),
            "networks": {
                "privnet": {
                    "aliases": [f"{safe_name(ue.name)}.ue.free5gc.org"],
                }
            },
            "depends_on": [service_map["gnb"][ue.gnb]],
        }
        services[service_name] = service
        service_map["ue"][ue.name] = service_name

    return compose, service_map