"""Bridge script generation for optional inline tap harness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from adapters.free5gc_ueransim.compose_override import UPF_CONTROL_IP, gnb_service_ip, upf_service_ip
from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import ResolvedScenarioTopology, resolve_scenario_topology


@dataclass(slots=True)
class BridgeInterfacePlan:
    link_index: int
    gnb_name: str
    gnb_service: str
    upf_name: str
    upf_service: str
    gnb_tap: str
    upf_tap: str
    gnb_bridge: str
    upf_bridge: str
    gnb_host_veth: str
    upf_host_veth: str
    gnb_ns_if: str
    upf_ns_if: str
    gnb_ip: str
    upf_ip: str
    gnb_route_target: str
    upf_route_target: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def _short_ifname(prefix: str, index: int) -> str:
    return f"{prefix}{index}"[:15]


def build_bridge_plan(
    scenario: ScenarioConfig,
    service_map: dict[str, dict[str, str]],
    resolved_topology: ResolvedScenarioTopology | None = None,
    inspect_targets: dict[str, str] | None = None,
) -> list[BridgeInterfacePlan]:
    resolved_topology = resolved_topology or resolve_scenario_topology(scenario)
    plans: list[BridgeInterfacePlan] = []
    inspect_targets = inspect_targets or {}
    upf_index_by_name = {upf.name: index for index, upf in enumerate(scenario.upfs, start=1)}
    for index, gnb in enumerate(scenario.gnbs, start=1):
        target_upf = resolved_topology.gnb_to_upf[gnb.name]
        gnb_service_name = service_map["gnb"][gnb.name]
        upf_service_name = service_map["upf"][target_upf]
        gnb_service = inspect_targets.get(gnb_service_name, gnb_service_name)
        upf_service = inspect_targets.get(upf_service_name, upf_service_name)
        route_target = UPF_CONTROL_IP
        if scenario.free5gc.mode == "ulcl":
            route_target = upf_service_ip(upf_index_by_name[target_upf])
        plans.append(
            BridgeInterfacePlan(
                link_index=index,
                gnb_name=gnb.name,
                gnb_service=gnb_service,
                upf_name=target_upf,
                upf_service=upf_service,
                gnb_tap=_short_ifname("tgnb", index),
                upf_tap=_short_ifname("tupf", index),
                gnb_bridge=_short_ifname("brg", index),
                upf_bridge=_short_ifname("bru", index),
                gnb_host_veth=_short_ifname("vgh", index),
                upf_host_veth=_short_ifname("vuh", index),
                gnb_ns_if=_short_ifname("esg", index),
                upf_ns_if=_short_ifname("esu", index),
                gnb_ip=f"10.210.{index}.1",
                upf_ip=f"10.210.{index}.2",
                gnb_route_target=gnb_service_ip(index),
                upf_route_target=route_target,
            )
        )
    return plans


def render_bridge_script(plans: list[BridgeInterfacePlan], output_path: Path) -> None:
    cleanup_names = []
    for plan in plans:
        cleanup_names.extend(
            [
                plan.gnb_host_veth,
                plan.upf_host_veth,
                plan.gnb_bridge,
                plan.upf_bridge,
                plan.gnb_tap,
                plan.upf_tap,
            ]
        )

    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "delete_link() {",
        "  if ip link show \"$1\" >/dev/null 2>&1; then",
        "    ip link del \"$1\"",
        "  fi",
        "}",
        "",
        "modprobe br_netfilter >/dev/null 2>&1 || true",
        "for name in " + " ".join(f'"{name}"' for name in cleanup_names) + "; do",
        "  delete_link \"$name\"",
        "done",
        "",
    ]

    for plan in plans:
        lines.extend(
            [
                f"gnb_pid_{plan.link_index}=$(docker inspect --format '{{{{ .State.Pid }}}}' {plan.gnb_service})",
                f"upf_pid_{plan.link_index}=$(docker inspect --format '{{{{ .State.Pid }}}}' {plan.upf_service})",
                f"ip tuntap add mode tap {plan.gnb_tap}",
                f"ip tuntap add mode tap {plan.upf_tap}",
                f"ip link add name {plan.gnb_bridge} type bridge",
                f"ip link add name {plan.upf_bridge} type bridge",
                f"ip link set {plan.gnb_tap} master {plan.gnb_bridge}",
                f"ip link set {plan.upf_tap} master {plan.upf_bridge}",
                f"ip link set {plan.gnb_tap} up promisc on",
                f"ip link set {plan.upf_tap} up promisc on",
                f"ip link set {plan.gnb_bridge} up",
                f"ip link set {plan.upf_bridge} up",
                f"ip link add {plan.gnb_host_veth} type veth peer name tmpg{plan.link_index}",
                f"ip link set {plan.gnb_host_veth} master {plan.gnb_bridge}",
                f"ip link set {plan.gnb_host_veth} up",
                f"ip link set tmpg{plan.link_index} netns $gnb_pid_{plan.link_index}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip link set tmpg{plan.link_index} name {plan.gnb_ns_if}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip link set {plan.gnb_ns_if} up",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip addr add {plan.gnb_ip}/30 dev {plan.gnb_ns_if}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip route replace {plan.upf_route_target}/32 dev {plan.gnb_ns_if}",
                f"ip link add {plan.upf_host_veth} type veth peer name tmpu{plan.link_index}",
                f"ip link set {plan.upf_host_veth} master {plan.upf_bridge}",
                f"ip link set {plan.upf_host_veth} up",
                f"ip link set tmpu{plan.link_index} netns $upf_pid_{plan.link_index}",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip link set tmpu{plan.link_index} name {plan.upf_ns_if}",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip link set {plan.upf_ns_if} up",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip addr add {plan.upf_ip}/30 dev {plan.upf_ns_if}",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip route replace {plan.gnb_route_target}/32 dev {plan.upf_ns_if}",
                "",
            ]
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output_path.chmod(0o755)