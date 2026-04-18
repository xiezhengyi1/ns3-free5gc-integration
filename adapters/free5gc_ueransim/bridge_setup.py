"""Bridge script generation for optional inline tap harness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import ResolvedScenarioTopology, resolve_scenario_topology


@dataclass(slots=True)
class BridgeInterfacePlan:
    link_index: int
    gnb_name: str
    gnb_service: str
    ue_name: str
    ue_service: str
    gnb_tap: str
    ue_tap: str
    gnb_bridge: str
    ue_bridge: str
    gnb_host_veth: str
    ue_host_veth: str
    gnb_ns_if: str
    ue_ns_if: str
    gnb_ip: str
    ue_ip: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def _short_ifname(prefix: str, index: int) -> str:
    return f"{prefix}{index}"[:15]


def build_bridge_plan(
    scenario: ScenarioConfig,
    service_map: dict[str, dict[str, str]],
    resolved_topology: ResolvedScenarioTopology | None = None,
) -> list[BridgeInterfacePlan]:
    resolved_topology = resolved_topology or resolve_scenario_topology(scenario)
    plans: list[BridgeInterfacePlan] = []
    for index, ue in enumerate(scenario.ues, start=1):
        target_gnb = resolved_topology.ue_to_gnb[ue.name]
        gnb_service = service_map["gnb"][target_gnb]
        ue_service = service_map["ue"][ue.name]
        plans.append(
            BridgeInterfacePlan(
                link_index=index,
                gnb_name=target_gnb,
                gnb_service=gnb_service,
                ue_name=ue.name,
                ue_service=ue_service,
                gnb_tap=_short_ifname("tgnb", index),
                ue_tap=_short_ifname("tue", index),
                gnb_bridge=_short_ifname("brg", index),
                ue_bridge=_short_ifname("bru", index),
                gnb_host_veth=_short_ifname("vgh", index),
                ue_host_veth=_short_ifname("vuh", index),
                gnb_ns_if=_short_ifname("esg", index),
                ue_ns_if=_short_ifname("esu", index),
                gnb_ip=f"10.210.{index}.1",
                ue_ip=f"10.210.{index}.2",
            )
        )
    return plans


def render_bridge_script(plans: list[BridgeInterfacePlan], output_path: Path) -> None:
    cleanup_names = []
    for plan in plans:
        cleanup_names.extend(
            [
                plan.gnb_host_veth,
                plan.ue_host_veth,
                plan.gnb_bridge,
                plan.ue_bridge,
                plan.gnb_tap,
                plan.ue_tap,
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
                f"ue_pid_{plan.link_index}=$(docker inspect --format '{{{{ .State.Pid }}}}' {plan.ue_service})",
                f"ip tuntap add mode tap {plan.gnb_tap}",
                f"ip tuntap add mode tap {plan.ue_tap}",
                f"ip link add name {plan.gnb_bridge} type bridge",
                f"ip link add name {plan.ue_bridge} type bridge",
                f"ip link set {plan.gnb_tap} master {plan.gnb_bridge}",
                f"ip link set {plan.ue_tap} master {plan.ue_bridge}",
                f"ip link set {plan.gnb_tap} up promisc on",
                f"ip link set {plan.ue_tap} up promisc on",
                f"ip link set {plan.gnb_bridge} up",
                f"ip link set {plan.ue_bridge} up",
                f"ip link add {plan.gnb_host_veth} type veth peer name tmpg{plan.link_index}",
                f"ip link set {plan.gnb_host_veth} master {plan.gnb_bridge}",
                f"ip link set {plan.gnb_host_veth} up",
                f"ip link set tmpg{plan.link_index} netns $gnb_pid_{plan.link_index}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip link set tmpg{plan.link_index} name {plan.gnb_ns_if}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip link set {plan.gnb_ns_if} up",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip addr add {plan.gnb_ip}/30 dev {plan.gnb_ns_if}",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip route replace {plan.ue_ip}/32 dev {plan.gnb_ns_if}",
                f"ip link add {plan.ue_host_veth} type veth peer name tmpu{plan.link_index}",
                f"ip link set {plan.ue_host_veth} master {plan.ue_bridge}",
                f"ip link set {plan.ue_host_veth} up",
                f"ip link set tmpu{plan.link_index} netns $ue_pid_{plan.link_index}",
                f"nsenter -t $ue_pid_{plan.link_index} -n ip link set tmpu{plan.link_index} name {plan.ue_ns_if}",
                f"nsenter -t $ue_pid_{plan.link_index} -n ip link set {plan.ue_ns_if} up",
                f"nsenter -t $ue_pid_{plan.link_index} -n ip addr add {plan.ue_ip}/30 dev {plan.ue_ns_if}",
                f"nsenter -t $ue_pid_{plan.link_index} -n ip route replace {plan.gnb_ip}/32 dev {plan.ue_ns_if}",
                "",
            ]
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output_path.chmod(0o755)