"""Bridge script generation for optional inline tap harness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from adapters.free5gc_ueransim.compose_override import N3NetworkPlan
from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import ResolvedScenarioTopology


@dataclass(slots=True)
class BridgeInterfacePlan:
    link_index: int
    segment_index: int
    gnb_name: str
    gnb_service: str
    upf_name: str
    upf_service: str
    gnb_tap: str
    upf_tap: str
    gnb_n3_ip: str
    upf_n3_ip: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def _short_ifname(prefix: str, index: int) -> str:
    return f"{prefix}{index}"[:15]


def build_bridge_plan(
    scenario: ScenarioConfig,
    service_map: dict[str, dict[str, str]],
    n3_network_plan: N3NetworkPlan | None = None,
    resolved_topology: ResolvedScenarioTopology | None = None,
    inspect_targets: dict[str, str] | None = None,
) -> list[BridgeInterfacePlan]:
    if resolved_topology is None:
        raise ValueError("resolved_topology is required to build bridge plans")
    if n3_network_plan is None:
        raise ValueError("n3_network_plan is required to build inline bridge plans")

    plans: list[BridgeInterfacePlan] = []
    inspect_targets = inspect_targets or {}
    upf_segment_indices = {upf.name: index for index, upf in enumerate(scenario.upfs, start=1)}
    link_index = 0
    for gnb_index, gnb in enumerate(scenario.gnbs, start=1):
        for target_upf in resolved_topology.gnb_to_upfs[gnb.name]:
            link_index += 1
            gnb_service_name = service_map["gnb"][gnb.name]
            upf_service_name = service_map["upf"][target_upf]
            plans.append(
                BridgeInterfacePlan(
                    link_index=link_index,
                    segment_index=upf_segment_indices[target_upf],
                    gnb_name=gnb.name,
                    gnb_service=inspect_targets.get(gnb_service_name, gnb_service_name),
                    upf_name=target_upf,
                    upf_service=inspect_targets.get(upf_service_name, upf_service_name),
                    gnb_tap=_short_ifname("tgnb", gnb_index),
                    upf_tap=_short_ifname("tupf", upf_segment_indices[target_upf]),
                    gnb_n3_ip=n3_network_plan.gnb_ips[gnb.name],
                    upf_n3_ip=n3_network_plan.upf_ips[target_upf],
                )
            )
    return plans


def render_bridge_script(plans: list[BridgeInterfacePlan], output_path: Path) -> None:
    tap_names = list(dict.fromkeys([tap for plan in plans for tap in (plan.gnb_tap, plan.upf_tap)]))
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
        "delete_tc_qdisc() {",
        "  local ifname=$1",
        "  tc qdisc del dev \"$ifname\" clsact >/dev/null 2>&1 || true",
        "}",
        "",
        "resolve_container_pid() {",
        "  local target=$1",
        "  docker inspect --format '{{ .State.Pid }}' \"$target\" 2>/dev/null || true",
        "}",
        "",
        "wait_for_container_pid() {",
        "  local target=$1",
        "  local pid=''",
        "  for _ in $(seq 1 50); do",
        "    pid=$(resolve_container_pid \"$target\")",
        "    if [[ -n \"$pid\" && \"$pid\" != \"0\" ]] && nsenter -t \"$pid\" -n true >/dev/null 2>&1; then",
        "      printf '%s\\n' \"$pid\"",
        "      return 0",
        "    fi",
        "    sleep 0.2",
        "  done",
        "  echo \"timed out waiting for live netns: $target\" >&2",
        "  return 1",
        "}",
        "",
        "resolve_ns_ifname_by_ipv4() {",
        "  local pid=$1",
        "  local ipv4_addr=$2",
        "  local ifname",
        "  ifname=$(nsenter -t \"$pid\" -n ip -o -4 addr show | awk -v ip=\"$ipv4_addr\" '$4 ~ (\"^\" ip \"/\") { print $2; exit }')",
        "  if [[ -z \"$ifname\" ]]; then",
        "    echo \"unable to resolve container interface for pid=$pid ipv4=$ipv4_addr\" >&2",
        "    return 1",
        "  fi",
        "  printf '%s\\n' \"$ifname\"",
        "}",
        "",
        "resolve_host_peer_ifname() {",
        "  local pid=$1",
        "  local ifname=$2",
        "  local peer_index",
        "  local host_if",
        "  peer_index=$(nsenter -t \"$pid\" -n -m cat \"/sys/class/net/$ifname/iflink\")",
        "  host_if=$(ip -o link | awk -F': ' -v idx=\"$peer_index\" '$1 == idx { print $2; exit }' | sed 's/@.*//')",
        "  if [[ -z \"$host_if\" ]]; then",
        "    echo \"unable to resolve host peer for pid=$pid ifname=$ifname iflink=$peer_index\" >&2",
        "    return 1",
        "  fi",
        "  printf '%s\\n' \"$host_if\"",
        "}",
        "",
        "disable_ipv6() {",
        "  local ifname=$1",
        "  if [[ -e /proc/sys/net/ipv6/conf/${ifname}/disable_ipv6 ]]; then",
        "    sysctl -qw net.ipv6.conf.${ifname}.disable_ipv6=1 || true",
        "  fi",
        "}",
        "",
        "disable_offload() {",
        "  local ifname=$1",
        "  ethtool -K \"$ifname\" rx off tx off tso off gso off gro off lro off sg off txvlan off rxvlan off || true",
        "}",
        "",
        "set_promisc() {",
        "  local ifname=$1",
        "  ip link set \"$ifname\" up promisc on",
        "}",
        "",
        "attach_redirect_pair() {",
        "  local left=$1",
        "  local right=$2",
        "  tc qdisc replace dev \"$left\" clsact",
        "  tc qdisc replace dev \"$right\" clsact",
        "  tc filter replace dev \"$left\" ingress prio 1 protocol all matchall action mirred egress redirect dev \"$right\"",
        "  tc filter replace dev \"$right\" ingress prio 1 protocol all matchall action mirred egress redirect dev \"$left\"",
        "}",
        "",
    ]

    if tap_names:
        lines.extend(
            [
                "for name in " + " ".join(f'"{name}"' for name in tap_names) + "; do",
                "  delete_tc_qdisc \"$name\"",
                "  delete_link \"$name\"",
                "done",
                "",
            ]
        )

    endpoints: list[tuple[str, int, str, str, str, str]] = []
    seen_gnbs: set[str] = set()
    seen_upfs: set[str] = set()
    for plan in plans:
        if plan.gnb_name not in seen_gnbs:
            endpoints.append(
                (
                    "gnb",
                    len(seen_gnbs) + 1,
                    plan.gnb_name,
                    plan.gnb_service,
                    plan.gnb_tap,
                    plan.gnb_n3_ip,
                )
            )
            seen_gnbs.add(plan.gnb_name)
        if plan.upf_name not in seen_upfs:
            endpoints.append(
                (
                    "upf",
                    plan.segment_index,
                    plan.upf_name,
                    plan.upf_service,
                    plan.upf_tap,
                    plan.upf_n3_ip,
                )
            )
            seen_upfs.add(plan.upf_name)

    for role, endpoint_index, name, service, tap, n3_ip in endpoints:
        pid_variable = f"{role}_pid_{endpoint_index}"
        n3_if_variable = f"{role}_n3_if_{endpoint_index}"
        host_if_variable = f"{role}_host_if_{endpoint_index}"
        lines.extend(
            [
                f"{pid_variable}=$(wait_for_container_pid {service})",
                f"{n3_if_variable}=$(resolve_ns_ifname_by_ipv4 ${pid_variable} {n3_ip})",
                f"{host_if_variable}=$(resolve_host_peer_ifname ${pid_variable} \"${n3_if_variable}\")",
                f"delete_tc_qdisc \"${host_if_variable}\"",
                f"ip tuntap add mode tap {tap}",
                f"set_promisc {tap}",
                f"set_promisc \"${host_if_variable}\"",
                f"disable_ipv6 {tap}",
                f"disable_ipv6 \"${host_if_variable}\"",
                f"disable_offload {tap}",
                f"disable_offload \"${host_if_variable}\"",
                f"ip link set \"${host_if_variable}\" nomaster || true",
                f"attach_redirect_pair \"${host_if_variable}\" {tap}",
                f"echo 'bridge-inline endpoint={role} name={name} ip={n3_ip} tap={tap}'",
                f"nsenter -t ${pid_variable} -n ip -4 addr show dev \"${n3_if_variable}\"",
                "",
            ]
        )

    for plan in plans:
        lines.append(
            f"echo 'bridge-inline link={plan.link_index} segment={plan.segment_index} "
            f"gnb={plan.gnb_name} gnb_ip={plan.gnb_n3_ip} upf={plan.upf_name} upf_ip={plan.upf_n3_ip}'"
        )
    if plans:
        lines.append("")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output_path.chmod(0o755)


def render_bridge_probe_script(plans: list[BridgeInterfacePlan], output_path: Path) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "sleep_seconds=${1:-8}",
        "sleep \"$sleep_seconds\"",
        "",
        "resolve_container_pid() {",
        "  local target=$1",
        "  docker inspect --format '{{ .State.Pid }}' \"$target\" 2>/dev/null || true",
        "}",
        "",
        "wait_for_container_pid() {",
        "  local target=$1",
        "  local pid=''",
        "  for _ in $(seq 1 50); do",
        "    pid=$(resolve_container_pid \"$target\")",
        "    if [[ -n \"$pid\" && \"$pid\" != \"0\" ]] && nsenter -t \"$pid\" -n true >/dev/null 2>&1; then",
        "      printf '%s\\n' \"$pid\"",
        "      return 0",
        "    fi",
        "    sleep 0.2",
        "  done",
        "  echo \"timed out waiting for live netns: $target\" >&2",
        "  return 1",
        "}",
        "",
        "resolve_ns_ifname_by_ipv4() {",
        "  local pid=$1",
        "  local ipv4_addr=$2",
        "  local ifname",
        "  ifname=$(nsenter -t \"$pid\" -n ip -o -4 addr show | awk -v ip=\"$ipv4_addr\" '$4 ~ (\"^\" ip \"/\") { print $2; exit }')",
        "  if [[ -z \"$ifname\" ]]; then",
        "    echo \"unable to resolve container interface for pid=$pid ipv4=$ipv4_addr\" >&2",
        "    return 1",
        "  fi",
        "  printf '%s\\n' \"$ifname\"",
        "}",
        "",
        "resolve_host_peer_ifname() {",
        "  local pid=$1",
        "  local ifname=$2",
        "  local peer_index",
        "  local host_if",
        "  peer_index=$(nsenter -t \"$pid\" -n -m cat \"/sys/class/net/$ifname/iflink\")",
        "  host_if=$(ip -o link | awk -F': ' -v idx=\"$peer_index\" '$1 == idx { print $2; exit }' | sed 's/@.*//')",
        "  if [[ -z \"$host_if\" ]]; then",
        "    echo \"unable to resolve host peer for pid=$pid ifname=$ifname iflink=$peer_index\" >&2",
        "    return 1",
        "  fi",
        "  printf '%s\\n' \"$host_if\"",
        "}",
        "",
        "run_capture() {",
        "  local label=$1",
        "  shift",
        "  echo \"[capture-start] ${label}\"",
        "  \"$@\" 2>&1 | sed \"s/^/[${label}] /\" || true",
        "}",
        "",
    ]

    for plan in plans:
        lines.extend(
            [
                f"gnb_pid_{plan.link_index}=$(wait_for_container_pid {plan.gnb_service})",
                f"upf_pid_{plan.link_index}=$(wait_for_container_pid {plan.upf_service})",
                f"gnb_n3_if_{plan.link_index}=$(resolve_ns_ifname_by_ipv4 $gnb_pid_{plan.link_index} {plan.gnb_n3_ip})",
                f"upf_n3_if_{plan.link_index}=$(resolve_ns_ifname_by_ipv4 $upf_pid_{plan.link_index} {plan.upf_n3_ip})",
                f"gnb_host_if_{plan.link_index}=$(resolve_host_peer_ifname $gnb_pid_{plan.link_index} \"$gnb_n3_if_{plan.link_index}\")",
                f"upf_host_if_{plan.link_index}=$(resolve_host_peer_ifname $upf_pid_{plan.link_index} \"$upf_n3_if_{plan.link_index}\")",
                (
                    f"echo 'ns3-bridge-probe link={plan.link_index} segment={plan.segment_index} "
                    f"gnb={plan.gnb_name} upf={plan.upf_name}'"
                ),
                f"tc qdisc show dev \"$gnb_host_if_{plan.link_index}\" || true",
                f"tc qdisc show dev {plan.gnb_tap} || true",
                f"tc filter show dev \"$gnb_host_if_{plan.link_index}\" ingress || true",
                f"tc filter show dev {plan.gnb_tap} ingress || true",
                f"tc qdisc show dev \"$upf_host_if_{plan.link_index}\" || true",
                f"tc qdisc show dev {plan.upf_tap} || true",
                f"tc filter show dev \"$upf_host_if_{plan.link_index}\" ingress || true",
                f"tc filter show dev {plan.upf_tap} ingress || true",
                f"ip -d link show dev {plan.gnb_tap} || true",
                f"ip -d link show dev {plan.upf_tap} || true",
                f"ip -d link show dev \"$gnb_host_if_{plan.link_index}\" || true",
                f"ip -d link show dev \"$upf_host_if_{plan.link_index}\" || true",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip -d link show dev \"$gnb_n3_if_{plan.link_index}\" || true",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip -d link show dev \"$upf_n3_if_{plan.link_index}\" || true",
                f"nsenter -t $gnb_pid_{plan.link_index} -n ip -4 addr show dev \"$gnb_n3_if_{plan.link_index}\" || true",
                f"nsenter -t $upf_pid_{plan.link_index} -n ip -4 addr show dev \"$upf_n3_if_{plan.link_index}\" || true",
                f"nsenter -t $gnb_pid_{plan.link_index} -n sh -c \"ss -unap | grep -E '2152|38412' || true\"",
                f"nsenter -t $upf_pid_{plan.link_index} -n sh -c \"ss -unap | grep 2152 || true\"",
                (
                    f"run_capture link{plan.link_index}-tgnb timeout 3 tcpdump -eni {plan.gnb_tap} "
                    "'arp or icmp or udp port 2152' &"
                ),
                (
                    f"run_capture link{plan.link_index}-tupf timeout 3 tcpdump -eni {plan.upf_tap} "
                    "'arp or icmp or udp port 2152' &"
                ),
                (
                    f"run_capture link{plan.link_index}-gnb-host timeout 3 tcpdump -eni "
                    f"\"$gnb_host_if_{plan.link_index}\" 'arp or icmp or udp port 2152' &"
                ),
                (
                    f"run_capture link{plan.link_index}-upf-host timeout 3 tcpdump -eni "
                    f"\"$upf_host_if_{plan.link_index}\" 'arp or icmp or udp port 2152' &"
                ),
                (
                    f"run_capture link{plan.link_index}-gnb-n3 timeout 3 nsenter -t $gnb_pid_{plan.link_index} -n "
                    f"tcpdump -eni \"$gnb_n3_if_{plan.link_index}\" 'arp or icmp or udp port 2152' &"
                ),
                (
                    f"run_capture link{plan.link_index}-upf-n3 timeout 3 nsenter -t $upf_pid_{plan.link_index} -n "
                    f"tcpdump -eni \"$upf_n3_if_{plan.link_index}\" 'arp or icmp or udp port 2152' &"
                ),
                "wait || true",
                "",
            ]
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output_path.chmod(0o755)
