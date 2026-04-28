#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import ipaddress
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import time


def _stable_hash(value: str) -> int:
    result = 0
    for char in value:
        result = ((result * 131) + ord(char)) & 0xFFFFFFFF
    return result


def _activity_factor(flow_id: str, tick_index: int, requested_rate_pps: float, capped_rate_pps: float) -> float:
    if requested_rate_pps <= 0.0 or capped_rate_pps <= 0.0:
        return 0.0
    headroom_ratio = min(1.0, capped_rate_pps / requested_rate_pps)
    base = 0.82 + 0.12 * headroom_ratio
    variation = ((_stable_hash(f"{flow_id}:{tick_index}") % 11) - 5) * 0.01
    return max(0.70, min(0.98, base + variation))


def _select_interface_for_session(rows: list[list[str]], session_index: int) -> tuple[list[str], bool]:
    if not rows:
        raise ValueError("rows must not be empty")
    selected_index = min(max(0, int(session_index)), len(rows) - 1)
    return rows[selected_index], session_index >= len(rows)


def _list_ue_interfaces(container: str) -> list[list[str]]:
    output = subprocess.check_output(
        [
            "docker",
            "exec",
            container,
            "bash",
            "-lc",
            """
find /sys/class/net -maxdepth 1 -name 'uesimtun*' -printf '%f\n' | sort -V | while read -r iface; do
    cidr=$(ip -4 -o addr show dev "$iface" | sed -n 's/.* inet \\([^ ]*\\).*/\\1/p' | head -n 1)
  [ -n "$cidr" ] && echo "$iface ${cidr%%/*}"
done
""",
        ],
        text=True,
    )
    rows: list[list[str]] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            ipaddress.ip_address(parts[1])
        except ValueError:
            continue
        rows.append(parts)
    return rows


def _resolve_ue_interface(container: str, session_index: int) -> tuple[dict[str, str], bool] | None:
    rows = _list_ue_interfaces(container)
    if not rows:
        return None
    selected_row, used_fallback = _select_interface_for_session(rows, session_index)
    return {
        "iface": selected_row[0],
        "ip": selected_row[1],
    }, used_fallback


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate real UE UDP traffic from ns-3 flow profiles")
    parser.add_argument("--flow-profile-file", required=True)
    parser.add_argument("--clock-file", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--target-ip", required=True)
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument("--source-base-port", type=int, default=15000)
    parser.add_argument("--tick-ms", type=int, default=1000)
    parser.add_argument("--upf-container", action="append", required=True)
    parser.add_argument("ue_mappings", nargs="+", help="UE-to-container mapping, for example ue1=nrint-ue1")
    args = parser.parse_args(argv)

    ue_containers = dict(item.split("=", 1) for item in args.ue_mappings)
    upf_containers = list(dict.fromkeys(args.upf_container))
    sessions_by_ue: dict[str, list[str]] = {}
    flows: list[dict[str, object]] = []
    with Path(args.flow_profile_file).open("r", encoding="utf-8") as handle:
        for index, row in enumerate(csv.DictReader(handle, delimiter="\t")):
            ue_name = (row.get("ue_name") or "").strip()
            if ue_name not in ue_containers:
                raise ValueError(f"flow {row.get('flow_id')} references unknown UE mapping {ue_name}")
            session_ref = (row.get("session_ref") or "").strip()
            if not session_ref:
                raise ValueError(f"flow {row.get('flow_id')} must define session_ref")
            ue_sessions = sessions_by_ue.setdefault(ue_name, [])
            if session_ref not in ue_sessions:
                ue_sessions.append(session_ref)
            packet_size = int(float(row["packet_size_bytes"]))
            rate_pps = float(row["arrival_rate_pps"])
            if packet_size < 1 or packet_size > 65507:
                raise ValueError(f"flow {row.get('flow_id')} packet_size_bytes must be 1..65507 for UDP")
            if rate_pps <= 0:
                raise ValueError(f"flow {row.get('flow_id')} arrival_rate_pps must be positive")
            source_port = args.source_base_port + index
            if source_port > 65535:
                raise ValueError("source-base-port plus flow index exceeds 65535")
            flows.append(
                {
                    "flow_id": row["flow_id"],
                    "ue_name": ue_name,
                    "session_ref": session_ref,
                    "session_index": ue_sessions.index(session_ref),
                    "container": ue_containers[ue_name],
                    "port": args.base_port + index,
                    "source_port": source_port,
                    "packet_size": packet_size,
                    "rate_pps": rate_pps,
                    "carry_ul": 0.0,
                    "carry_dl": 0.0,
                }
            )

    state_path = Path(args.state_file)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    marker = f"real-ue-udp-{os.getpid()}"
    sender_binary = state_path.parent / "ue_udp_sender"
    subprocess.run(
        [
            "gcc",
            "-O2",
            "-static",
            str(Path(__file__).with_name("ue_udp_sender.c")),
            "-o",
            str(sender_binary),
        ],
        check=True,
    )
    container_sender = f"/tmp/ue_udp_sender-{marker}"
    for container in sorted(set(ue_containers.values()) | set(upf_containers)):
        subprocess.run(["docker", "cp", str(sender_binary), f"{container}:{container_sender}"], check=True)
        subprocess.run(["docker", "exec", container, "chmod", "+x", container_sender], check=True)

    active_processes: list[subprocess.Popen[str]] = []
    stopping = False

    def stop(_signum: int | None = None, _frame: object | None = None) -> None:
        nonlocal stopping
        stopping = True
        for process in active_processes:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for container in sorted(set(ue_containers.values()) | set(upf_containers)):
            subprocess.run(
                ["docker", "exec", container, "pkill", "-f", f"[{marker[0]}]{marker[1:]}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_tick: int | None = None
    last_sim_time_ms: int | None = None
    ue_interfaces: dict[tuple[str, int], dict[str, str]] = {}
    dl_routes: dict[str, dict[str, str]] = {}
    missing_interface_keys: set[tuple[str, int]] = set()
    missing_route_ips: set[str] = set()
    try:
        while not stopping:
            try:
                clock = json.loads(Path(args.clock_file).read_text(encoding="utf-8"))
                if clock.get("run_id") != args.run_id or clock.get("scenario_id") != args.scenario_id:
                    time.sleep(0.2)
                    continue
                tick_index = int(clock["tick_index"])
                sim_time_ms = int(clock["sim_time_ms"])
                allocation_by_flow = {
                    str(item["flow_id"]): item
                    for item in clock.get("flows", [])
                    if isinstance(item, dict) and "flow_id" in item
                }
            except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
                time.sleep(0.2)
                continue

            if tick_index == last_tick:
                time.sleep(0.05)
                continue

            elapsed_ms = args.tick_ms if last_sim_time_ms is None else sim_time_ms - last_sim_time_ms
            if elapsed_ms <= 0:
                last_tick = tick_index
                last_sim_time_ms = sim_time_ms
                continue

            scheduled: list[dict[str, object]] = []
            for flow in flows:
                allocation = allocation_by_flow.get(str(flow["flow_id"]), {})
                packet_size = float(flow["packet_size"])
                allocated_ul = float(allocation.get("allocated_bandwidth_ul_mbps", 0.0) or 0.0)
                allocated_dl = float(allocation.get("allocated_bandwidth_dl_mbps", 0.0) or 0.0)
                requested_rate_pps = float(flow["rate_pps"])
                capped_ul_rate_pps = allocated_ul * 1e6 / 8.0 / packet_size if allocated_ul > 0.0 else 0.0
                capped_dl_rate_pps = allocated_dl * 1e6 / 8.0 / packet_size if allocated_dl > 0.0 else 0.0
                ul_rate_pps = min(
                    requested_rate_pps,
                    capped_ul_rate_pps
                    * _activity_factor(
                        str(flow["flow_id"]),
                        tick_index,
                        requested_rate_pps,
                        capped_ul_rate_pps,
                    ),
                )
                dl_rate_pps = min(
                    requested_rate_pps,
                    capped_dl_rate_pps
                    * _activity_factor(
                        f"{flow['flow_id']}:dl",
                        tick_index,
                        requested_rate_pps,
                        capped_dl_rate_pps,
                    ),
                )
                exact_ul_packets = ul_rate_pps * elapsed_ms / 1000.0 + float(flow["carry_ul"])
                exact_dl_packets = dl_rate_pps * elapsed_ms / 1000.0 + float(flow["carry_dl"])
                ul_packets = int(exact_ul_packets)
                dl_packets = int(exact_dl_packets)
                flow["carry_ul"] = exact_ul_packets - ul_packets
                flow["carry_dl"] = exact_dl_packets - dl_packets
                if ul_packets <= 0 and dl_packets <= 0:
                    continue
                key = (str(flow["container"]), int(flow["session_index"]))
                if key not in ue_interfaces:
                    resolved = _resolve_ue_interface(str(flow["container"]), int(flow["session_index"]))
                    if resolved is None:
                        if key not in missing_interface_keys:
                            print(
                                f"waiting: {flow['container']} has no usable uesimtun interface for session index {flow['session_index']}",
                                flush=True,
                            )
                            missing_interface_keys.add(key)
                        continue
                    selected_interface, used_fallback = resolved
                    missing_interface_keys.discard(key)
                    if used_fallback:
                        print(
                            f"warning: {flow['container']} is missing session index {flow['session_index']}; "
                            f"using {selected_interface['iface']} instead",
                            flush=True,
                        )
                    ue_interfaces[key] = selected_interface
                flow["iface"] = ue_interfaces[key]["iface"]
                flow["ue_ip"] = ue_interfaces[key]["ip"]
                if str(flow["ue_ip"]) not in dl_routes:
                    for upf_container in upf_containers:
                        route = subprocess.check_output(
                            [
                                "docker",
                                "exec",
                                upf_container,
                                "sh",
                                "-lc",
                                f"ip route get {shlex.quote(str(flow['ue_ip']))} | head -n 1",
                            ],
                            text=True,
                        ).split()
                        if "dev" in route and route[route.index("dev") + 1] == "upfgtp":
                            dl_routes[str(flow["ue_ip"])] = {
                                "container": upf_container,
                                "iface": "upfgtp",
                            }
                            break
                    if str(flow["ue_ip"]) not in dl_routes:
                        if str(flow["ue_ip"]) not in missing_route_ips:
                            print(
                                f"waiting: no UPF upfgtp route found for UE IP {flow['ue_ip']}",
                                flush=True,
                            )
                            missing_route_ips.add(str(flow["ue_ip"]))
                        continue
                missing_route_ips.discard(str(flow["ue_ip"]))
                flow["dl_container"] = dl_routes[str(flow["ue_ip"])]["container"]
                flow["dl_iface"] = dl_routes[str(flow["ue_ip"])]["iface"]
                scheduled.append({**flow, "ul_packets": ul_packets, "dl_packets": dl_packets})

            grouped_ul: dict[tuple[str, str], list[dict[str, object]]] = {}
            grouped_dl: dict[tuple[str, str], list[dict[str, object]]] = {}
            for flow in scheduled:
                if int(flow["ul_packets"]) > 0:
                    grouped_ul.setdefault((str(flow["container"]), str(flow["iface"])), []).append(flow)
                if int(flow["dl_packets"]) > 0:
                    grouped_dl.setdefault((str(flow["dl_container"]), str(flow["dl_iface"])), []).append(flow)

            active_processes.clear()
            for (container, iface), container_flows in grouped_ul.items():
                ports = " ".join(str(flow["port"]) for flow in container_flows)
                source_ports = " ".join(str(flow["source_port"]) for flow in container_flows)
                sizes = " ".join(str(flow["packet_size"]) for flow in container_flows)
                counts = " ".join(str(flow["ul_packets"]) for flow in container_flows)
                flow_ids = " ".join(shlex.quote(str(flow["flow_id"])) for flow in container_flows)
                script = f"""
REAL_FLOW_MARKER={shlex.quote(marker)}
target_ip={shlex.quote(args.target_ip)}
tick_index={tick_index}
sim_time_ms={sim_time_ms}
iface={shlex.quote(iface)}
ports=({ports})
source_ports=({source_ports})
sizes=({sizes})
counts=({counts})
flow_ids=({flow_ids})
source_cidr=$(ip -4 -o addr show dev "$iface" | sed -n 's/.* inet \\([^ ]*\\).*/\\1/p' | head -n 1)
source_ip=${{source_cidr%%/*}}
for i in "${{!ports[@]}}"; do
  {shlex.quote(container_sender)} "$target_ip" "${{ports[$i]}}" "${{source_ports[$i]}}" "$iface" "${{sizes[$i]}}" "${{counts[$i]}}"
  echo "direction=ul tick=$tick_index sim_ms=$sim_time_ms flow=${{flow_ids[$i]}} src=$source_ip:${{source_ports[$i]}} dst=$target_ip:${{ports[$i]}} iface=$iface size=${{sizes[$i]}} packets=${{counts[$i]}}"
done
"""
                active_processes.append(
                    subprocess.Popen(
                        ["docker", "exec", container, "bash", "-lc", script],
                        text=True,
                        start_new_session=True,
                    )
                )
            for (container, iface), container_flows in grouped_dl.items():
                ports = " ".join(str(flow["source_port"]) for flow in container_flows)
                source_ports = " ".join(str(flow["port"]) for flow in container_flows)
                target_ips = " ".join(shlex.quote(str(flow["ue_ip"])) for flow in container_flows)
                sizes = " ".join(str(flow["packet_size"]) for flow in container_flows)
                counts = " ".join(str(flow["dl_packets"]) for flow in container_flows)
                flow_ids = " ".join(shlex.quote(str(flow["flow_id"])) for flow in container_flows)
                script = f"""
REAL_FLOW_MARKER={shlex.quote(marker)}
tick_index={tick_index}
sim_time_ms={sim_time_ms}
iface={shlex.quote(iface)}
ports=({ports})
source_ports=({source_ports})
target_ips=({target_ips})
sizes=({sizes})
counts=({counts})
flow_ids=({flow_ids})
for i in "${{!ports[@]}}"; do
  {shlex.quote(container_sender)} "${{target_ips[$i]}}" "${{ports[$i]}}" "${{source_ports[$i]}}" "$iface" "${{sizes[$i]}}" "${{counts[$i]}}"
  echo "direction=dl tick=$tick_index sim_ms=$sim_time_ms flow=${{flow_ids[$i]}} src=$HOSTNAME:${{source_ports[$i]}} dst=${{target_ips[$i]}}:${{ports[$i]}} iface=$iface size=${{sizes[$i]}} packets=${{counts[$i]}}"
done
"""
                active_processes.append(
                    subprocess.Popen(
                        ["docker", "exec", container, "bash", "-lc", script],
                        text=True,
                        start_new_session=True,
                    )
                )

            return_code = 0
            for process in active_processes:
                return_code = max(return_code, process.wait())
            if return_code != 0:
                return return_code

            with state_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "tick_index": tick_index,
                            "sim_time_ms": sim_time_ms,
                            "target_ip": args.target_ip,
                            "flows": [
                                {
                                    "flow_id": str(flow["flow_id"]),
                                    "ue_name": str(flow["ue_name"]),
                                    "session_ref": str(flow["session_ref"]),
                                    "container": str(flow["container"]),
                                    "interface": str(flow["iface"]),
                                    "ue_ip": str(flow["ue_ip"]),
                                    "dl_container": str(flow["dl_container"]),
                                    "source_port": int(flow["source_port"]),
                                    "destination_port": int(flow["port"]),
                                    "packet_size_bytes": int(flow["packet_size"]),
                                    "ul_packets_sent": int(flow["ul_packets"]),
                                    "dl_packets_sent": int(flow["dl_packets"]),
                                }
                                for flow in scheduled
                            ],
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            last_tick = tick_index
            last_sim_time_ms = sim_time_ms
    finally:
        stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
