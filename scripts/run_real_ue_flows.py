#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import ipaddress
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import time
from typing import Callable

from bridge.user_plane.protocol import MessageType, StreamDecoder


def _select_interface_for_session(rows: list[list[str]], session_index: int) -> list[str] | None:
    if not rows:
        raise ValueError("rows must not be empty")
    selected_index = max(0, int(session_index))
    return rows[selected_index] if selected_index < len(rows) else None


def _list_ue_interfaces(container: str) -> list[list[str]]:
    output = subprocess.check_output(
        [
            "docker",
            "exec",
            container,
            "bash",
            "-lc",
            r"""
find /sys/class/net -maxdepth 1 -name 'uesimtun*' -printf '%f\n' | sort -V | while read -r iface; do
    cidr=$(ip -4 -o addr show dev "$iface" | sed -n 's/.* inet \([^ ]*\).*/\1/p' | head -n 1)
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


def _resolve_ue_interface(container: str, session_index: int) -> dict[str, str] | None:
    try:
        rows = _list_ue_interfaces(container)
    except (OSError, subprocess.CalledProcessError):
        return None
    if not rows:
        return None
    selected = _select_interface_for_session(rows, session_index)
    if selected is None:
        return None
    return {"iface": selected[0], "ip": selected[1]}


def _controlled_sender_argv(
    *,
    sender: str,
    target_ip: str,
    destination_port: int,
    source_port: int,
    interface: str,
    packet_size: int,
    flow_id: str,
    epoch_id: int,
    application_sequence: int,
) -> list[str]:
    return [
        sender,
        target_ip,
        str(destination_port),
        str(source_port),
        interface,
        str(packet_size),
        "1",
        flow_id,
        str(epoch_id),
        str(application_sequence),
    ]


class _AuthorizationLedger:
    def __init__(self) -> None:
        self._completed: set[int] = set()

    def should_dispatch(self, authorization_id: int) -> bool:
        return authorization_id not in self._completed

    def complete(self, authorization_id: int) -> None:
        self._completed.add(authorization_id)


def _resolve_downlink_route(upf_containers: list[str], ue_ip: str) -> dict[str, str] | None:
    for container in upf_containers:
        try:
            route = subprocess.check_output(
                [
                    "docker",
                    "exec",
                    container,
                    "sh",
                    "-lc",
                    f"ip route get {shlex.quote(ue_ip)} | head -n 1",
                ],
                text=True,
            ).split()
        except (OSError, subprocess.CalledProcessError):
            continue
        if "dev" in route and route[route.index("dev") + 1] == "upfgtp":
            return {"container": container, "iface": "upfgtp"}
    return None


def _run_controlled(
    *,
    args: argparse.Namespace,
    flows: list[dict[str, object]],
    container_sender: str,
    upf_containers: list[str],
    should_stop: Callable[[], bool],
) -> int:
    flows_by_id = {str(flow["flow_id"]): flow for flow in flows}
    decoder = StreamDecoder()
    ledger = _AuthorizationLedger()
    connection: socket.socket | None = None
    while not should_stop():
        if connection is None:
            try:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                connection.connect(args.authorization_socket)
            except OSError:
                if connection is not None:
                    connection.close()
                connection = None
                time.sleep(0.2)
                continue
        try:
            data = connection.recv(65536)
        except OSError:
            connection.close()
            connection = None
            decoder = StreamDecoder()
            continue
        if not data:
            connection.close()
            connection = None
            decoder = StreamDecoder()
            continue
        for message in decoder.feed(data):
            if message.message_type is not MessageType.AUTHORIZE_SEND:
                continue
            authorization_id = int(message.payload["authorization_id"])
            if not ledger.should_dispatch(authorization_id):
                continue
            flow_id = str(message.payload["flow_id"])
            flow = flows_by_id.get(flow_id)
            if flow is None:
                raise ValueError(f"authorization references unknown flow {flow_id}")
            direction = str(message.payload["direction"])
            packet_size = int(message.payload.get("payload_size", flow["packet_size"]))
            epoch_id = int(message.payload["epoch_id"])
            sequence = int(message.payload.get("application_sequence", message.sequence))
            while not should_stop():
                selected = _resolve_ue_interface(
                    str(flow["container"]), int(flow["session_index"])
                )
                if selected is None:
                    time.sleep(0.2)
                    continue
                if direction == "uplink":
                    container = str(flow["container"])
                    interface = selected["iface"]
                    target_ip = args.target_ip
                    destination_port = int(flow["port"])
                    source_port = int(flow["source_port"])
                    break
                if direction == "downlink":
                    route = _resolve_downlink_route(upf_containers, selected["ip"])
                    if route is None:
                        time.sleep(0.2)
                        continue
                    container = route["container"]
                    interface = route["iface"]
                    target_ip = selected["ip"]
                    destination_port = int(flow["source_port"])
                    source_port = int(flow["port"])
                    break
                raise ValueError(f"unsupported authorization direction {direction}")
            else:
                break
            subprocess.run(
                [
                    "docker",
                    "exec",
                    container,
                    *_controlled_sender_argv(
                        sender=container_sender,
                        target_ip=target_ip,
                        destination_port=destination_port,
                        source_port=source_port,
                        interface=interface,
                        packet_size=packet_size,
                        flow_id=flow_id,
                        epoch_id=epoch_id,
                        application_sequence=sequence,
                    ),
                ],
                check=True,
            )
            ledger.complete(authorization_id)
    if connection is not None:
        connection.close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate gate-authorized real UE UDP traffic"
    )
    parser.add_argument("--flow-profile-file", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--target-ip", required=True)
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument("--source-base-port", type=int, default=15000)
    parser.add_argument("--upf-container", action="append", required=True)
    parser.add_argument("--authorization-socket", required=True)
    parser.add_argument("ue_mappings", nargs="+", help="for example ue1=nrint-ue1")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    ue_containers = dict(item.split("=", 1) for item in args.ue_mappings)
    upf_containers = list(dict.fromkeys(args.upf_container))
    sessions_by_ue: dict[str, list[str]] = {}
    flows: list[dict[str, object]] = []
    with Path(args.flow_profile_file).open("r", encoding="utf-8") as handle:
        for index, row in enumerate(csv.DictReader(handle, delimiter="\t")):
            ue_name = (row.get("ue_name") or "").strip()
            if ue_name not in ue_containers:
                raise ValueError(
                    f"flow {row.get('flow_id')} references unknown UE mapping {ue_name}"
                )
            session_ref = (row.get("session_ref") or "").strip()
            if not session_ref:
                raise ValueError(f"flow {row.get('flow_id')} must define session_ref")
            ue_sessions = sessions_by_ue.setdefault(ue_name, [])
            if session_ref not in ue_sessions:
                ue_sessions.append(session_ref)
            packet_size = int(float(row["packet_size_bytes"]))
            if not 1 <= packet_size <= 65507:
                raise ValueError(
                    f"flow {row.get('flow_id')} packet_size_bytes must be 1..65507 for UDP"
                )
            if float(row["arrival_rate_pps"]) <= 0:
                raise ValueError(f"flow {row.get('flow_id')} arrival_rate_pps must be positive")
            source_port = args.source_base_port + index
            if source_port > 65535:
                raise ValueError("source-base-port plus flow index exceeds 65535")
            flows.append(
                {
                    "flow_id": row["flow_id"],
                    "session_index": ue_sessions.index(session_ref),
                    "container": ue_containers[ue_name],
                    "port": args.base_port + index,
                    "source_port": source_port,
                    "packet_size": packet_size,
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
    containers = sorted(set(ue_containers.values()) | set(upf_containers))
    for container in containers:
        subprocess.run(
            ["docker", "cp", str(sender_binary), f"{container}:{container_sender}"],
            check=True,
        )
        subprocess.run(
            ["docker", "exec", container, "chmod", "+x", container_sender],
            check=True,
        )

    stopping = False

    def stop(_signum: int | None = None, _frame: object | None = None) -> None:
        nonlocal stopping
        stopping = True
        for container in containers:
            subprocess.run(
                ["docker", "exec", container, "pkill", "-f", f"[{marker[0]}]{marker[1:]}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        return _run_controlled(
            args=args,
            flows=flows,
            container_sender=container_sender,
            upf_containers=upf_containers,
            should_stop=lambda: stopping,
        )
    finally:
        stop()


if __name__ == "__main__":
    raise SystemExit(main())
