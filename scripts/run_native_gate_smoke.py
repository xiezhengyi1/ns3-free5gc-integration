#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bridge.user_plane.protocol import (
    Message,
    MessageType,
    StreamDecoder,
    encode_message,
)


def _write_flow_profile(path: Path) -> None:
    fields = [
        "flow_id",
        "supi",
        "five_qi",
        "qfi",
        "packet_size_bytes",
        "arrival_rate_pps",
        "rlc_mode",
        "virtual_expiry_ms",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
        writer.writeheader()
        writer.writerow(
            {
                "flow_id": "flow-native-smoke",
                "supi": "imsi-001010000000001",
                "five_qi": 7,
                "qfi": 9,
                "packet_size_bytes": 128,
                "arrival_rate_pps": 10,
                "rlc_mode": "UM",
                "virtual_expiry_ms": 50,
            }
        )


def _write_bearer_map(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "flows": [
                    {
                        "flow_id": "flow-native-smoke",
                        "five_qi": 7,
                        "qfi": 9,
                        "rlc_mode": "UM",
                        "virtual_expiry_us": 50_000,
                    }
                ]
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def run_native_gate_smoke(ns3_root: Path) -> dict[str, int]:
    ns3_root = ns3_root.expanduser().resolve()
    ns3 = ns3_root / "ns3"
    if not ns3.exists():
        raise FileNotFoundError(ns3)

    with tempfile.TemporaryDirectory(prefix="n6ai-native-") as tmp:
        output_dir = Path(tmp)
        socket_path = output_dir / "gate.sock"
        flow_profile = output_dir / "flows.tsv"
        bearer_map = output_dir / "bearer-map.json"
        snapshot = output_dir / "snapshots.jsonl"
        _write_flow_profile(flow_profile)
        _write_bearer_map(bearer_map)

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        listener.settimeout(15.0)
        run_spec = " ".join(
            [
                "scratch/nr_multignb_multiupf",
                "--simTimeMs=600",
                "--tickMs=100",
                "--simulator=DefaultSimulatorImpl",
                "--externalTrafficOnly=true",
                "--ueSupis=imsi-001010000000001",
                f"--flowProfileFile={flow_profile}",
                f"--bearerMapFile={bearer_map}",
                f"--userPlaneGateSocket={socket_path}",
                f"--outputFile={snapshot}",
                "--virtualEpochUs=100000",
                "--rngSeed=1",
                "--rngRun=1",
            ]
        )
        process = subprocess.Popen(
            [str(ns3), "run", run_spec],
            cwd=ns3_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        connection: socket.socket | None = None
        try:
            connection, _ = listener.accept()
            connection.settimeout(0.1)
            decoder = StreamDecoder()
            sequence = 1
            next_packet_id = 1
            current_epoch: tuple[int, int] | None = None
            submitted = 0
            delivered = 0
            dropped = 0
            completed = 0
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                try:
                    data = connection.recv(65536)
                except socket.timeout:
                    if process.poll() is not None:
                        break
                    continue
                if not data:
                    break
                for message in decoder.feed(data):
                    if message.message_type is MessageType.EPOCH_START:
                        current_epoch = (
                            int(message.payload["epoch_id"]),
                            int(message.payload["ns3_time_us"]),
                        )
                    elif message.message_type is MessageType.AUTHORIZE_SEND:
                        if current_epoch is None:
                            raise RuntimeError("authorization preceded EPOCH_START")
                        epoch_id, enqueue_ns3_us = current_epoch
                        enqueue = Message(
                            MessageType.PACKET_ENQUEUE,
                            sequence=sequence,
                            payload={
                                "packet_id": next_packet_id,
                                "epoch_id": epoch_id,
                                "flow_id": str(message.payload["flow_id"]),
                                "direction": str(message.payload["direction"]),
                                "size_bytes": int(message.payload["payload_size"]),
                                "qfi": 9,
                                "enqueue_ns3_us": enqueue_ns3_us,
                                "virtual_expiry_us": 50_000,
                            },
                        )
                        connection.sendall(encode_message(enqueue))
                        sequence += 1
                        next_packet_id += 1
                        submitted += 1
                    elif message.message_type is MessageType.PACKET_DELIVER:
                        delivered += 1
                    elif message.message_type is MessageType.PACKET_DROP:
                        dropped += 1
                    elif message.message_type is MessageType.TICK_COMPLETE:
                        completed += 1
            stdout, stderr = process.communicate(timeout=10)
            if process.returncode != 0:
                raise RuntimeError(
                    f"native gated scenario failed ({process.returncode})\n{stdout}\n{stderr}"
                )
            if submitted < 2 or delivered + dropped < 2 or completed < 1:
                raise RuntimeError(
                    "native gated scenario did not complete a packet epoch: "
                    f"submitted={submitted} delivered={delivered} "
                    f"dropped={dropped} completed={completed}"
                )
            return {
                "submitted_packets": submitted,
                "delivered_packets": delivered,
                "dropped_packets": dropped,
                "completed_epochs": completed,
            }
        finally:
            if connection is not None:
                connection.close()
            listener.close()
            if process.poll() is None:
                process.kill()
                process.wait()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a native gated ns-3 shadow-bearer smoke scenario."
    )
    parser.add_argument("--ns3-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    print(json.dumps(run_native_gate_smoke(args.ns3_root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
