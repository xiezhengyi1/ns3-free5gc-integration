#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import subprocess
import tempfile


def run_smoke(ns3_root: Path) -> dict[str, str]:
    with tempfile.TemporaryDirectory(prefix="multi-n3-smoke-") as directory:
        root = Path(directory)
        profile_file = root / "flows.tsv"
        output_file = root / "snapshots.jsonl"
        rows = [
            {
                "flow_id": "flow-via-b",
                "flow_name": "flow-via-b",
                "ue_name": "ue-1",
                "supi": "imsi-208930000000001",
                "upf_ref": "upf-b",
                "packet_size_bytes": "256",
                "arrival_rate_pps": "20",
            },
            {
                "flow_id": "flow-via-a",
                "flow_name": "flow-via-a",
                "ue_name": "ue-2",
                "supi": "imsi-208930000000002",
                "upf_ref": "upf-a",
                "packet_size_bytes": "256",
                "arrival_rate_pps": "20",
            },
        ]
        with profile_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

        invocation = " ".join(
            [
                "scratch/nr_multignb_multiupf",
                "--gNbNum=2",
                "--ueNum=2",
                "--ueSupis=imsi-208930000000001,imsi-208930000000002",
                "--ueGnbMap=1,2",
                "--upfNames=upf-a,upf-b",
                "--gnbUpfMap=1,2",
                f"--flowProfileFile={profile_file}",
                f"--outputFile={output_file}",
                "--simulator=DefaultSimulatorImpl",
                "--simTimeMs=2000",
                "--tickMs=1000",
                "--rngSeed=1",
                "--rngRun=1",
            ]
        )
        subprocess.run(
            [str(ns3_root / "ns3"), "run", invocation],
            cwd=ns3_root,
            check=True,
        )
        snapshots = [
            json.loads(line)
            for line in output_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not snapshots:
            raise RuntimeError("multi-N3 smoke produced no snapshots")
        selected_upfs = {
            str(flow["flow_id"]): str(flow["dst_upf"])
            for snapshot in snapshots
            for flow in snapshot.get("flows", [])
            if flow.get("flow_id") in {"flow-via-a", "flow-via-b"}
        }
        expected = {"flow-via-a": "upf-a", "flow-via-b": "upf-b"}
        if selected_upfs != expected:
            raise RuntimeError(
                f"unexpected per-flow UPF selection: expected {expected}, got {selected_upfs}"
            )
        return selected_upfs


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a native 2-gNB/2-UPF ns-3 smoke")
    parser.add_argument("--ns3-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_smoke(args.ns3_root.resolve()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
