#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import tempfile


FLOW_HEADER = (
    "flow_id\tflow_name\tue_name\tsupi\tapp_id\tapp_name\tsession_ref\t"
    "slice_ref\tslice_snssai\tdnn\tupf_ref\tservice_type\tservice_type_id\t"
    "five_qi\tpacket_size_bytes\tarrival_rate_pps\tdl_packet_size_bytes\t"
    "ul_packet_size_bytes\tdl_arrival_rate_pps\tul_arrival_rate_pps\tlatency_ms\t"
    "jitter_ms\tloss_rate\tbandwidth_dl_mbps\tbandwidth_ul_mbps\t"
    "guaranteed_bandwidth_dl_mbps\tguaranteed_bandwidth_ul_mbps\tpriority\t"
    "allocated_bandwidth_dl_mbps\tallocated_bandwidth_ul_mbps\toptimize_requested\t"
    "policy_filter\tprecedence\tqos_ref\tcharging_method\tquota\tunit_cost\tenabled"
)


def _find_binary(ns3_root: Path) -> Path:
    candidates = sorted((ns3_root / "build" / "scratch").glob("ns3.*-nr_multignb_multiupf_split-*"))
    binary = next((path for path in candidates if path.is_file()), None)
    if binary is None:
        raise FileNotFoundError("split ns-3 binary not found; run scripts/build_ns3_split.sh first")
    return binary


def run_smoke(ns3_root: Path) -> dict[str, object]:
    binary = _find_binary(ns3_root.resolve())
    with tempfile.TemporaryDirectory(prefix="ns3-split-smoke-", dir=ns3_root / "scratch") as directory:
        root = Path(directory)
        flow_file = root / "flows.tsv"
        output_file = root / "snapshots.jsonl"
        values = [
            "flow-upf-b", "Flow UPF B", "ue-1", "imsi-208930000000001", "app-1", "App 1",
            "ue-1:internet", "slice-1-000001", "01000001", "internet", "upf-b", "eMBB", "1",
            "9", "512", "100", "512", "512", "100", "100", "20", "5", "0.01", "2", "2",
            "1", "1", "1", "2", "2", "false", "", "128", "1", "", "", "", "true",
        ]
        flow_file.write_text(FLOW_HEADER + "\n" + "\t".join(values) + "\n", encoding="utf-8")

        command = [
            str(binary),
            "--runId=native-split-smoke",
            "--scenarioId=native-split-smoke",
            "--gNbNum=1",
            "--ueNum=1",
            "--tickMs=500",
            "--simTimeMs=1600",
            "--simulator=DefaultSimulatorImpl",
            f"--outputFile={output_file}",
            f"--flowProfileFile={flow_file}",
            "--upfNames=upf-a,upf-b",
            "--sliceSds=000001",
            "--ueSupis=imsi-208930000000001",
            "--ueGnbMap=1",
            "--gnbUpfLinks=1:1;1:2",
        ]
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=60)
        if completed.returncode != 0:
            raise RuntimeError(f"native split smoke failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}")

        snapshots = [json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not snapshots:
            raise RuntimeError("native split smoke produced no snapshots")
        snapshot = snapshots[-1]
        n3_targets = {
            link["target"]
            for link in snapshot["links"]
            if link.get("source") == "ran-node-1" and link.get("type") == "tunneled_via"
        }
        if n3_targets != {"core-node-1", "core-node-2"}:
            raise RuntimeError(f"unexpected N3 links: {sorted(n3_targets)}")
        flow = next((item for item in snapshot["flows"] if item.get("flow_id") == "flow-upf-b"), None)
        if flow is None or flow.get("dst_upf") != "upf-b":
            raise RuntimeError(f"unexpected flow UPF selection: {flow}")
        return {"snapshot_count": len(snapshots), "n3_targets": sorted(n3_targets), "dst_upf": flow["dst_upf"]}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the native split multi-N3 smoke scenario")
    parser.add_argument("--ns3-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_smoke(args.ns3_root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
