#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bridge.split_mode.config import load_split_mode_config
from bridge.split_mode.renderer import render_split_run
from bridge.split_mode.runner import run_fast_reset_server, run_manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render and start split control-plane/user-plane mode")
    parser.add_argument("scenario", help="split-mode scenario YAML")
    parser.add_argument("--run-id", help="explicit run identifier")
    parser.add_argument("--live-graph-snapshot-id", help="explicit live graph snapshot id for graph delta writes")
    parser.add_argument("--instance-slot", type=int, default=0, help="non-zero worker slot for isolated Docker resources")
    parser.add_argument("--gateway-port", type=int, help="policy gateway TCP port (auto-scoped by worker slot by default)")
    parser.add_argument("--reset-port", type=int, help="fast-reset TCP port (auto-scoped by worker slot by default)")
    parser.add_argument("--graph-db-url", help="worker-scoped graph database URL")
    parser.add_argument(
        "--wait-background",
        dest="wait_background",
        action="store_true",
        help="wait for background commands before exiting without printing startup logs",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    scenario_path = Path(args.scenario).expanduser().resolve()
    run_id = args.run_id
    if run_id is not None and run_id.startswith("-"):
        raise ValueError(f"invalid run id: {run_id}")
    gateway_port = args.gateway_port if args.gateway_port is not None else 18080 + (args.instance_slot * 2)
    reset_port = args.reset_port if args.reset_port is not None else 18081 + (args.instance_slot * 2)
    config = load_split_mode_config(scenario_path)
    rendered = render_split_run(
        Path(__file__).resolve().parents[1],
        config,
        run_id=run_id,
        live_graph_snapshot_id=args.live_graph_snapshot_id,
        instance_slot=args.instance_slot,
        gateway_port=gateway_port,
        reset_port=reset_port,
        graph_db_url=args.graph_db_url,
    )
    if not args.wait_background:
        print(f"run_id={rendered.run_id}")
        print(f"manifest={rendered.manifest_path}")
        print(f"run_dir={rendered.run_dir}")
        if rendered.manifest.live_graph_snapshot_id:
            print(f"live_graph_snapshot_id={rendered.manifest.live_graph_snapshot_id}")
    if args.wait_background:
        return run_fast_reset_server(rendered.manifest_path)
    return run_manifest(rendered.manifest_path, wait_background=False)


if __name__ == "__main__":
    raise SystemExit(main())
