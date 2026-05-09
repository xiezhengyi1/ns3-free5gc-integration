#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bridge.split_mode.config import load_split_mode_config
from bridge.split_mode.renderer import render_split_run
from bridge.split_mode.runner import run_manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render and start split control-plane/user-plane mode")
    parser.add_argument("scenario", help="split-mode scenario YAML")
    parser.add_argument("--run-id", help="explicit run identifier")
    parser.add_argument("--live-graph-snapshot-id", help="explicit live graph snapshot id for graph delta writes")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    scenario_path = Path(args.scenario).expanduser().resolve()
    run_id = args.run_id
    if run_id is not None and run_id.startswith("-"):
        raise ValueError(f"invalid run id: {run_id}")
    config = load_split_mode_config(scenario_path)
    rendered = render_split_run(
        Path(__file__).resolve().parents[1],
        config,
        run_id=run_id,
        live_graph_snapshot_id=args.live_graph_snapshot_id,
    )
    print(f"run_id={rendered.run_id}")
    print(f"manifest={rendered.manifest_path}")
    print(f"run_dir={rendered.run_dir}")
    if rendered.manifest.live_graph_snapshot_id:
        print(f"live_graph_snapshot_id={rendered.manifest.live_graph_snapshot_id}")
    return run_manifest(rendered.manifest_path)


if __name__ == "__main__":
    raise SystemExit(main())
