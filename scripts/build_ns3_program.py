#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bridge.common.scenario import load_scenario


@dataclass(frozen=True, slots=True)
class Ns3BuildPreparation:
    ns3_root: Path
    copied_files: tuple[Path, ...]
    build_command: list[str]


def prepare_ns3_program(
    *,
    project_root: Path,
    ns3_root: Path | None = None,
    scenario_path: Path | None = None,
    run_build: bool = True,
) -> Ns3BuildPreparation:
    resolved_root = project_root.resolve()
    resolved_ns3_root = _resolve_ns3_root(
        project_root=resolved_root,
        ns3_root=ns3_root,
        scenario_path=scenario_path,
    )
    scratch_dir = resolved_ns3_root / "scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    native_test_dir = scratch_dir / "gtpu_shadow_peer_test"
    native_test_dir.mkdir(parents=True, exist_ok=True)
    (scratch_dir / "gtpu_shadow_peer_test.cc").unlink(missing_ok=True)

    source_map = [
        (
            resolved_root / "sim" / "ns3" / "nr_multignb_multiupf.cc",
            scratch_dir / "nr_multignb_multiupf.cc",
        ),
        (
            resolved_root / "sim" / "ns3" / "gtpu_shadow_peer.h",
            scratch_dir / "gtpu_shadow_peer.h",
        ),
        (
            resolved_root / "sim" / "ns3" / "gtpu_shadow_peer.h",
            native_test_dir / "gtpu_shadow_peer.h",
        ),
        (
            resolved_root / "sim" / "ns3" / "gtpu_shadow_peer.cc",
            native_test_dir / "gtpu_shadow_peer.cc",
        ),
        (
            resolved_root / "sim" / "ns3" / "gtpu_shadow_peer_test.cc",
            native_test_dir / "gtpu_shadow_peer_test.cc",
        ),
    ]
    copied: list[Path] = []
    for source, target in source_map:
        if not source.exists():
            raise FileNotFoundError(source)
        shutil.copy2(source, target)
        copied.append(target)

    ns3_executable = resolved_ns3_root / ("ns3.exe" if _is_windows_ns3(resolved_ns3_root) else "ns3")
    build_command = [str(ns3_executable), "build"]
    if run_build:
        subprocess.run(build_command, cwd=resolved_ns3_root, check=True)
    return Ns3BuildPreparation(
        ns3_root=resolved_ns3_root,
        copied_files=tuple(copied),
        build_command=build_command,
    )


def _resolve_ns3_root(
    *,
    project_root: Path,
    ns3_root: Path | None,
    scenario_path: Path | None,
) -> Path:
    if ns3_root is not None:
        return ns3_root.resolve()
    if scenario_path is not None:
        scenario = load_scenario(scenario_path)
        if scenario.ns3.ns3_root:
            return Path(scenario.ns3.ns3_root).expanduser().resolve()
    return Path("/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1")


def _is_windows_ns3(ns3_root: Path) -> bool:
    return (ns3_root / "ns3.exe").exists() and not (ns3_root / "ns3").exists()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and optionally build ns-3 scratch programs.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--ns3-root", type=Path)
    parser.add_argument("--scenario", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = prepare_ns3_program(
        project_root=args.project_root,
        ns3_root=args.ns3_root,
        scenario_path=args.scenario,
        run_build=not args.dry_run,
    )
    print(f"ns3_root={result.ns3_root}")
    for path in result.copied_files:
        print(f"copied={path}")
    print("build_command=" + " ".join(result.build_command))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
