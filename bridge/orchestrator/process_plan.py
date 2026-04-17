"""Run manifest creation for generated integration runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

from bridge.common.scenario import ScenarioConfig


@dataclass(slots=True)
class CommandSpec:
    name: str
    cwd: str
    argv: list[str]
    background: bool = False
    env: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class RunManifest:
    run_id: str
    scenario_id: str
    run_dir: str
    compose_file: str
    bridge_script: str
    snapshot_file: str
    state_db: str
    archive_dir: str
    ns3_source_file: str
    service_map: dict[str, dict[str, str]]
    commands: list[CommandSpec]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["commands"] = [asdict(command) for command in self.commands]
        return payload


def build_run_manifest(
    project_root: Path,
    scenario: ScenarioConfig,
    run_id: str,
    run_dir: Path,
    compose_file: Path,
    bridge_script: Path,
    snapshot_file: Path,
    state_db: Path,
    archive_dir: Path,
    service_map: dict[str, dict[str, str]],
) -> RunManifest:
    max_ues_per_gnb = max(len(group) for group in scenario.ue_groups().values())
    upf_names = ",".join(upf.name for upf in scenario.upfs) or "upf"
    slice_sds = ",".join(slice_config.sd for slice_config in scenario.slices) or "010203"
    commands = [
        CommandSpec(
            name="compose-up",
            cwd=str(project_root),
            argv=["docker", "compose", "-f", str(compose_file), "up", "-d"],
        ),
        CommandSpec(
            name="writer-follow",
            cwd=str(project_root),
            argv=[
                "python3",
                "-m",
                "bridge.writer.cli",
                "follow-jsonl",
                str(snapshot_file),
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
            ],
            background=True,
        ),
        CommandSpec(
            name="ns3-build",
            cwd=str(project_root),
            argv=["bash", str(project_root / "scripts" / "build_ns3_twin.sh")],
        ),
        CommandSpec(
            name="ns3-run",
            cwd=str(project_root),
            argv=[
                "bash",
                str(project_root / "scripts" / "run_ns3_twin.sh"),
                "--run-id",
                run_id,
                "--scenario-id",
                scenario.scenario_id,
                "--g-nb-num",
                str(len(scenario.gnbs)),
                "--ue-num-per-g-nb",
                str(max_ues_per_gnb),
                "--tick-ms",
                str(scenario.tick_ms),
                "--sim-time-ms",
                str(scenario.ns3.sim_time_ms),
                "--output-file",
                str(snapshot_file),
                "--upf-names",
                upf_names,
                "--slice-sds",
                slice_sds,
            ],
        ),
        CommandSpec(
            name="compose-down",
            cwd=str(project_root),
            argv=["docker", "compose", "-f", str(compose_file), "down"],
        ),
    ]
    if scenario.bridge.enable_inline_harness:
        commands.insert(
            1,
            CommandSpec(
                name="bridge-setup",
                cwd=str(project_root),
                argv=["bash", str(bridge_script)],
            ),
        )

    return RunManifest(
        run_id=run_id,
        scenario_id=scenario.scenario_id,
        run_dir=str(run_dir),
        compose_file=str(compose_file),
        bridge_script=str(bridge_script),
        snapshot_file=str(snapshot_file),
        state_db=str(state_db),
        archive_dir=str(archive_dir),
        ns3_source_file=str(project_root / "sim" / "ns3" / "nr_multignb_multiupf.cc"),
        service_map=service_map,
        commands=commands,
    )