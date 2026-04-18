"""Run manifest creation for generated integration runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

from bridge.common.scenario import ScenarioConfig
from bridge.common.topology import ResolvedScenarioTopology


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
    compose_project_name: str
    free5gc_webui_url: str
    bridge_script: str
    snapshot_file: str
    ns3_flow_profile_file: str
    state_db: str
    archive_dir: str
    ns3_source_file: str
    core_services: list[str]
    ran_services: list[str]
    subscriber_payloads: list[str]
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
    flow_profile_file: Path,
    state_db: Path,
    archive_dir: Path,
    service_map: dict[str, dict[str, str]],
    core_services: list[str],
    ran_services: list[str],
    subscriber_payloads: list[Path],
    free5gc_webui_url: str,
    resolved_topology: ResolvedScenarioTopology,
) -> RunManifest:
    compose_base_argv = [
        "docker",
        "compose",
        "-p",
        scenario.free5gc.project_name,
        "-f",
        str(compose_file),
    ]
    upf_names = ",".join(upf.name for upf in scenario.upfs) or "upf"
    slice_sds = ",".join(slice_config.sd for slice_config in scenario.slices) or "010203"
    gnb_index_by_name = {gnb.name: index for index, gnb in enumerate(scenario.gnbs, start=1)}
    upf_index_by_name = {upf.name: index for index, upf in enumerate(scenario.upfs, start=1)}
    ue_gnb_map = ",".join(
        str(gnb_index_by_name[resolved_topology.ue_to_gnb[ue.name]])
        for ue in scenario.ues
    )
    gnb_upf_map = ",".join(
        str(upf_index_by_name[resolved_topology.gnb_to_upf[gnb.name]])
        for gnb in scenario.gnbs
    )
    ue_supis = ",".join(ue.supi for ue in scenario.ues)
    gnb_positions = ";".join(
        (
            f"{position.x}:{position.y}:{position.z}"
            if (position := resolved_topology.gnb_positions.get(gnb.name)) is not None
            else "auto"
        )
        for gnb in scenario.gnbs
    )
    ue_positions = ";".join(
        (
            f"{position.x}:{position.y}:{position.z}"
            if (position := resolved_topology.ue_positions.get(ue.name)) is not None
            else "auto"
        )
        for ue in scenario.ues
    )
    commands = [
        CommandSpec(
            name="compose-up-core",
            cwd=str(project_root),
            argv=[*compose_base_argv, "up", "-d", *core_services],
        ),
        CommandSpec(
            name="writer-follow-free5gc",
            cwd=str(project_root),
            argv=[
                "python3",
                "-m",
                "bridge.writer.cli",
                "follow-compose-logs",
                "--parser",
                "free5gc",
                "--compose-file",
                str(compose_file),
                "--project-name",
                scenario.free5gc.project_name,
                "--run-id",
                run_id,
                "--scenario-id",
                scenario.scenario_id,
                "--tick-ms",
                str(scenario.tick_ms),
                "--tail",
                "all",
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
            ] + [item for service in core_services for item in ("--service", service)],
            background=True,
        ),
        CommandSpec(
            name="bootstrap-subscribers",
            cwd=str(project_root),
            argv=[
                "python3",
                "-m",
                "adapters.free5gc_ueransim.subscriber_bootstrap",
                "--base-url",
                free5gc_webui_url,
                "--timeout-seconds",
                "120",
                "--interval-seconds",
                "2",
                *[str(path) for path in subscriber_payloads],
            ],
        ),
        CommandSpec(
            name="compose-up-ran",
            cwd=str(project_root),
            argv=[*compose_base_argv, "up", "-d", *ran_services],
        ),
        CommandSpec(
            name="writer-follow-ueransim",
            cwd=str(project_root),
            argv=[
                "python3",
                "-m",
                "bridge.writer.cli",
                "follow-compose-logs",
                "--parser",
                "ueransim",
                "--compose-file",
                str(compose_file),
                "--project-name",
                scenario.free5gc.project_name,
                "--run-id",
                run_id,
                "--scenario-id",
                scenario.scenario_id,
                "--tick-ms",
                str(scenario.tick_ms),
                "--tail",
                "all",
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
            ] + [item for service in ran_services for item in ("--service", service)],
            background=True,
        ),
        CommandSpec(
            name="writer-follow-ns3",
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
                "--ue-num",
                str(len(scenario.ues)),
                "--ue-num-per-g-nb",
                str(max(1, len(scenario.ues))),
                "--tick-ms",
                str(scenario.tick_ms),
                "--sim-time-ms",
                str(scenario.ns3.sim_time_ms),
                "--output-file",
                str(snapshot_file),
                "--flow-profile-file",
                str(flow_profile_file),
                "--upf-names",
                upf_names,
                "--slice-sds",
                slice_sds,
                "--ue-supis",
                ue_supis,
                "--ue-gnb-map",
                ue_gnb_map,
                "--gnb-upf-map",
                gnb_upf_map,
                "--gnb-positions",
                gnb_positions,
                "--ue-positions",
                ue_positions,
            ],
        ),
        CommandSpec(
            name="compose-down",
            cwd=str(project_root),
            argv=[*compose_base_argv, "down"],
        ),
    ]
    if scenario.bridge.enable_inline_harness:
        compose_up_ran_index = next(
            index for index, command in enumerate(commands) if command.name == "compose-up-ran"
        )
        commands.insert(
            compose_up_ran_index + 1,
            CommandSpec(
                name="bridge-setup",
                cwd=str(project_root),
                argv=["bash", str(bridge_script)],
            ),
        )
    if scenario.writer.graph_db_url:
        next(
            command for command in commands if command.name == "writer-follow-ns3"
        ).argv.extend(["--graph-db-url", scenario.writer.graph_db_url])
    if resolved_topology.source_graph_file:
        next(
            command for command in commands if command.name == "writer-follow-ns3"
        ).argv.extend(["--topology-version", Path(resolved_topology.source_graph_file).name])

    return RunManifest(
        run_id=run_id,
        scenario_id=scenario.scenario_id,
        run_dir=str(run_dir),
        compose_file=str(compose_file),
        compose_project_name=scenario.free5gc.project_name,
        free5gc_webui_url=free5gc_webui_url,
        bridge_script=str(bridge_script),
        snapshot_file=str(snapshot_file),
        ns3_flow_profile_file=str(flow_profile_file),
        state_db=str(state_db),
        archive_dir=str(archive_dir),
        ns3_source_file=str(project_root / "sim" / "ns3" / "nr_multignb_multiupf.cc"),
        core_services=core_services,
        ran_services=ran_services,
        subscriber_payloads=[str(path) for path in subscriber_payloads],
        service_map=service_map,
        commands=commands,
    )