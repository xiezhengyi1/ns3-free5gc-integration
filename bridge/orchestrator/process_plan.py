"""Run manifest creation for generated integration runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

from adapters.free5gc_ueransim.bridge_setup import BridgeInterfacePlan
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
    live_graph_snapshot_id: str | None
    run_dir: str
    compose_file: str
    compose_project_name: str
    free5gc_webui_url: str
    bridge_script: str
    bridge_probe_script: str
    bridge_links: list[dict[str, object]]
    snapshot_file: str
    clock_file: str
    ns3_flow_profile_file: str
    ns3_slice_resource_file: str
    user_plane_gate_file: str | None
    bearer_map_file: str | None
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
    bridge_probe_script: Path,
    bridge_plans: list[BridgeInterfacePlan],
    snapshot_file: Path,
    clock_file: Path,
    flow_profile_file: Path,
    slice_resource_file: Path,
    user_plane_gate_file: Path | None,
    bearer_map_file: Path | None,
    state_db: Path,
    archive_dir: Path,
    service_map: dict[str, dict[str, str]],
    core_services: list[str],
    ran_services: list[str],
    subscriber_payloads: list[Path],
    free5gc_webui_url: str,
    resolved_topology: ResolvedScenarioTopology,
    live_graph_snapshot_id: str | None = None,
) -> RunManifest:
    python_executable = project_root / ".venv" / "bin" / "python3"
    python_command = str(python_executable) if python_executable.exists() else "python3"
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
    upf_services = list(service_map["upf"].values())
    gnb_services = list(service_map["gnb"].values())
    ue_services = list(service_map["ue"].values())
    smf_services = [service for service in core_services if service == "free5gc-smf"]
    infra_core_services = [
        service for service in core_services if service not in set(upf_services) and service not in set(smf_services)
    ]
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
    resolved_live_graph_snapshot_id = str(live_graph_snapshot_id or "").strip() or f"live-{scenario.scenario_id}"
    commands = [
        CommandSpec(
            name="compose-up-core",
            cwd=str(project_root),
            argv=[
                *compose_base_argv,
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                *infra_core_services,
            ],
        ),
        CommandSpec(
            name="writer-follow-free5gc",
            cwd=str(project_root),
            argv=[
                python_command,
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
                "--clock-file",
                str(clock_file),
                "--tail",
                "all",
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
            ] + [item for service in [*infra_core_services, *upf_services, *smf_services] for item in ("--service", service)],
            background=True,
        ),
        CommandSpec(
            name="bootstrap-subscribers",
            cwd=str(project_root),
            argv=[
                python_command,
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
        *(
            [
                CommandSpec(
                    name="bootstrap-app-data",
                    cwd=str(project_root),
                    argv=[
                        python_command,
                        str(project_root / "scripts" / "bootstrap_app_data.py"),
                        "--uerouting-file",
                        str(run_dir / "generated" / "config" / "uerouting.yaml"),
                        "--mongo-container",
                        "mongodb",
                        "--database",
                        "free5gc",
                    ],
                )
            ]
            if scenario.free5gc.mode == "ulcl"
            else []
        ),
        CommandSpec(
            name="compose-up-upf",
            cwd=str(project_root),
            argv=[
                *compose_base_argv,
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                *upf_services,
            ],
        ),
        CommandSpec(
            name="compose-up-gnb",
            cwd=str(project_root),
            argv=[
                *compose_base_argv,
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                *gnb_services,
            ],
        ),
        CommandSpec(
            name="writer-follow-ueransim",
            cwd=str(project_root),
            argv=[
                python_command,
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
                "--clock-file",
                str(clock_file),
                "--tail",
                "all",
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
            ] + [item for service in [*gnb_services, *ue_services] for item in ("--service", service)],
            background=True,
        ),
        *(
            [
                CommandSpec(
                    name="real-ue-flows",
                    cwd=str(project_root),
                    argv=[
                        python_command,
                        str(project_root / "scripts" / "run_real_ue_flows.py"),
                        "--flow-profile-file",
                        str(flow_profile_file),
                        "--clock-file",
                        str(clock_file),
                        "--state-file",
                        str(clock_file.parent / "real-ue-flows.jsonl"),
                        "--run-id",
                        run_id,
                        "--scenario-id",
                        scenario.scenario_id,
                        "--target-ip",
                        "8.8.8.8",
                        "--base-port",
                        "5000",
                        "--source-base-port",
                        "15000",
                        "--tick-ms",
                        str(scenario.tick_ms),
                        *[
                            item
                            for container in (
                                upf.name if scenario.free5gc.mode == "ulcl" else service_map["upf"][upf.name]
                                for upf in scenario.upfs
                            )
                            for item in ("--upf-container", container)
                        ],
                        *[
                            f"{ue.name}={service_map['ue'][ue.name]}"
                            for ue in scenario.ues
                        ],
                    ],
                    background=True,
                )
            ]
            if scenario.bridge.enable_inline_harness
            else []
        ),
        CommandSpec(
            name="compose-up-smf",
            cwd=str(project_root),
            argv=[
                *compose_base_argv,
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                *smf_services,
            ],
        ),
        CommandSpec(
            name="compose-up-ue",
            cwd=str(project_root),
            argv=[
                *compose_base_argv,
                "up",
                "-d",
                "--force-recreate",
                "--remove-orphans",
                *ue_services,
            ],
        ),
        CommandSpec(
            name="writer-follow-ns3",
            cwd=str(project_root),
            argv=[
                python_command,
                "-m",
                "bridge.writer.cli",
                "follow-jsonl",
                str(snapshot_file),
                "--state-db",
                str(state_db),
                "--archive-dir",
                str(archive_dir),
                "--tick-ms",
                str(scenario.tick_ms),
            ],
            background=True,
        ),
        CommandSpec(
            name="policy-acceptor",
            cwd=str(project_root),
            argv=[
                "bash",
                str(project_root / "scripts" / "run_policy_acceptor.sh"),
                "--host",
                "0.0.0.0",
                "--port",
                "18080",
                "--flow-profile-file",
                str(flow_profile_file),
                "--latest-snapshot-file",
                str(archive_dir / run_id / "latest.json"),
                "--state-file",
                str(run_dir / "state" / "policy-acceptor-state.json"),
                "--upstream-pcf-host",
                "10.100.200.20",
                "--upstream-pcf-port",
                "8000",
                "--default-timeout-ms",
                "30000",
            ],
            background=True,
        ),
        CommandSpec(
            name="ns3-build",
            cwd=str(project_root),
            argv=["bash", str(project_root / "scripts" / "build_ns3_twin.sh")],
            env={"NS3_ROOT": scenario.ns3.ns3_root},
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
                "--simulator",
                scenario.ns3.simulator,
                "--output-file",
                str(snapshot_file),
                "--clock-file",
                str(clock_file),
                "--flow-profile-file",
                str(flow_profile_file),
                "--slice-resource-file",
                str(slice_resource_file),
                "--policy-reload-ms",
                str(scenario.ns3.policy_reload_ms),
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
            env={"NS3_ROOT": scenario.ns3.ns3_root},
        ),
        CommandSpec(
            name="compose-down",
            cwd=str(project_root),
            argv=[*compose_base_argv, "down"],
        ),
    ]
    if (
        scenario.bridge.enable_inline_harness
        and bridge_plans
        and not scenario.bridge.user_plane_gate.enabled
    ):
        ns3_run = next(command for command in commands if command.name == "ns3-run")
        ns3_run.argv.extend(
            [
                "--bridge-gnb-taps",
                ",".join(plan.gnb_tap for plan in bridge_plans),
                "--bridge-upf-taps",
                ",".join(plan.upf_tap for plan in bridge_plans),
                "--bridge-link-rate-mbps",
                str(scenario.ns3.bridge_link_rate_mbps),
                "--bridge-link-delay-ms",
                str(scenario.ns3.bridge_link_delay_ms),
                "--bridge-link-loss-rate",
                str(scenario.ns3.bridge_link_loss_rate),
                "--external-traffic-only",
                "--external-traffic-target-ip",
                "8.8.8.8",
                "--external-traffic-source-base-port",
                "15000",
            ]
        )
    if scenario.bridge.enable_inline_harness:
        next(command for command in commands if command.name == "writer-follow-ns3").argv.extend(
            [
                "--real-traffic-state-file",
                str(clock_file.parent / "real-ue-flows.jsonl"),
                "--real-traffic-timeout-seconds",
                "15",
            ]
        )
        compose_up_gnb_index = next(
            index for index, command in enumerate(commands) if command.name == "compose-up-gnb"
        )
        commands.insert(
            compose_up_gnb_index + 1,
            CommandSpec(
                name="bridge-setup",
                cwd=str(project_root),
                argv=[
                    "docker",
                    "run",
                    "--rm",
                    "--privileged",
                    "--pid",
                    "host",
                    "--network",
                    "host",
                    "-v",
                    "/:/host",
                    "free5gc/base:latest",
                    "chroot",
                    "/host",
                    "bash",
                    str(bridge_script),
                ],
            ),
        )
        if scenario.bridge.user_plane_gate.enabled:
            if user_plane_gate_file is None or bearer_map_file is None:
                raise ValueError("gated user plane requires rendered gate and bearer map files")
            real_ue_flows = next(
                command for command in commands if command.name == "real-ue-flows"
            )
            real_ue_flows.argv.extend(
                [
                    "--controlled",
                    "--authorization-socket",
                    f"{scenario.bridge.user_plane_gate.socket_path}.agents",
                ]
            )
            ns3_run = next(command for command in commands if command.name == "ns3-run")
            ns3_run.argv.extend(
                [
                    "--user-plane-gate-socket",
                    scenario.bridge.user_plane_gate.socket_path,
                    "--bearer-map-file",
                    str(bearer_map_file),
                    "--rng-seed",
                    str(scenario.seed),
                    "--rng-run",
                    str(scenario.ns3.rng_run),
                    "--virtual-epoch-us",
                    str(scenario.ns3.virtual_epoch_us),
                    "--channel-update-ms",
                    str(scenario.ns3.channel_update_ms),
                    "--shadowing-enabled",
                    "true" if scenario.ns3.shadowing_enabled else "false",
                    "--external-traffic-only",
                ]
            )
            bridge_setup_index = next(
                index for index, command in enumerate(commands) if command.name == "bridge-setup"
            )
            commands.insert(
                bridge_setup_index + 1,
                CommandSpec(
                    name="user-plane-gate",
                    cwd=str(project_root),
                    argv=[
                        python_command,
                        "-m",
                        "bridge.user_plane.cli",
                        "--config",
                        str(user_plane_gate_file),
                    ],
                    background=True,
                ),
            )
        ns3_run_index = next(
            index for index, command in enumerate(commands) if command.name == "ns3-run"
        )
        commands.insert(
            ns3_run_index,
            CommandSpec(
                name="bridge-probe-post-ns3",
                cwd=str(project_root),
                argv=[
                    "docker",
                    "run",
                    "--rm",
                    "--privileged",
                    "--pid",
                    "host",
                    "--network",
                    "host",
                    "-v",
                    "/:/host",
                    "free5gc/base:latest",
                    "chroot",
                    "/host",
                    "bash",
                    str(bridge_probe_script),
                    "12",
                ],
                background=True,
            ),
        )
    if scenario.writer.graph_db_url:
        next(
            command for command in commands if command.name == "writer-follow-ns3"
        ).argv.extend(
            [
                "--graph-db-url",
                scenario.writer.graph_db_url,
                "--live-graph-snapshot-id",
                resolved_live_graph_snapshot_id,
            ]
        )
    if resolved_topology.source_graph_file:
        next(
            command for command in commands if command.name == "writer-follow-ns3"
        ).argv.extend(["--topology-version", Path(resolved_topology.source_graph_file).name])

    return RunManifest(
        run_id=run_id,
        scenario_id=scenario.scenario_id,
        live_graph_snapshot_id=(resolved_live_graph_snapshot_id if scenario.writer.graph_db_url else None),
        run_dir=str(run_dir),
        compose_file=str(compose_file),
        compose_project_name=scenario.free5gc.project_name,
        free5gc_webui_url=free5gc_webui_url,
        bridge_script=str(bridge_script),
        bridge_probe_script=str(bridge_probe_script),
        bridge_links=[plan.to_dict() for plan in bridge_plans],
        snapshot_file=str(snapshot_file),
        clock_file=str(clock_file),
        ns3_flow_profile_file=str(flow_profile_file),
        ns3_slice_resource_file=str(slice_resource_file),
        user_plane_gate_file=(
            str(user_plane_gate_file) if user_plane_gate_file is not None else None
        ),
        bearer_map_file=str(bearer_map_file) if bearer_map_file is not None else None,
        state_db=str(state_db),
        archive_dir=str(archive_dir),
        ns3_source_file=str(project_root / "sim" / "ns3" / "nr_multignb_multiupf.cc"),
        core_services=core_services,
        ran_services=ran_services,
        subscriber_payloads=[str(path) for path in subscriber_payloads],
        service_map=service_map,
        commands=commands,
    )
