"""Manifest types for split-mode runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class SplitCommandSpec:
    name: str
    cwd: str
    argv: list[str]
    background: bool = False
    env: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class SplitRunManifest:
    run_id: str
    scenario_id: str
    live_graph_snapshot_id: str | None
    run_dir: str
    compose_file: str
    compose_project_name: str
    snapshot_file: str
    clock_file: str
    state_db: str
    archive_dir: str
    flow_profile_file: str
    slice_resource_file: str
    runtime_state_file: str
    result_file: str
    subscriber_payloads: list[str]
    core_services: list[str]
    ran_services: list[str]
    service_map: dict[str, dict[str, str]]
    commands: list[SplitCommandSpec]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["commands"] = [asdict(item) for item in self.commands]
        return payload
