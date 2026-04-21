"""Scenario configuration models and YAML loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from bridge.common.graph_adapter import load_graph_snapshot_payload, merge_semantic_graph_payload
from bridge.common.topology import merge_topology_graph_payload
from bridge.common.ue_context_adapter import load_ue_context_rows, merge_ue_context_payload


def _resolve_path(value: str | Path, base_dir: Path | None = None) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return str(path.resolve())


def _coerce_slice_identifier(sst: int, sd: str) -> str:
    return f"slice-{sst}-{sd.lower()}"


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _default_session_ref(ue_name: str, app_id: str, slice_ref: str, apn: str) -> str:
    return f"{ue_name}:{app_id}:{slice_ref}:{apn}"


@dataclass(slots=True, frozen=True)
class SliceConfig:
    sst: int
    sd: str
    label: str | None = None

    @property
    def slice_id(self) -> str:
        return _coerce_slice_identifier(self.sst, self.sd)


@dataclass(slots=True, frozen=True)
class SessionConfig:
    apn: str
    slice_ref: str
    session_ref: str
    session_type: str = "IPv4"
    five_qi: int = 9
    app_id: str = "default-app"


@dataclass(slots=True, frozen=True)
class SlaTargetConfig:
    latency_ms: float | None = None
    jitter_ms: float | None = None
    loss_rate: float | None = None
    bandwidth_dl_mbps: float | None = None
    bandwidth_ul_mbps: float | None = None
    guaranteed_bandwidth_dl_mbps: float | None = None
    guaranteed_bandwidth_ul_mbps: float | None = None
    priority: int | None = None
    processing_delay_ms: float | None = None


@dataclass(slots=True, frozen=True)
class AppConfig:
    app_id: str
    name: str
    supi: str
    ue_name: str | None = None
    flow_ids: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class FlowConfig:
    flow_id: str
    name: str
    supi: str
    app_id: str
    slice_ref: str
    session_ref: str | None = None
    ue_name: str | None = None
    app_name: str | None = None
    dnn: str | None = None
    five_qi: int = 9
    service_type: str | None = None
    service_type_id: int | None = None
    packet_size_bytes: float | None = None
    arrival_rate_pps: float | None = None
    current_slice_snssai: str | None = None
    allocated_bandwidth_dl_mbps: float | None = None
    allocated_bandwidth_ul_mbps: float | None = None
    optimize_requested: bool | None = None
    policy_filter: str | None = None
    precedence: int | None = None
    qos_ref: int | None = None
    charging_method: str | None = None
    quota: str | None = None
    unit_cost: str | None = None
    sla_target: SlaTargetConfig = field(default_factory=SlaTargetConfig)


@dataclass(slots=True, frozen=True)
class UpfConfig:
    name: str
    role: str = "upf"
    dnn: str = "internet"


@dataclass(slots=True, frozen=True)
class GnbConfig:
    name: str
    alias: str
    tac: int = 1
    nci: str = "0x000000010"
    slices: tuple[str, ...] = ()
    backhaul_upf: str | None = None


@dataclass(slots=True, frozen=True)
class UeConfig:
    name: str
    supi: str
    gnb: str | None
    key: str
    op: str
    op_type: str = "OPC"
    amf: str = "8000"
    free5gc_policy: "Free5gcUePolicyConfig" = field(default_factory=lambda: Free5gcUePolicyConfig())
    sessions: tuple[SessionConfig, ...] = ()


@dataclass(slots=True, frozen=True)
class Free5gcUePolicyConfig:
    target_gnb: str | None = None
    preferred_gnbs: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class Free5gcConfig:
    compose_file: str
    config_root: str
    bridge_name: str = "br-free5gc"
    mode: str = "single_upf"
    project_name: str = "ns3int"


@dataclass(slots=True, frozen=True)
class Ns3Config:
    ns3_root: str
    scratch_name: str = "nr_multignb_multiupf"
    output_subdir: str = "ns3"
    simulator: str = "RealtimeSimulatorImpl"
    sim_time_ms: int = 30000
    bridge_link_rate_mbps: float = 1000.0
    bridge_link_delay_ms: float = 1.0
    policy_reload_ms: int = 1000


@dataclass(slots=True, frozen=True)
class WriterConfig:
    archive_dir: str = "artifacts/archive"
    state_db: str = "artifacts/state/writer.db"
    ingestion_url: str | None = None
    graph_db_url: str | None = None


@dataclass(slots=True, frozen=True)
class TopologyConfig:
    graph_file: str | None = None
    graph_snapshot_id: str | None = None
    graph_db_url: str | None = None


@dataclass(slots=True, frozen=True)
class BridgeHarnessConfig:
    enable_inline_harness: bool = False
    gnb_prefix: str = "tap-gnb"
    upf_prefix: str = "tap-upf"
    bridge_prefix: str = "br-ran"
    host_veth_prefix: str = "veth-ran"


@dataclass(slots=True, frozen=True)
class PolicyConfig:
    db_url: str | None = None
    ue_context_table: str | None = None
    ue_context_query: str | None = None


@dataclass(slots=True, frozen=True)
class ScenarioConfig:
    name: str
    scenario_id: str
    tick_ms: int
    seed: int
    slices: tuple[SliceConfig, ...]
    upfs: tuple[UpfConfig, ...]
    gnbs: tuple[GnbConfig, ...]
    ues: tuple[UeConfig, ...]
    free5gc: Free5gcConfig
    ns3: Ns3Config
    writer: WriterConfig = field(default_factory=WriterConfig)
    topology: TopologyConfig = field(default_factory=TopologyConfig)
    bridge: BridgeHarnessConfig = field(default_factory=BridgeHarnessConfig)
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    apps: tuple[AppConfig, ...] = ()
    flows: tuple[FlowConfig, ...] = ()

    def slice_map(self) -> dict[str, SliceConfig]:
        return {slice_config.slice_id: slice_config for slice_config in self.slices}

    def gnb_map(self) -> dict[str, GnbConfig]:
        return {gnb.name: gnb for gnb in self.gnbs}

    def app_map(self) -> dict[str, AppConfig]:
        return {app.app_id: app for app in self.apps}

    def flow_map(self) -> dict[str, FlowConfig]:
        return {flow.flow_id: flow for flow in self.flows}

    def flows_for_ue(self, ue_name: str) -> tuple[FlowConfig, ...]:
        return tuple(flow for flow in self.flows if flow.ue_name == ue_name)

    def resolve_flow_session(self, ue: UeConfig, flow: FlowConfig) -> SessionConfig:
        if flow.session_ref is not None:
            for session in ue.sessions:
                if session.session_ref == flow.session_ref:
                    if session.slice_ref != flow.slice_ref:
                        raise ValueError(
                            f"flow {flow.flow_id} session {flow.session_ref} uses slice {session.slice_ref}, "
                            f"not {flow.slice_ref}"
                        )
                    if flow.dnn is not None and session.apn != flow.dnn:
                        raise ValueError(
                            f"flow {flow.flow_id} session {flow.session_ref} uses DNN {session.apn}, not {flow.dnn}"
                        )
                    return session
            raise ValueError(f"flow {flow.flow_id} references unknown session {flow.session_ref} for UE {ue.name}")

        app_matches = [session for session in ue.sessions if session.app_id == flow.app_id]
        if len(app_matches) == 1:
            session = app_matches[0]
            if session.slice_ref != flow.slice_ref:
                raise ValueError(
                    f"flow {flow.flow_id} app {flow.app_id} resolves to session slice {session.slice_ref}, "
                    f"not {flow.slice_ref}"
                )
            if flow.dnn is not None and session.apn != flow.dnn:
                raise ValueError(
                    f"flow {flow.flow_id} app {flow.app_id} resolves to DNN {session.apn}, not {flow.dnn}"
                )
            return session

        candidate_sessions = [session for session in ue.sessions if session.slice_ref == flow.slice_ref]
        if flow.dnn is not None:
            candidate_sessions = [session for session in candidate_sessions if session.apn == flow.dnn]
        if len(candidate_sessions) == 1:
            return candidate_sessions[0]

        if len(ue.sessions) == 1:
            session = ue.sessions[0]
            if session.slice_ref != flow.slice_ref:
                raise ValueError(
                    f"flow {flow.flow_id} uses slice {flow.slice_ref}, but UE {ue.name} only has {session.slice_ref}"
                )
            if flow.dnn is not None and session.apn != flow.dnn:
                raise ValueError(
                    f"flow {flow.flow_id} uses DNN {flow.dnn}, but UE {ue.name} only has {session.apn}"
                )
            return session

        raise ValueError(
            f"flow {flow.flow_id} cannot be mapped unambiguously to a session on UE {ue.name}; "
            f"set flow.session_ref explicitly"
        )

    def ue_groups(self) -> dict[str, list[UeConfig]]:
        grouped: dict[str, list[UeConfig]] = {gnb.name: [] for gnb in self.gnbs}
        for ue in self.ues:
            target_gnb = ue.gnb or ue.free5gc_policy.target_gnb
            if target_gnb is None and ue.free5gc_policy.preferred_gnbs:
                target_gnb = ue.free5gc_policy.preferred_gnbs[0]
            if target_gnb is None:
                continue
            grouped.setdefault(target_gnb, []).append(ue)
        return grouped

    def validate(self) -> None:
        slices = self.slice_map()
        gnbs = self.gnb_map()
        if not self.gnbs:
            raise ValueError("scenario must define at least one gNB")
        if not self.ues:
            raise ValueError("scenario must define at least one UE")
        if self.topology.graph_file and self.topology.graph_snapshot_id:
            raise ValueError("topology.graph_file and topology.graph_snapshot_id are mutually exclusive")
        if self.topology.graph_file and not Path(self.topology.graph_file).exists():
            raise ValueError(f"topology graph file does not exist: {self.topology.graph_file}")
        if self.topology.graph_snapshot_id and not (self.topology.graph_db_url or self.writer.graph_db_url):
            raise ValueError("graph snapshot input requires topology.graph_db_url or writer.graph_db_url")
        if (self.policy.ue_context_table or self.policy.ue_context_query) and not self.policy.db_url:
            raise ValueError("ue_context policy loading requires policy.db_url or writer.graph_db_url")

        ue_by_name = {ue.name: ue for ue in self.ues}
        ue_by_supi = {ue.supi: ue for ue in self.ues}
        app_ids = {app.app_id for app in self.apps}
        for ue in self.ues:
            if (
                ue.gnb is None
                and ue.free5gc_policy.target_gnb is None
                and not ue.free5gc_policy.preferred_gnbs
                and not self.topology.graph_file
                and not self.topology.graph_snapshot_id
            ):
                raise ValueError(
                    f"UE {ue.name} must define gnb, free5gc_policy target/preference, or a topology graph"
                )
            gnb_references = []
            if ue.gnb is not None:
                gnb_references.append(ue.gnb)
            if ue.free5gc_policy.target_gnb is not None:
                gnb_references.append(ue.free5gc_policy.target_gnb)
            gnb_references.extend(ue.free5gc_policy.preferred_gnbs)
            for gnb_name in gnb_references:
                if gnb_name not in gnbs:
                    raise ValueError(f"UE {ue.name} references unknown gNB {gnb_name}")
            if not ue.sessions:
                raise ValueError(f"UE {ue.name} must define at least one session")
            seen_session_refs: set[str] = set()
            for session in ue.sessions:
                if session.slice_ref not in slices:
                    raise ValueError(
                        f"UE {ue.name} session references unknown slice {session.slice_ref}"
                    )
                if session.session_ref in seen_session_refs:
                    raise ValueError(f"UE {ue.name} defines duplicate session_ref {session.session_ref}")
                seen_session_refs.add(session.session_ref)
        for app in self.apps:
            if app.supi not in ue_by_supi:
                raise ValueError(f"app {app.app_id} references unknown SUPI {app.supi}")
            if app.ue_name is not None and app.ue_name not in ue_by_name:
                raise ValueError(f"app {app.app_id} references unknown UE {app.ue_name}")
        for flow in self.flows:
            if flow.supi not in ue_by_supi:
                raise ValueError(f"flow {flow.flow_id} references unknown SUPI {flow.supi}")
            if flow.ue_name is not None and flow.ue_name not in ue_by_name:
                raise ValueError(f"flow {flow.flow_id} references unknown UE {flow.ue_name}")
            if flow.ue_name is not None and ue_by_name[flow.ue_name].supi != flow.supi:
                raise ValueError(
                    f"flow {flow.flow_id} references UE {flow.ue_name} and SUPI {flow.supi}, but they do not match"
                )
            if flow.slice_ref not in slices:
                raise ValueError(f"flow {flow.flow_id} references unknown slice {flow.slice_ref}")
            if app_ids and flow.app_id not in app_ids:
                raise ValueError(f"flow {flow.flow_id} references unknown app {flow.app_id}")
            target_ue = ue_by_name[flow.ue_name] if flow.ue_name is not None else ue_by_supi[flow.supi]
            self.resolve_flow_session(target_ue, flow)

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        base_dir: Path | None = None,
    ) -> "ScenarioConfig":
        writer_payload = payload.get("writer", {})
        topology_payload = dict(payload.get("topology", {}))
        policy_payload = dict(payload.get("policy", {}))
        resolved_graph_file = (
            _resolve_path(topology_payload["graph_file"], base_dir)
            if topology_payload.get("graph_file")
            else None
        )
        resolved_graph_db_url = topology_payload.get("graph_db_url") or writer_payload.get("graph_db_url")
        resolved_graph_snapshot_id = topology_payload.get("graph_snapshot_id")
        resolved_policy_db_url = policy_payload.get("db_url") or writer_payload.get("graph_db_url")

        if resolved_graph_snapshot_id is not None:
            if resolved_graph_db_url is None:
                raise ValueError("topology.graph_snapshot_id requires topology.graph_db_url or writer.graph_db_url")
            payload = merge_semantic_graph_payload(
                payload,
                load_graph_snapshot_payload(resolved_graph_db_url, str(resolved_graph_snapshot_id)),
            )
            topology_payload = dict(payload.get("topology", {}))
        if resolved_graph_file is not None:
            payload = merge_topology_graph_payload(payload, resolved_graph_file)
            topology_payload = dict(payload.get("topology", {}))
        if policy_payload.get("ue_context_table") or policy_payload.get("ue_context_query"):
            if resolved_policy_db_url is None:
                raise ValueError("ue_context policy loading requires policy.db_url or writer.graph_db_url")
            payload = merge_ue_context_payload(
                payload,
                load_ue_context_rows(
                    resolved_policy_db_url,
                    table_name=policy_payload.get("ue_context_table", "ue_context"),
                    query=policy_payload.get("ue_context_query"),
                ),
            )
            policy_payload = dict(payload.get("policy", policy_payload))

        slices = tuple(
            SliceConfig(
                sst=int(item["sst"]),
                sd=str(item["sd"]),
                label=item.get("label"),
            )
            for item in payload.get("slices", [])
        )

        slice_map = {_coerce_slice_identifier(item.sst, item.sd): item for item in slices}
        upfs = tuple(
            UpfConfig(
                name=item["name"],
                role=item.get("role", "upf"),
                dnn=item.get("dnn", "internet"),
            )
            for item in payload.get("upfs", [])
        )

        gnbs = []
        for index, item in enumerate(payload.get("gnbs", []), start=1):
            alias = item.get("alias") or f"gnb-{index}.free5gc.org"
            gnb = GnbConfig(
                name=item["name"],
                alias=alias,
                tac=int(item.get("tac", 1)),
                nci=str(item.get("nci", f"0x{index:09x}")),
                slices=tuple(item.get("slices", tuple(slice_map.keys()))),
                backhaul_upf=item.get("backhaul_upf"),
            )
            gnbs.append(gnb)

        ues = []
        for item in payload.get("ues", []):
            free5gc_policy_payload = item.get("free5gc_policy", {})
            sessions = tuple(
                SessionConfig(
                    apn=session.get("apn", "internet"),
                    slice_ref=session["slice_ref"],
                    session_ref=session.get(
                        "session_ref",
                        _default_session_ref(
                            item["name"],
                            session.get("app_id", f"app-{item['name']}"),
                            session["slice_ref"],
                            session.get("apn", "internet"),
                        ),
                    ),
                    session_type=session.get("type", "IPv4"),
                    five_qi=int(session.get("five_qi", 9)),
                    app_id=session.get("app_id", f"app-{item['name']}"),
                )
                for session in item.get("sessions", [])
            )
            ues.append(
                UeConfig(
                    name=item["name"],
                    supi=item["supi"],
                    gnb=item.get("gnb"),
                    key=item["key"],
                    op=item["op"],
                    op_type=item.get("op_type", "OPC"),
                    amf=str(item.get("amf", "8000")),
                    free5gc_policy=Free5gcUePolicyConfig(
                        target_gnb=free5gc_policy_payload.get("target_gnb"),
                        preferred_gnbs=tuple(free5gc_policy_payload.get("preferred_gnbs", ())),
                    ),
                    sessions=sessions,
                )
            )

        free5gc_payload = payload["free5gc"]
        ns3_payload = payload["ns3"]
        bridge_payload = payload.get("bridge", {})
        apps = tuple(
            AppConfig(
                app_id=item["app_id"],
                name=item.get("name", item["app_id"]),
                supi=item["supi"],
                ue_name=item.get("ue_name"),
                flow_ids=tuple(item.get("flow_ids", ())),
            )
            for item in payload.get("apps", [])
        )
        flows = tuple(
            FlowConfig(
                flow_id=item["flow_id"],
                name=item.get("name", item["flow_id"]),
                supi=item["supi"],
                app_id=item["app_id"],
                slice_ref=item["slice_ref"],
                session_ref=item.get("session_ref"),
                ue_name=item.get("ue_name"),
                app_name=item.get("app_name"),
                dnn=item.get("dnn"),
                five_qi=int(item.get("five_qi", 9)),
                service_type=item.get("service_type"),
                service_type_id=_optional_int(item.get("service_type_id")),
                packet_size_bytes=_optional_float(item.get("packet_size_bytes")),
                arrival_rate_pps=_optional_float(item.get("arrival_rate_pps")),
                current_slice_snssai=item.get("current_slice_snssai"),
                allocated_bandwidth_dl_mbps=_optional_float(item.get("allocated_bandwidth_dl_mbps")),
                allocated_bandwidth_ul_mbps=_optional_float(item.get("allocated_bandwidth_ul_mbps")),
                optimize_requested=(
                    bool(item["optimize_requested"])
                    if item.get("optimize_requested") is not None
                    else None
                ),
                policy_filter=item.get("policy_filter") or item.get("filter"),
                precedence=_optional_int(item.get("precedence")),
                qos_ref=_optional_int(item.get("qos_ref") or item.get("qosRef") or item.get("qfi")),
                charging_method=item.get("charging_method") or item.get("chargingMethod"),
                quota=item.get("quota"),
                unit_cost=item.get("unit_cost") or item.get("unitCost"),
                sla_target=SlaTargetConfig(
                    latency_ms=_optional_float(item.get("sla_target", {}).get("latency_ms"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    jitter_ms=_optional_float(item.get("sla_target", {}).get("jitter_ms"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    loss_rate=_optional_float(item.get("sla_target", {}).get("loss_rate"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    bandwidth_dl_mbps=_optional_float(item.get("sla_target", {}).get("bandwidth_dl_mbps"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    bandwidth_ul_mbps=_optional_float(item.get("sla_target", {}).get("bandwidth_ul_mbps"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    guaranteed_bandwidth_dl_mbps=_optional_float(
                        item.get("sla_target", {}).get("guaranteed_bandwidth_dl_mbps")
                    )
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    guaranteed_bandwidth_ul_mbps=_optional_float(
                        item.get("sla_target", {}).get("guaranteed_bandwidth_ul_mbps")
                    )
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    priority=_optional_int(item.get("sla_target", {}).get("priority"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                    processing_delay_ms=_optional_float(item.get("sla_target", {}).get("processing_delay_ms"))
                    if isinstance(item.get("sla_target"), dict)
                    else None,
                ),
            )
            for item in payload.get("flows", [])
        )

        scenario = cls(
            name=payload["name"],
            scenario_id=payload["scenario_id"],
            tick_ms=int(payload.get("tick_ms", 1000)),
            seed=int(payload.get("seed", 1)),
            slices=slices,
            upfs=upfs,
            gnbs=tuple(gnbs),
            ues=tuple(ues),
            apps=apps,
            flows=flows,
            free5gc=Free5gcConfig(
                compose_file=_resolve_path(free5gc_payload["compose_file"], base_dir),
                config_root=_resolve_path(free5gc_payload["config_root"], base_dir),
                bridge_name=free5gc_payload.get("bridge_name", "br-free5gc"),
                mode=free5gc_payload.get("mode", "single_upf"),
                project_name=free5gc_payload.get("project_name", "ns3int"),
            ),
            ns3=Ns3Config(
                ns3_root=_resolve_path(ns3_payload["ns3_root"], base_dir),
                scratch_name=ns3_payload.get("scratch_name", "nr_multignb_multiupf"),
                output_subdir=ns3_payload.get("output_subdir", "ns3"),
                simulator=ns3_payload.get("simulator", "RealtimeSimulatorImpl"),
                sim_time_ms=int(ns3_payload.get("sim_time_ms", 30000)),
                bridge_link_rate_mbps=float(ns3_payload.get("bridge_link_rate_mbps", 1000.0)),
                bridge_link_delay_ms=float(ns3_payload.get("bridge_link_delay_ms", 1.0)),
                policy_reload_ms=int(ns3_payload.get("policy_reload_ms", 1000)),
            ),
            writer=WriterConfig(
                archive_dir=writer_payload.get("archive_dir", "artifacts/archive"),
                state_db=writer_payload.get("state_db", "artifacts/state/writer.db"),
                ingestion_url=writer_payload.get("ingestion_url"),
                graph_db_url=writer_payload.get("graph_db_url"),
            ),
            topology=TopologyConfig(
                graph_file=resolved_graph_file,
                graph_snapshot_id=(str(resolved_graph_snapshot_id) if resolved_graph_snapshot_id is not None else None),
                graph_db_url=resolved_graph_db_url,
            ),
            bridge=BridgeHarnessConfig(
                enable_inline_harness=bool(
                    bridge_payload.get("enable_inline_harness", False)
                ),
                gnb_prefix=bridge_payload.get("gnb_prefix", "tap-gnb"),
                upf_prefix=bridge_payload.get("upf_prefix", "tap-upf"),
                bridge_prefix=bridge_payload.get("bridge_prefix", "br-ran"),
                host_veth_prefix=bridge_payload.get("host_veth_prefix", "veth-ran"),
            ),
            policy=PolicyConfig(
                db_url=resolved_policy_db_url,
                ue_context_table=policy_payload.get("ue_context_table"),
                ue_context_query=policy_payload.get("ue_context_query"),
            ),
        )
        scenario.validate()
        return scenario


def load_scenario(path: str | Path) -> ScenarioConfig:
    resolved_path = Path(path).expanduser().resolve()
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("scenario YAML root must be a mapping")
    return ScenarioConfig.from_dict(payload, base_dir=resolved_path.parent)