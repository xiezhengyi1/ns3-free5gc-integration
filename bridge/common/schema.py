"""Tick snapshot schema used between ns-3, writer, and ingestion."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _require(value: dict[str, Any], key: str) -> Any:
    if key not in value:
        raise ValueError(f"missing required field: {key}")
    return value[key]


def _number(value: Any, key: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"field {key} must be numeric") from exc


@dataclass(slots=True)
class NodeRecord:
    id: str
    type: str
    label: str
    attributes: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeRecord":
        return cls(
            id=str(_require(payload, "id")),
            type=str(_require(payload, "type")),
            label=str(payload.get("label", payload["id"])),
            attributes=dict(payload.get("attributes", {})),
        )


@dataclass(slots=True)
class LinkRecord:
    source: str
    target: str
    type: str
    attributes: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LinkRecord":
        return cls(
            source=str(_require(payload, "source")),
            target=str(_require(payload, "target")),
            type=str(_require(payload, "type")),
            attributes=dict(payload.get("attributes", {})),
        )


@dataclass(slots=True)
class GnbRecord:
    gnb_id: str
    node_id: str
    alias: str
    attached_ues: list[str] = field(default_factory=list)
    dst_upf: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GnbRecord":
        return cls(
            gnb_id=str(_require(payload, "gnb_id")),
            node_id=str(_require(payload, "node_id")),
            alias=str(_require(payload, "alias")),
            attached_ues=[str(item) for item in payload.get("attached_ues", [])],
            dst_upf=payload.get("dst_upf"),
        )


@dataclass(slots=True)
class UeRecord:
    ue_id: str
    supi: str
    gnb_id: str
    slice_id: str
    ip_address: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "UeRecord":
        return cls(
            ue_id=str(_require(payload, "ue_id")),
            supi=str(_require(payload, "supi")),
            gnb_id=str(_require(payload, "gnb_id")),
            slice_id=str(_require(payload, "slice_id")),
            ip_address=payload.get("ip_address"),
        )


@dataclass(slots=True)
class SliceRecord:
    slice_id: str
    sst: int
    sd: str
    label: str | None = None
    resource: dict[str, Any] = field(default_factory=dict)
    telemetry: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SliceRecord":
        return cls(
            slice_id=str(_require(payload, "slice_id")),
            sst=int(_require(payload, "sst")),
            sd=str(_require(payload, "sd")),
            label=payload.get("label"),
            resource=dict(payload.get("resource", {})),
            telemetry=dict(payload.get("telemetry", {})),
        )


@dataclass(slots=True)
class FlowRecord:
    flow_id: str
    supi: str
    app_id: str
    src_gnb: str
    dst_upf: str
    slice_id: str
    five_qi: int
    delay_ms: float
    jitter_ms: float
    loss_rate: float
    throughput_ul_mbps: float
    throughput_dl_mbps: float
    queue_bytes: int
    rlc_buffer_bytes: int
    session_ref: str | None = None
    name: str | None = None
    app_name: str | None = None
    service: dict[str, Any] = field(default_factory=dict)
    traffic: dict[str, Any] = field(default_factory=dict)
    sla: dict[str, Any] = field(default_factory=dict)
    allocation: dict[str, Any] = field(default_factory=dict)
    telemetry: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FlowRecord":
        return cls(
            flow_id=str(_require(payload, "flow_id")),
            supi=str(_require(payload, "supi")),
            app_id=str(_require(payload, "app_id")),
            src_gnb=str(_require(payload, "src_gnb")),
            dst_upf=str(_require(payload, "dst_upf")),
            slice_id=str(_require(payload, "slice_id")),
            session_ref=(str(payload["session_ref"]) if payload.get("session_ref") is not None else None),
            five_qi=int(_require(payload, "5qi")),
            delay_ms=_number(_require(payload, "delay_ms"), "delay_ms"),
            jitter_ms=_number(_require(payload, "jitter_ms"), "jitter_ms"),
            loss_rate=_number(_require(payload, "loss_rate"), "loss_rate"),
            throughput_ul_mbps=_number(
                _require(payload, "throughput_ul_mbps"),
                "throughput_ul_mbps",
            ),
            throughput_dl_mbps=_number(
                _require(payload, "throughput_dl_mbps"),
                "throughput_dl_mbps",
            ),
            queue_bytes=int(_require(payload, "queue_bytes")),
            rlc_buffer_bytes=int(_require(payload, "rlc_buffer_bytes")),
            name=(str(payload["name"]) if payload.get("name") is not None else None),
            app_name=(str(payload["app_name"]) if payload.get("app_name") is not None else None),
            service=dict(payload.get("service", {})),
            traffic=dict(payload.get("traffic", {})),
            sla=dict(payload.get("sla", {})),
            allocation=dict(payload.get("allocation", {})),
            telemetry=dict(payload.get("telemetry", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["5qi"] = payload.pop("five_qi")
        return payload


@dataclass(slots=True)
class TickSnapshot:
    run_id: str
    scenario_id: str
    tick_index: int
    sim_time_ms: int
    nodes: list[NodeRecord]
    links: list[LinkRecord]
    gnbs: list[GnbRecord]
    ues: list[UeRecord]
    flows: list[FlowRecord]
    slices: list[SliceRecord]
    kpis: dict[str, float]
    reward_inputs: dict[str, float]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TickSnapshot":
        snapshot = cls(
            run_id=str(_require(payload, "run_id")),
            scenario_id=str(_require(payload, "scenario_id")),
            tick_index=int(_require(payload, "tick_index")),
            sim_time_ms=int(_require(payload, "sim_time_ms")),
            nodes=[NodeRecord.from_dict(item) for item in _require(payload, "nodes")],
            links=[LinkRecord.from_dict(item) for item in _require(payload, "links")],
            gnbs=[GnbRecord.from_dict(item) for item in _require(payload, "gnbs")],
            ues=[UeRecord.from_dict(item) for item in _require(payload, "ues")],
            flows=[FlowRecord.from_dict(item) for item in _require(payload, "flows")],
            slices=[SliceRecord.from_dict(item) for item in _require(payload, "slices")],
            kpis={key: float(value) for key, value in _require(payload, "kpis").items()},
            reward_inputs={
                key: float(value)
                for key, value in _require(payload, "reward_inputs").items()
            },
        )
        snapshot.validate()
        return snapshot

    def validate(self) -> None:
        if self.tick_index < 0:
            raise ValueError("tick_index must be >= 0")
        if self.sim_time_ms < 0:
            raise ValueError("sim_time_ms must be >= 0")
        node_ids = {node.id for node in self.nodes}
        slice_ids = {item.slice_id for item in self.slices}
        gnb_ids = {item.gnb_id for item in self.gnbs}
        supis = {item.supi for item in self.ues}

        for link in self.links:
            if link.source not in node_ids or link.target not in node_ids:
                raise ValueError("links must reference known nodes")
        for gnb in self.gnbs:
            if gnb.node_id not in node_ids:
                raise ValueError(f"gNB {gnb.gnb_id} references unknown node {gnb.node_id}")
        for ue in self.ues:
            if ue.gnb_id not in gnb_ids:
                raise ValueError(f"UE {ue.ue_id} references unknown gNB {ue.gnb_id}")
            if ue.slice_id not in slice_ids:
                raise ValueError(f"UE {ue.ue_id} references unknown slice {ue.slice_id}")
        for flow in self.flows:
            if flow.supi not in supis:
                raise ValueError(f"flow {flow.flow_id} references unknown SUPI {flow.supi}")
            if flow.src_gnb not in gnb_ids:
                raise ValueError(f"flow {flow.flow_id} references unknown gNB {flow.src_gnb}")
            if flow.slice_id not in slice_ids:
                raise ValueError(f"flow {flow.flow_id} references unknown slice {flow.slice_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "scenario_id": self.scenario_id,
            "tick_index": self.tick_index,
            "sim_time_ms": self.sim_time_ms,
            "nodes": [asdict(item) for item in self.nodes],
            "links": [asdict(item) for item in self.links],
            "gnbs": [asdict(item) for item in self.gnbs],
            "ues": [asdict(item) for item in self.ues],
            "flows": [item.to_dict() for item in self.flows],
            "slices": [asdict(item) for item in self.slices],
            "kpis": dict(self.kpis),
            "reward_inputs": dict(self.reward_inputs),
        }


@dataclass(slots=True)
class SimEvent:
    run_id: str
    tick_index: int
    event_type: str
    entity_type: str
    entity_id: str
    payload_json: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SimEvent":
        return cls(
            run_id=str(_require(payload, "run_id")),
            tick_index=int(_require(payload, "tick_index")),
            event_type=str(_require(payload, "event_type")),
            entity_type=str(_require(payload, "entity_type")),
            entity_id=str(_require(payload, "entity_id")),
            payload_json=dict(payload.get("payload_json", {})),
            created_at=str(
                payload.get(
                    "created_at",
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
