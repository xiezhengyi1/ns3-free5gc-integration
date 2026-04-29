"""Map tick snapshots into graph snapshot rows for PostgreSQL persistence."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from bridge.common.schema import FlowRecord, TickSnapshot


GRAPH_ROW_DELETED = "__deleted__"
_RAN_NODE_TYPE = "AN"
_CORE_NODE_TYPE = "CN"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _edge_key(edge_type: str, source_key: str, target_key: str) -> str:
    del edge_type
    return f"{source_key}->{target_key}"


def _stable_name(value: str) -> str:
    return value.strip().replace(" ", "_")


def _snssai(sst: int, sd: str) -> str:
    return f"{sst:02d}{sd.lower()}"


def _slice_node_key_from_values(sst: int, sd: str) -> str:
    return f"slice:{_snssai(sst, sd)}"


def _ue_node_key(supi: str) -> str:
    return f"ue:{supi}"


def _app_node_key(supi: str, app_id: str) -> str:
    return f"app:{supi}:{app_id}"


def _flow_node_key(flow: FlowRecord) -> str:
    return f"flow:{flow.supi}:{flow.app_id}:{flow.flow_id}"


def _session_node_key(supi: str, session_ref: str) -> str:
    return f"session:{supi}:{session_ref}"


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _numeric_value(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _summary_node_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "node_key": row["node_key"],
        "node_type": row["node_type"],
        "label": row.get("label"),
        "properties": _normalized_properties(row.get("properties")),
    }


def _summary_edge_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "edge_key": row["edge_key"],
        "edge_type": row["edge_type"],
        "source_key": row["source_key"],
        "target_key": row["target_key"],
        "properties": _normalized_properties(row.get("properties")),
    }


def _summary_metric_row(row: dict[str, object]) -> dict[str, object]:
    observed_at = row.get("observed_at")
    if isinstance(observed_at, datetime):
        observed_at = observed_at.isoformat()
    return {
        "owner_type": row["owner_type"],
        "owner_key": row["owner_key"],
        "metric_name": row["metric_name"],
        "metric_value": row["metric_value"],
        "observed_at": observed_at,
    }


def _append_numeric_property_metrics(
    rows: list[dict[str, object]],
    snapshot_id: str,
    owner_key: str,
    properties: dict[str, object],
    observed_at: datetime,
    *,
    include: tuple[str, ...] | None = None,
    exclude: tuple[str, ...] = (),
) -> None:
    include_set = set(include or ())
    exclude_set = set(exclude)
    for metric_name, metric_value in properties.items():
        if metric_name in exclude_set:
            continue
        if include is not None and metric_name not in include_set:
            continue
        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            continue
        _append_metric(rows, snapshot_id, "node", owner_key, metric_name, metric_value, observed_at)


def _dict_properties(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _append_nested_numeric_metrics(
    rows: list[dict[str, object]],
    snapshot_id: str,
    owner_key: str,
    prefix: str,
    properties: dict[str, object],
    observed_at: datetime,
) -> None:
    for metric_name, metric_value in properties.items():
        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            continue
        _append_metric(
            rows,
            snapshot_id,
            "node",
            owner_key,
            f"{prefix}.{metric_name}",
            metric_value,
            observed_at,
        )


def _flow_service_properties(flow: FlowRecord) -> dict[str, object]:
    return _dict_properties(flow.service)


def _flow_traffic_properties(flow: FlowRecord) -> dict[str, object]:
    properties = _dict_properties(flow.traffic)
    if isinstance(properties.get("five_tuple"), dict):
        properties.setdefault("direction", "downlink")
        properties.setdefault("source_entity", "ns3_remote_host")
        properties.setdefault("destination_entity", "ue_pdu_ip")
    return properties


def _flow_dnn(flow: FlowRecord) -> str | None:
    service = _flow_service_properties(flow)
    for key in ("dnn", "apn"):
        value = service.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _flow_session_ref(flow: FlowRecord) -> str | None:
    if flow.session_ref:
        return flow.session_ref
    for container in (flow.traffic, flow.service, flow.allocation):
        if not isinstance(container, dict):
            continue
        for key in ("session_ref", "sessionRef"):
            value = container.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _flow_sla_properties(flow: FlowRecord, *, include_observed_fallback: bool = True) -> dict[str, object]:
    properties = _dict_properties(flow.sla)
    if include_observed_fallback:
        properties.setdefault("latency", flow.delay_ms)
        properties.setdefault("jitter", flow.jitter_ms)
        properties.setdefault("loss_rate", flow.loss_rate)
        properties.setdefault("bandwidth_dl", flow.throughput_dl_mbps)
        properties.setdefault("bandwidth_ul", flow.throughput_ul_mbps)
    return properties


def _flow_allocation_properties(
    flow: FlowRecord,
    slice_node_key: str,
    *,
    include_observed_fallback: bool = True,
) -> dict[str, object]:
    properties = _dict_properties(flow.allocation)
    snssai = slice_node_key.split(":", 1)[1]
    properties.setdefault("current_slice_snssai", snssai)
    sla = _flow_sla_properties(flow, include_observed_fallback=include_observed_fallback)
    if include_observed_fallback:
        properties.setdefault("allocated_bandwidth_dl", sla.get("bandwidth_dl", flow.throughput_dl_mbps))
        properties.setdefault("allocated_bandwidth_ul", sla.get("bandwidth_ul", flow.throughput_ul_mbps))
    properties.setdefault("optimize_requested", False)
    return properties


def _flow_telemetry_properties(flow: FlowRecord) -> dict[str, object]:
    properties = _dict_properties(flow.telemetry)
    properties.setdefault("latency", flow.delay_ms)
    properties.setdefault("jitter", flow.jitter_ms)
    properties.setdefault("loss_rate", flow.loss_rate)
    properties.setdefault("throughput_dl", flow.throughput_dl_mbps)
    properties.setdefault("throughput_ul", flow.throughput_ul_mbps)
    properties.setdefault("packet_sent", None)
    properties.setdefault("packet_received", None)
    return properties


def _flow_node_properties(flow: FlowRecord, slice_node_key: str) -> dict[str, object]:
    properties = {
        "id": flow.flow_id,
        "name": flow.name or flow.flow_id,
        "supi": flow.supi,
        "app_id": flow.app_id,
        "app_name": flow.app_name or flow.app_id,
        "slice_ref": flow.slice_id,
        "service": _flow_service_properties(flow),
        "traffic": _flow_traffic_properties(flow),
        "sla": _flow_sla_properties(flow, include_observed_fallback=False),
        "allocation": _flow_allocation_properties(
            flow,
            slice_node_key,
            include_observed_fallback=False,
        ),
    }
    session_ref = _flow_session_ref(flow)
    if session_ref is not None:
        properties["session_ref"] = session_ref
    dnn = _flow_dnn(flow)
    if dnn is not None:
        properties["dnn"] = dnn
    return properties


def _summary_flow_properties(flow: FlowRecord, slice_node_key: str) -> dict[str, object]:
    properties = _flow_node_properties(flow, slice_node_key)
    properties["sla"] = _flow_sla_properties(flow, include_observed_fallback=True)
    properties["allocation"] = _flow_allocation_properties(
        flow,
        slice_node_key,
        include_observed_fallback=True,
    )
    properties["telemetry"] = _flow_telemetry_properties(flow)
    return properties


def _node_capacity_properties(raw_properties: dict[str, object]) -> dict[str, object]:
    return _dict_properties(raw_properties.get("capacity"))


def _node_telemetry_properties(raw_properties: dict[str, object]) -> dict[str, object]:
    return _dict_properties(raw_properties.get("telemetry"))


@dataclass(slots=True)
class GraphSnapshotBundle:
    snapshot_row: dict[str, object]
    node_rows: list[dict[str, object]]
    edge_rows: list[dict[str, object]]
    metric_rows: list[dict[str, object]]


def _normalized_properties(properties: object) -> dict[str, object]:
    return dict(properties) if isinstance(properties, dict) else {}


def is_graph_row_deleted(row: dict[str, object]) -> bool:
    return bool(_normalized_properties(row.get("properties")).get(GRAPH_ROW_DELETED))


def _canonical_node_row(row: dict[str, object] | None) -> dict[str, object] | None:
    if row is None:
        return None
    properties = _normalized_properties(row.get("properties"))
    if row["node_type"] == "slice":
        properties = {
            key: value
            for key, value in properties.items()
            if key not in {"capacity", "load", "qos", "telemetry"}
        }
    return {
        "node_key": row["node_key"],
        "node_type": row["node_type"],
        "label": row.get("label"),
        "properties": properties,
    }


def _canonical_edge_row(row: dict[str, object] | None) -> dict[str, object] | None:
    if row is None:
        return None
    return {
        "edge_key": row["edge_key"],
        "edge_type": row["edge_type"],
        "source_key": row["source_key"],
        "target_key": row["target_key"],
        "properties": _normalized_properties(row.get("properties")),
    }


def _node_tombstone(snapshot_id: str, row: dict[str, object]) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "node_key": row["node_key"],
        "node_type": row["node_type"],
        "label": row.get("label"),
        "properties": {GRAPH_ROW_DELETED: True},
    }


def _edge_tombstone(snapshot_id: str, row: dict[str, object]) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "edge_key": row["edge_key"],
        "edge_type": row["edge_type"],
        "source_key": row["source_key"],
        "target_key": row["target_key"],
        "properties": {GRAPH_ROW_DELETED: True},
    }


def build_delta_graph_snapshot_bundle(
    bundle: GraphSnapshotBundle,
    previous_node_rows: dict[str, dict[str, object]],
    previous_edge_rows: dict[str, dict[str, object]],
) -> GraphSnapshotBundle:
    snapshot_id = str(bundle.snapshot_row["snapshot_id"])
    current_node_rows = {row["node_key"]: row for row in bundle.node_rows}
    current_edge_rows = {row["edge_key"]: row for row in bundle.edge_rows}

    delta_node_rows: list[dict[str, object]] = []
    delta_edge_rows: list[dict[str, object]] = []
    deleted_node_count = 0
    deleted_edge_count = 0

    for node_key, row in current_node_rows.items():
        if _canonical_node_row(row) != _canonical_node_row(previous_node_rows.get(node_key)):
            delta_node_rows.append({**row, "snapshot_id": snapshot_id})

    for node_key, row in previous_node_rows.items():
        if node_key in current_node_rows:
            continue
        deleted_node_count += 1
        delta_node_rows.append(_node_tombstone(snapshot_id, row))

    for edge_key, row in current_edge_rows.items():
        if _canonical_edge_row(row) != _canonical_edge_row(previous_edge_rows.get(edge_key)):
            delta_edge_rows.append({**row, "snapshot_id": snapshot_id})

    for edge_key, row in previous_edge_rows.items():
        if edge_key in current_edge_rows:
            continue
        deleted_edge_count += 1
        delta_edge_rows.append(_edge_tombstone(snapshot_id, row))

    graph_summary = dict(bundle.snapshot_row.get("graph_summary", {}))
    graph_summary.update(
        {
            "write_mode": "delta" if previous_node_rows or previous_edge_rows else "snapshot",
            "delta_node_count": len(delta_node_rows),
            "delta_edge_count": len(delta_edge_rows),
            "delta_metric_count": len(bundle.metric_rows),
            "deleted_node_count": deleted_node_count,
            "deleted_edge_count": deleted_edge_count,
        }
    )

    snapshot_row = dict(bundle.snapshot_row)
    snapshot_row["graph_summary"] = graph_summary

    return GraphSnapshotBundle(
        snapshot_row=snapshot_row,
        node_rows=delta_node_rows,
        edge_rows=delta_edge_rows,
        metric_rows=list(bundle.metric_rows),
    )


def _resolve_gnb_node_key(snapshot: TickSnapshot) -> dict[str, str]:
    return {item.gnb_id: item.node_id for item in snapshot.gnbs}


def _resolve_core_node_key(snapshot: TickSnapshot) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for node in snapshot.nodes:
        if node.type != "core_node":
            continue
        if node.label:
            resolved[node.label] = node.id
        alias = node.attributes.get("alias") if isinstance(node.attributes, dict) else None
        if alias:
            resolved[str(alias)] = node.id
    return resolved


def _resolve_ue_node_key(snapshot: TickSnapshot) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for node in snapshot.nodes:
        if node.type != "ue":
            continue
        if node.id.startswith("ue-node-"):
            suffix = node.id.removeprefix("ue-node-")
            resolved[f"ue-{suffix}"] = node.id
        supi = node.attributes.get("supi") if isinstance(node.attributes, dict) else None
        if supi:
            resolved[str(supi)] = node.id
    return resolved


def _append_metric(
    rows: list[dict[str, object]],
    snapshot_id: str,
    owner_type: str,
    owner_key: str,
    metric_name: str,
    metric_value: object,
    observed_at: datetime,
) -> None:
    rows.append(
        {
            "snapshot_id": snapshot_id,
            "owner_type": owner_type,
            "owner_key": owner_key,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "observed_at": observed_at,
        }
    )


def _add_flow_metrics(
    rows: list[dict[str, object]],
    snapshot_id: str,
    flow: FlowRecord,
    observed_at: datetime,
) -> None:
    _append_metric(rows, snapshot_id, "node", flow.flow_id, "delay_ms", flow.delay_ms, observed_at)
    _append_metric(rows, snapshot_id, "node", flow.flow_id, "jitter_ms", flow.jitter_ms, observed_at)
    _append_metric(rows, snapshot_id, "node", flow.flow_id, "loss_rate", flow.loss_rate, observed_at)
    _append_metric(
        rows,
        snapshot_id,
        "node",
        flow.flow_id,
        "throughput_ul_mbps",
        flow.throughput_ul_mbps,
        observed_at,
    )
    _append_metric(
        rows,
        snapshot_id,
        "node",
        flow.flow_id,
        "throughput_dl_mbps",
        flow.throughput_dl_mbps,
        observed_at,
    )
    _append_metric(rows, snapshot_id, "node", flow.flow_id, "queue_bytes", flow.queue_bytes, observed_at)
    _append_metric(
        rows,
        snapshot_id,
        "node",
        flow.flow_id,
        "rlc_buffer_bytes",
        flow.rlc_buffer_bytes,
        observed_at,
    )


def build_graph_snapshot_bundle(
    snapshot: TickSnapshot,
    *,
    base_network_snapshot_id: str | None = None,
    trigger_event: str | None = None,
) -> GraphSnapshotBundle:
    created_at = _utcnow()
    snapshot_id = str(uuid4())
    resolved_trigger_event = trigger_event or f"sim_tick:{snapshot.run_id}:{snapshot.tick_index}"
    node_by_id = {node.id: node for node in snapshot.nodes}

    node_rows_by_key: dict[str, dict[str, object]] = {}
    edge_rows_by_key: dict[str, dict[str, object]] = {}
    summary_nodes_by_key: dict[str, dict[str, object]] = {}
    metric_rows: list[dict[str, object]] = []

    def add_node(node_key: str, node_type: str, label: str | None, properties: dict[str, object]) -> None:
        node_rows_by_key.setdefault(
            node_key,
            {
                "snapshot_id": snapshot_id,
                "node_key": node_key,
                "node_type": node_type,
                "label": label,
                "properties": properties,
            },
        )

    def add_edge(
        edge_type: str,
        source_key: str,
        target_key: str,
        properties: dict[str, object] | None = None,
    ) -> None:
        edge_key = _edge_key(edge_type, source_key, target_key)
        edge_rows_by_key.setdefault(
            edge_key,
            {
                "snapshot_id": snapshot_id,
                "edge_key": edge_key,
                "edge_type": edge_type,
                "source_key": source_key,
                "target_key": target_key,
                "properties": properties or {},
            },
        )

    def add_summary_node(
        node_key: str,
        node_type: str,
        label: str | None,
        properties: dict[str, object],
    ) -> None:
        summary_nodes_by_key[node_key] = {
            "snapshot_id": snapshot_id,
            "node_key": node_key,
            "node_type": node_type,
            "label": label,
            "properties": properties,
        }

    slice_records_by_id = {slice_record.slice_id: slice_record for slice_record in snapshot.slices}
    slice_node_keys = {
        slice_record.slice_id: _slice_node_key_from_values(slice_record.sst, slice_record.sd)
        for slice_record in snapshot.slices
    }

    ran_node_keys: dict[str, str] = {}
    for gnb in snapshot.gnbs:
        node = node_by_id.get(gnb.node_id)
        name = node.label if node is not None else gnb.alias or gnb.gnb_id
        ran_node_keys[gnb.gnb_id] = f"ran_node:{_stable_name(name)}"

    core_node_keys: dict[str, str] = {}
    for node in snapshot.nodes:
        if node.type != "core_node":
            continue
        name = str(node.attributes.get("name", node.label or node.id))
        node_key = f"core_node:{_stable_name(name)}"
        core_node_keys[node.id] = node_key
        core_node_keys[node.label] = node_key
        core_node_keys[name] = node_key

    flows_by_app: dict[tuple[str, str], list[FlowRecord]] = defaultdict(list)
    flows_by_session: dict[tuple[str, str], list[FlowRecord]] = defaultdict(list)
    flows_by_slice: dict[str, list[FlowRecord]] = defaultdict(list)
    hosted_slices_by_ran: dict[str, set[str]] = defaultdict(set)
    hosted_slices_by_core: dict[str, set[str]] = defaultdict(set)

    for flow in snapshot.flows:
        flows_by_app[(flow.supi, flow.app_id)].append(flow)
        session_ref = _flow_session_ref(flow)
        if session_ref is not None:
            flows_by_session[(flow.supi, session_ref)].append(flow)
        flows_by_slice[flow.slice_id].append(flow)
        slice_node_key = slice_node_keys.get(flow.slice_id)
        if slice_node_key is None:
            continue
        ran_node_key = ran_node_keys.get(flow.src_gnb)
        if ran_node_key is not None:
            hosted_slices_by_ran[ran_node_key].add(slice_node_key.split(":", 1)[1])
        core_node_key = core_node_keys.get(flow.dst_upf)
        if core_node_key is None:
            core_node_key = f"core_node:{_stable_name(flow.dst_upf)}"
            core_node_keys[flow.dst_upf] = core_node_key
        hosted_slices_by_core[core_node_key].add(slice_node_key.split(":", 1)[1])

    for ue in snapshot.ues:
        node_key = _ue_node_key(ue.supi)
        properties = {"supi": ue.supi}
        add_node(node_key, "ue", ue.supi, properties)
        add_summary_node(node_key, "ue", ue.supi, dict(properties))

    for gnb in snapshot.gnbs:
        node_key = ran_node_keys[gnb.gnb_id]
        raw_node = node_by_id.get(gnb.node_id)
        raw_properties = dict(raw_node.attributes) if raw_node is not None else {}
        hosted_slices = sorted(hosted_slices_by_ran.get(node_key, set()))
        capacity = _node_capacity_properties(raw_properties)
        base_properties: dict[str, object] = {
            "id": gnb.gnb_id,
            "name": raw_properties.get("name", raw_node.label if raw_node is not None else gnb.alias),
            "node_type": _RAN_NODE_TYPE,
            "hosted_slice_snssais": hosted_slices,
            "capacity": capacity,
        }
        add_node(node_key, "ran_node", raw_node.label if raw_node is not None else gnb.alias, base_properties)
        summary_properties = dict(base_properties)
        summary_properties["telemetry"] = _node_telemetry_properties(raw_properties)
        add_summary_node(
            node_key,
            "ran_node",
            raw_node.label if raw_node is not None else gnb.alias,
            summary_properties,
        )
        _append_nested_numeric_metrics(metric_rows, snapshot_id, node_key, "capacity", capacity, created_at)
        _append_nested_numeric_metrics(
            metric_rows,
            snapshot_id,
            node_key,
            "telemetry",
            summary_properties["telemetry"],
            created_at,
        )

    for node in snapshot.nodes:
        if node.type != "core_node":
            continue
        node_key = core_node_keys[node.id]
        raw_properties = dict(node.attributes)
        hosted_slices = sorted(hosted_slices_by_core.get(node_key, set()))
        capacity = _node_capacity_properties(raw_properties)
        base_properties = {
            "id": node.id,
            "name": raw_properties.get("name", node.label or node.id),
            "node_type": _CORE_NODE_TYPE,
            "hosted_slice_snssais": hosted_slices,
            "capacity": capacity,
        }
        add_node(node_key, "core_node", node.label or node.id, base_properties)
        summary_properties = dict(base_properties)
        summary_properties["telemetry"] = _node_telemetry_properties(raw_properties)
        add_summary_node(node_key, "core_node", node.label or node.id, summary_properties)
        _append_nested_numeric_metrics(metric_rows, snapshot_id, node_key, "capacity", capacity, created_at)
        _append_nested_numeric_metrics(
            metric_rows,
            snapshot_id,
            node_key,
            "telemetry",
            summary_properties["telemetry"],
            created_at,
        )

    for flow in snapshot.flows:
        if flow.dst_upf not in core_node_keys:
            node_key = f"core_node:{_stable_name(flow.dst_upf)}"
            core_node_keys[flow.dst_upf] = node_key
            properties = {
                "id": flow.dst_upf,
                "name": flow.dst_upf,
                "node_type": _CORE_NODE_TYPE,
                "hosted_slice_snssais": sorted(hosted_slices_by_core.get(node_key, set())),
                "capacity": {},
            }
            add_node(node_key, "core_node", flow.dst_upf, properties)
            add_summary_node(node_key, "core_node", flow.dst_upf, {**properties, "telemetry": {}})

    for (supi, session_ref), session_flows in sorted(flows_by_session.items()):
        session_node_key = _session_node_key(supi, session_ref)
        primary_flow = sorted(session_flows, key=lambda item: item.flow_id)[0]
        slice_node_key = slice_node_keys.get(primary_flow.slice_id)
        session_properties: dict[str, object] = {
            "id": session_ref,
            "session_ref": session_ref,
            "supi": supi,
            "slice_ref": primary_flow.slice_id,
            "flow_ids": [flow.flow_id for flow in sorted(session_flows, key=lambda item: item.flow_id)],
            "app_ids": sorted({flow.app_id for flow in session_flows}),
            "five_qi": primary_flow.five_qi,
        }
        if len(session_properties["app_ids"]) == 1:
            session_properties["app_id"] = session_properties["app_ids"][0]
        dnn = _flow_dnn(primary_flow)
        if dnn is not None:
            session_properties["dnn"] = dnn
        if slice_node_key is not None:
            session_properties["snssai"] = slice_node_key.split(":", 1)[1]

        add_node(session_node_key, "session", session_ref, session_properties)
        add_summary_node(session_node_key, "session", session_ref, dict(session_properties))
        add_edge("uses_session", _ue_node_key(supi), session_node_key, {"supi": supi})
        if slice_node_key is not None:
            add_edge("uses_slice", session_node_key, slice_node_key, {"slice": slice_node_key.split(":", 1)[1]})
        for flow in sorted(session_flows, key=lambda item: item.flow_id):
            add_edge(
                "runs_on_session",
                _flow_node_key(flow),
                session_node_key,
                {"session_ref": session_ref},
            )

    app_keys: set[str] = set()
    for (supi, app_id), flows in sorted(flows_by_app.items()):
        app_key = _app_node_key(supi, app_id)
        app_keys.add(app_key)
        app_name = next((flow.app_name for flow in flows if flow.app_name), app_id)
        summary_flow_properties = [
            _summary_flow_properties(flow, slice_node_keys[flow.slice_id])
            for flow in sorted(flows, key=lambda item: item.flow_id)
            if flow.slice_id in slice_node_keys
        ]
        app_properties: dict[str, object] = {
            "id": app_id,
            "name": app_name,
            "supi": supi,
            "flow_ids": [flow.flow_id for flow in sorted(flows, key=lambda item: item.flow_id)],
        }
        summary_app_properties: dict[str, object] = {
            "id": app_id,
            "name": app_name,
            "supi": supi,
            "flows": summary_flow_properties,
        }
        add_node(app_key, "app", app_name, app_properties)
        add_summary_node(app_key, "app", app_name, summary_app_properties)
        add_edge("owns", _ue_node_key(supi), app_key, {"supi": supi})

        for flow in sorted(flows, key=lambda item: item.flow_id):
            slice_node_key = slice_node_keys.get(flow.slice_id)
            if slice_node_key is None:
                continue
            flow_key = _flow_node_key(flow)
            summary_flow_properties = _summary_flow_properties(flow, slice_node_key)
            flow_properties = _flow_node_properties(flow, slice_node_key)
            add_node(flow_key, "flow", flow.name or flow.flow_id, flow_properties)
            add_summary_node(flow_key, "flow", flow.name or flow.flow_id, summary_flow_properties)
            _append_nested_numeric_metrics(
                metric_rows,
                snapshot_id,
                flow_key,
                "sla",
                _dict_properties(summary_flow_properties.get("sla")),
                created_at,
            )
            _append_nested_numeric_metrics(
                metric_rows,
                snapshot_id,
                flow_key,
                "telemetry",
                _dict_properties(summary_flow_properties.get("telemetry")),
                created_at,
            )
            _append_nested_numeric_metrics(
                metric_rows,
                snapshot_id,
                flow_key,
                "allocation",
                _dict_properties(summary_flow_properties.get("allocation")),
                created_at,
            )
            add_edge("contains_flow", app_key, flow_key, {"app_id": app_id})
            add_edge("served_by_slice", flow_key, slice_node_key, {"slice": slice_node_key.split(":", 1)[1]})

    for slice_record in snapshot.slices:
        node_key = slice_node_keys[slice_record.slice_id]
        slice_flows = flows_by_slice.get(slice_record.slice_id, [])
        properties: dict[str, object] = {
            "name": slice_record.label or slice_record.slice_id,
            "snssai": node_key.split(":", 1)[1],
            "sst": slice_record.sst,
            "sd": slice_record.sd,
        }
        summary_properties = dict(properties)
        total_bandwidth_dl = 0.0
        total_bandwidth_ul = 0.0
        reserved_bandwidth_dl = 0.0
        reserved_bandwidth_ul = 0.0
        current_bandwidth_dl = 0.0
        current_bandwidth_ul = 0.0
        latency_targets: list[float] = []
        jitter_targets: list[float] = []
        loss_targets: list[float] = []
        processing_delays: list[float] = []
        telemetry_latency: list[float] = []
        telemetry_jitter: list[float] = []
        telemetry_loss: list[float] = []
        if slice_flows:
            for flow in slice_flows:
                flow_sla = _flow_sla_properties(flow)
                flow_telemetry = _flow_telemetry_properties(flow)
                total_bandwidth_dl += _numeric_value(flow_sla.get("bandwidth_dl")) or 0.0
                total_bandwidth_ul += _numeric_value(flow_sla.get("bandwidth_ul")) or 0.0
                reserved_bandwidth_dl += _numeric_value(flow_sla.get("guaranteed_bandwidth_dl")) or 0.0
                reserved_bandwidth_ul += _numeric_value(flow_sla.get("guaranteed_bandwidth_ul")) or 0.0
                current_bandwidth_dl += _numeric_value(flow_telemetry.get("throughput_dl")) or 0.0
                current_bandwidth_ul += _numeric_value(flow_telemetry.get("throughput_ul")) or 0.0
                latency_value = _numeric_value(flow_sla.get("latency"))
                jitter_value = _numeric_value(flow_sla.get("jitter"))
                loss_value = _numeric_value(flow_sla.get("loss_rate"))
                processing_delay_value = _numeric_value(flow_sla.get("processing_delay"))
                telemetry_latency_value = _numeric_value(flow_telemetry.get("latency"))
                telemetry_jitter_value = _numeric_value(flow_telemetry.get("jitter"))
                telemetry_loss_value = _numeric_value(flow_telemetry.get("loss_rate"))
                if latency_value is not None:
                    latency_targets.append(latency_value)
                if jitter_value is not None:
                    jitter_targets.append(jitter_value)
                if loss_value is not None:
                    loss_targets.append(loss_value)
                if processing_delay_value is not None:
                    processing_delays.append(processing_delay_value)
                if telemetry_latency_value is not None:
                    telemetry_latency.append(telemetry_latency_value)
                if telemetry_jitter_value is not None:
                    telemetry_jitter.append(telemetry_jitter_value)
                if telemetry_loss_value is not None:
                    telemetry_loss.append(telemetry_loss_value)
        slice_resource = _dict_properties(slice_record.resource)
        explicit_slice_qos = _dict_properties(getattr(slice_record, "qos", None))
        slice_capacity = {
            "total_bandwidth_dl": slice_resource.get("capacity_dl_mbps", total_bandwidth_dl),
            "total_bandwidth_ul": slice_resource.get("capacity_ul_mbps", total_bandwidth_ul),
            "reserved_bandwidth_dl": slice_resource.get("guaranteed_dl_mbps", reserved_bandwidth_dl),
            "reserved_bandwidth_ul": slice_resource.get("guaranteed_ul_mbps", reserved_bandwidth_ul),
        }
        slice_load = {
            "current_bandwidth_dl": _dict_properties(slice_record.telemetry).get("allocated_dl_mbps", current_bandwidth_dl),
            "current_bandwidth_ul": _dict_properties(slice_record.telemetry).get("allocated_ul_mbps", current_bandwidth_ul),
            "demand_bandwidth_dl": _dict_properties(slice_record.telemetry).get("demand_dl_mbps"),
            "demand_bandwidth_ul": _dict_properties(slice_record.telemetry).get("demand_ul_mbps"),
        }
        slice_qos = {
            "latency": explicit_slice_qos.get("latency", _average(latency_targets)),
            "jitter": explicit_slice_qos.get("jitter", _average(jitter_targets)),
            "loss_rate": explicit_slice_qos.get("loss_rate", _average(loss_targets)),
            "processing_delay": explicit_slice_qos.get("processing_delay", _average(processing_delays)),
        }
        utilization_dl = (
            current_bandwidth_dl / total_bandwidth_dl
            if total_bandwidth_dl > 0
            else None
        )
        utilization_ul = (
            current_bandwidth_ul / total_bandwidth_ul
            if total_bandwidth_ul > 0
            else None
        )
        slice_telemetry = {
            "latency": _average(telemetry_latency),
            "jitter": _average(telemetry_jitter),
            "loss_rate": _average(telemetry_loss),
            "utilization_dl": utilization_dl,
            "utilization_ul": utilization_ul,
        }
        slice_telemetry.update(_dict_properties(slice_record.telemetry))
        properties["capacity"] = slice_capacity
        properties["load"] = slice_load
        properties["telemetry"] = slice_telemetry
        summary_properties["capacity"] = slice_capacity
        summary_properties["load"] = slice_load
        summary_properties["qos"] = slice_qos
        summary_properties["telemetry"] = slice_telemetry
        add_node(node_key, "slice", slice_record.label or slice_record.slice_id, properties)
        add_summary_node(node_key, "slice", slice_record.label or slice_record.slice_id, summary_properties)
        _append_nested_numeric_metrics(metric_rows, snapshot_id, node_key, "capacity", slice_capacity, created_at)
        _append_nested_numeric_metrics(metric_rows, snapshot_id, node_key, "load", slice_load, created_at)
        _append_nested_numeric_metrics(metric_rows, snapshot_id, node_key, "qos", slice_qos, created_at)
        _append_nested_numeric_metrics(
            metric_rows,
            snapshot_id,
            node_key,
            "telemetry",
            slice_telemetry,
            created_at,
        )

        hosted_targets = {
            *sorted(
                ran_node_keys[flow.src_gnb]
                for flow in slice_flows
                if flow.src_gnb in ran_node_keys
            ),
            *sorted(
                core_node_keys.get(flow.dst_upf, f"core_node:{_stable_name(flow.dst_upf)}")
                for flow in slice_flows
            ),
        }
        for target_key in sorted(hosted_targets):
            add_edge("hosted_on", node_key, target_key, {"hosted": True})

    node_rows = [node_rows_by_key[key] for key in sorted(node_rows_by_key)]
    edge_rows = [edge_rows_by_key[key] for key in sorted(edge_rows_by_key)]
    summary_nodes = [summary_nodes_by_key[key] for key in sorted(summary_nodes_by_key)]
    metric_rows.sort(key=lambda row: (row["owner_type"], row["owner_key"], row["metric_name"]))

    graph_summary = {
        "snapshot_id": snapshot_id,
        "trigger_event": resolved_trigger_event,
        "run_id": snapshot.run_id,
        "scenario_id": snapshot.scenario_id,
        "tick_index": snapshot.tick_index,
        "sim_time_ms": snapshot.sim_time_ms,
        "nodes": [_summary_node_row(row) for row in summary_nodes],
        "edges": [_summary_edge_row(row) for row in edge_rows],
        "metrics": [_summary_metric_row(row) for row in metric_rows],
        "node_count": len(node_rows),
        "edge_count": len(edge_rows),
        "metric_count": len(metric_rows),
        "write_mode": "snapshot",
        "delta_node_count": len(node_rows),
        "delta_edge_count": len(edge_rows),
        "delta_metric_count": len(metric_rows),
        "deleted_node_count": 0,
        "deleted_edge_count": 0,
        "kpis": snapshot.kpis,
        "reward_inputs": snapshot.reward_inputs,
        "apps": sorted(app_keys),
    }

    snapshot_row = {
        "snapshot_id": snapshot_id,
        "base_network_snapshot_id": base_network_snapshot_id,
        "trigger_event": resolved_trigger_event,
        "graph_summary": graph_summary,
        "created_at": created_at,
    }

    return GraphSnapshotBundle(
        snapshot_row=snapshot_row,
        node_rows=node_rows,
        edge_rows=edge_rows,
        metric_rows=metric_rows,
    )
