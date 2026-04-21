"""Translate semantic graph snapshots into scenario payload fragments."""

from __future__ import annotations

import copy
from typing import Any

from bridge.common.topology import (
    _dedupe_sessions,
    _merge_named_items,
    _merge_slice_items,
    _merge_gnb_item,
    _merge_ue_item,
    _merge_upf_item,
)


_SERVICE_TYPE_DEFAULT_FIVE_QI = {
    "embb": 9,
    "urllc": 7,
    "mmtc": 6,
}


def _properties(record: dict[str, object]) -> dict[str, object]:
    properties = record.get("properties")
    return dict(properties) if isinstance(properties, dict) else {}


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string(value: object, fallback: str | None = None) -> str | None:
    if isinstance(value, str) and value:
        return value
    if value is None:
        return fallback
    return str(value)


def _slice_ref_from_values(sst: int, sd: str) -> str:
    return f"slice-{sst}-{sd.lower()}"


def _slice_ref_from_snssai(snssai: str) -> str:
    normalized = snssai.lower()
    if len(normalized) < 3:
        raise ValueError(f"invalid snssai: {snssai}")
    return _slice_ref_from_values(int(normalized[:2]), normalized[2:])


def _infer_five_qi(flow_payload: dict[str, object]) -> int:
    explicit_five_qi = _maybe_int(flow_payload.get("five_qi"))
    if explicit_five_qi is not None:
        return explicit_five_qi
    service_type = _string(flow_payload.get("service_type"))
    if service_type is None:
        return 9
    return _SERVICE_TYPE_DEFAULT_FIVE_QI.get(service_type.lower(), 9)


def _merge_flow_ids(existing: list[str], incoming: list[str]) -> list[str]:
    merged = list(existing)
    for flow_id in incoming:
        if flow_id not in merged:
            merged.append(flow_id)
    return merged


def _merge_app_item(existing: dict[str, object], derived: dict[str, object]) -> dict[str, object]:
    merged = copy.deepcopy(existing)
    for field in ("app_id", "name", "supi", "ue_name"):
        if field not in merged and field in derived:
            merged[field] = copy.deepcopy(derived[field])
    merged["flow_ids"] = tuple(
        _merge_flow_ids(
            [str(item) for item in merged.get("flow_ids", ())],
            [str(item) for item in derived.get("flow_ids", ())],
        )
    )
    return merged


def _merge_flow_item(existing: dict[str, object], derived: dict[str, object]) -> dict[str, object]:
    merged = copy.deepcopy(existing)
    for field in (
        "flow_id",
        "name",
        "supi",
        "ue_name",
        "app_id",
        "app_name",
        "slice_ref",
        "session_ref",
        "dnn",
        "five_qi",
        "service_type",
        "service_type_id",
        "packet_size_bytes",
        "arrival_rate_pps",
        "current_slice_snssai",
        "allocated_bandwidth_dl_mbps",
        "allocated_bandwidth_ul_mbps",
        "optimize_requested",
        "policy_filter",
        "precedence",
        "qos_ref",
        "charging_method",
        "quota",
        "unit_cost",
    ):
        if field not in merged and field in derived:
            merged[field] = copy.deepcopy(derived[field])
    existing_sla = dict(merged.get("sla_target", {}))
    for key, value in dict(derived.get("sla_target", {})).items():
        if key not in existing_sla and value is not None:
            existing_sla[key] = value
    if existing_sla:
        merged["sla_target"] = existing_sla
    return merged


def _merge_graph_items(
    existing: list[dict[str, object]],
    derived: dict[str, dict[str, object]],
    *,
    key_field: str,
    merge_item: Any,
) -> list[dict[str, object]]:
    merged = [copy.deepcopy(item) for item in existing]
    index_by_key = {
        str(item.get(key_field)): index
        for index, item in enumerate(merged)
        if item.get(key_field) is not None
    }
    for key, payload in derived.items():
        index = index_by_key.get(key)
        if index is None:
            merged.append(copy.deepcopy(payload))
            continue
        merged[index] = merge_item(merged[index], payload)
    return merged


def _rename_payload_map(
    source: dict[str, dict[str, object]],
    rename_map: dict[str, str],
) -> dict[str, dict[str, object]]:
    renamed: dict[str, dict[str, object]] = {}
    for key, payload in source.items():
        target_key = rename_map.get(key, key)
        updated_payload = copy.deepcopy(payload)
        updated_payload["name"] = target_key
        renamed[target_key] = updated_payload
    return renamed


def _align_semantic_graph_payload(
    payload: dict[str, Any],
    graph_payload: dict[str, Any],
) -> dict[str, Any]:
    renamed = copy.deepcopy(graph_payload)
    ue_name_by_supi = {
        str(item.get("supi")): str(item["name"])
        for item in payload.get("ues", [])
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("supi"), str)
    }
    rename_map: dict[str, str] = {}
    for name, ue_payload in renamed["ues"].items():
        supi = _string(ue_payload.get("supi"))
        if supi is not None and supi in ue_name_by_supi:
            rename_map[name] = ue_name_by_supi[supi]
    if rename_map:
        renamed["ues"] = _rename_payload_map(renamed["ues"], rename_map)
        for app_payload in renamed["apps"].values():
            ue_name = _string(app_payload.get("ue_name"))
            if ue_name in rename_map:
                app_payload["ue_name"] = rename_map[ue_name]
        for flow_payload in renamed["flows"].values():
            ue_name = _string(flow_payload.get("ue_name"))
            if ue_name in rename_map:
                flow_payload["ue_name"] = rename_map[ue_name]
    return renamed


def _build_semantic_graph_payload(graph_summary: dict[str, Any]) -> dict[str, Any]:
    nodes = graph_summary.get("nodes")
    edges = graph_summary.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ValueError("semantic graph summary must define nodes and edges arrays")

    slice_payloads: dict[str, dict[str, object]] = {}
    upf_payloads: dict[str, dict[str, object]] = {}
    gnb_payloads: dict[str, dict[str, object]] = {}
    ue_payloads: dict[str, dict[str, object]] = {}
    app_payloads: dict[str, dict[str, object]] = {}
    flow_payloads: dict[str, dict[str, object]] = {}

    ue_name_by_key: dict[str, str] = {}
    ue_name_by_supi: dict[str, str] = {}
    gnb_name_by_key: dict[str, str] = {}
    upf_name_by_key: dict[str, str] = {}
    slice_ref_by_key: dict[str, str] = {}
    app_id_by_key: dict[str, str] = {}
    app_name_by_key: dict[str, str] = {}
    flow_id_by_key: dict[str, str] = {}
    session_payloads_by_key: dict[str, dict[str, object]] = {}
    ran_hosts_by_slice: dict[str, list[str]] = {}
    core_hosts_by_slice: dict[str, list[str]] = {}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_key = _string(node.get("node_key"))
        node_type = _string(node.get("node_type"))
        label = _string(node.get("label"))
        properties = _properties(node)
        if node_key is None or node_type is None:
            continue

        if node_type == "slice":
            snssai = _string(properties.get("snssai"))
            sst = _maybe_int(properties.get("sst"))
            sd = _string(properties.get("sd"))
            if snssai is not None:
                slice_ref = _slice_ref_from_snssai(snssai)
                sst = int(snssai[:2]) if sst is None else sst
                sd = snssai[2:] if sd is None else sd
            elif sst is not None and sd is not None:
                slice_ref = _slice_ref_from_values(sst, sd)
                snssai = f"{sst:02d}{sd.lower()}"
            else:
                continue
            slice_ref_by_key[node_key] = slice_ref
            slice_payloads[slice_ref] = {
                "sst": sst,
                "sd": sd,
                "label": _string(properties.get("name"), label),
            }
            continue

        if node_type == "core_node":
            upf_name = _string(properties.get("name"), label)
            if upf_name is None:
                continue
            upf_name_by_key[node_key] = upf_name
            upf_payloads.setdefault(upf_name, {"name": upf_name})
            continue

        if node_type == "ran_node":
            gnb_name = _string(properties.get("name"), label)
            if gnb_name is None:
                continue
            gnb_name_by_key[node_key] = gnb_name
            gnb_payloads.setdefault(gnb_name, {"name": gnb_name, "slices": []})
            continue

        if node_type == "ue":
            supi = _string(properties.get("supi"), label)
            ue_name = _string(properties.get("name"), label or supi)
            if supi is None or ue_name is None:
                continue
            ue_name_by_key[node_key] = ue_name
            ue_name_by_supi[supi] = ue_name
            ue_payloads.setdefault(
                ue_name,
                {
                    "name": ue_name,
                    "supi": supi,
                    "sessions": [],
                },
            )
            continue

        if node_type == "app":
            app_id = _string(properties.get("id"), label)
            app_name = _string(properties.get("name"), label or app_id)
            supi = _string(properties.get("supi"))
            if app_id is None or app_name is None or supi is None:
                continue
            app_id_by_key[node_key] = app_id
            app_name_by_key[node_key] = app_name
            app_payloads.setdefault(
                app_id,
                {
                    "app_id": app_id,
                    "name": app_name,
                    "supi": supi,
                    "ue_name": ue_name_by_supi.get(supi),
                    "flow_ids": tuple(),
                },
            )
            continue

        if node_type == "flow":
            flow_id = _string(properties.get("id"), label)
            supi = _string(properties.get("supi"))
            app_id = _string(properties.get("app_id"))
            if flow_id is None or supi is None or app_id is None:
                continue
            flow_id_by_key[node_key] = flow_id
            service = dict(properties.get("service", {})) if isinstance(properties.get("service"), dict) else {}
            traffic = dict(properties.get("traffic", {})) if isinstance(properties.get("traffic"), dict) else {}
            sla = dict(properties.get("sla", {})) if isinstance(properties.get("sla"), dict) else {}
            allocation = dict(properties.get("allocation", {})) if isinstance(properties.get("allocation"), dict) else {}
            current_slice_snssai = _string(allocation.get("current_slice_snssai"))
            slice_ref = _slice_ref_from_snssai(current_slice_snssai) if current_slice_snssai else None
            flow_payloads.setdefault(
                flow_id,
                {
                    "flow_id": flow_id,
                    "name": _string(properties.get("name"), label or flow_id),
                    "supi": supi,
                    "ue_name": ue_name_by_supi.get(supi),
                    "app_id": app_id,
                    "app_name": _string(properties.get("app_name")),
                    "slice_ref": slice_ref,
                    "session_ref": _string(properties.get("session_ref"), _string(traffic.get("session_ref"))),
                    "dnn": _string(properties.get("dnn"), _string(service.get("dnn"), _string(service.get("apn")))),
                    "five_qi": _infer_five_qi(
                        {
                            "five_qi": properties.get("5qi"),
                            "service_type": service.get("service_type"),
                        }
                    ),
                    "service_type": _string(service.get("service_type")),
                    "service_type_id": _maybe_int(service.get("service_type_id")),
                    "packet_size_bytes": _maybe_float(traffic.get("packet_size")),
                    "arrival_rate_pps": _maybe_float(traffic.get("arrival_rate")),
                    "current_slice_snssai": current_slice_snssai,
                    "allocated_bandwidth_dl_mbps": _maybe_float(allocation.get("allocated_bandwidth_dl")),
                    "allocated_bandwidth_ul_mbps": _maybe_float(allocation.get("allocated_bandwidth_ul")),
                    "optimize_requested": allocation.get("optimize_requested"),
                    "policy_filter": _string(traffic.get("filter"), _string(properties.get("policy_filter"))),
                    "precedence": _maybe_int(traffic.get("precedence")),
                    "qos_ref": _maybe_int(allocation.get("qos_ref")),
                    "charging_method": _string(properties.get("charging_method")),
                    "quota": _string(properties.get("quota")),
                    "unit_cost": _string(properties.get("unit_cost")),
                    "sla_target": {
                        "latency_ms": _maybe_float(sla.get("latency")),
                        "jitter_ms": _maybe_float(sla.get("jitter")),
                        "loss_rate": _maybe_float(sla.get("loss_rate")),
                        "bandwidth_dl_mbps": _maybe_float(sla.get("bandwidth_dl")),
                        "bandwidth_ul_mbps": _maybe_float(sla.get("bandwidth_ul")),
                        "guaranteed_bandwidth_dl_mbps": _maybe_float(sla.get("guaranteed_bandwidth_dl")),
                        "guaranteed_bandwidth_ul_mbps": _maybe_float(sla.get("guaranteed_bandwidth_ul")),
                        "priority": _maybe_int(sla.get("priority")),
                    },
                },
            )
            continue

        if node_type == "session":
            session_ref = _string(properties.get("session_ref"), _string(properties.get("id"), label))
            supi = _string(properties.get("supi"))
            if session_ref is None or supi is None:
                continue
            session_payloads_by_key[node_key] = {
                "session_ref": session_ref,
                "supi": supi,
                "ue_name": ue_name_by_supi.get(supi),
                "slice_ref": _string(properties.get("slice_ref")),
                "apn": _string(properties.get("dnn"), _string(properties.get("apn"))),
                "type": _string(properties.get("type"), "IPv4"),
                "five_qi": _maybe_int(properties.get("five_qi")),
                "app_id": _string(properties.get("app_id")),
            }

    for app_payload in app_payloads.values():
        if app_payload.get("ue_name") is None:
            app_payload["ue_name"] = ue_name_by_supi.get(_string(app_payload.get("supi"), ""))

    for flow_payload in flow_payloads.values():
        if flow_payload.get("ue_name") is None:
            flow_payload["ue_name"] = ue_name_by_supi.get(_string(flow_payload.get("supi"), ""))

    for session_payload in session_payloads_by_key.values():
        if session_payload.get("ue_name") is None:
            session_payload["ue_name"] = ue_name_by_supi.get(_string(session_payload.get("supi"), ""))

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_type = _string(edge.get("edge_type"))
        source_key = _string(edge.get("source_key"))
        target_key = _string(edge.get("target_key"))
        if edge_type is None or source_key is None or target_key is None:
            continue

        if edge_type == "owns":
            ue_name = ue_name_by_key.get(source_key)
            app_id = app_id_by_key.get(target_key)
            if ue_name is None or app_id is None:
                continue
            app_payload = app_payloads.setdefault(app_id, {"app_id": app_id, "flow_ids": tuple()})
            app_payload["ue_name"] = ue_name
            if "supi" not in app_payload and ue_name in ue_payloads:
                app_payload["supi"] = ue_payloads[ue_name].get("supi")
            continue

        if edge_type == "contains_flow":
            app_id = app_id_by_key.get(source_key)
            flow_id = flow_id_by_key.get(target_key)
            if app_id is None or flow_id is None:
                continue
            app_payload = app_payloads.setdefault(app_id, {"app_id": app_id, "flow_ids": tuple()})
            app_payload["flow_ids"] = tuple(_merge_flow_ids(list(app_payload.get("flow_ids", ())), [flow_id]))
            flow_payload = flow_payloads.setdefault(flow_id, {"flow_id": flow_id, "app_id": app_id})
            flow_payload.setdefault("app_id", app_id)
            flow_payload.setdefault("app_name", app_name_by_key.get(source_key))
            continue

        if edge_type == "served_by_slice":
            flow_id = flow_id_by_key.get(source_key)
            slice_ref = slice_ref_by_key.get(target_key)
            if flow_id is None or slice_ref is None:
                session_payload = session_payloads_by_key.get(source_key)
                if session_payload is None or slice_ref is None:
                    continue
                session_payload["slice_ref"] = slice_ref
                continue
            flow_payload = flow_payloads.setdefault(flow_id, {"flow_id": flow_id})
            flow_payload["slice_ref"] = slice_ref
            if flow_payload.get("current_slice_snssai") is None:
                slice_payload = slice_payloads.get(slice_ref, {})
                sst = _maybe_int(slice_payload.get("sst"))
                sd = _string(slice_payload.get("sd"))
                if sst is not None and sd is not None:
                    flow_payload["current_slice_snssai"] = f"{sst:02d}{sd.lower()}"
            continue

        if edge_type == "uses_session":
            ue_name = ue_name_by_key.get(source_key)
            session_payload = session_payloads_by_key.get(target_key)
            if ue_name is None or session_payload is None:
                continue
            session_payload["ue_name"] = ue_name
            continue

        if edge_type == "runs_on_session":
            flow_id = flow_id_by_key.get(source_key)
            session_payload = session_payloads_by_key.get(target_key)
            if flow_id is None or session_payload is None:
                continue
            flow_payload = flow_payloads.setdefault(flow_id, {"flow_id": flow_id})
            flow_payload.setdefault("session_ref", session_payload.get("session_ref"))
            flow_payload.setdefault("slice_ref", session_payload.get("slice_ref"))
            flow_payload.setdefault("dnn", session_payload.get("apn"))
            continue

        if edge_type == "uses_slice":
            session_payload = session_payloads_by_key.get(source_key)
            slice_ref = slice_ref_by_key.get(target_key)
            if session_payload is None or slice_ref is None:
                continue
            session_payload["slice_ref"] = slice_ref
            continue

        if edge_type == "hosted_on":
            slice_ref = slice_ref_by_key.get(source_key)
            if slice_ref is None:
                continue
            gnb_name = gnb_name_by_key.get(target_key)
            if gnb_name is not None:
                ran_hosts = ran_hosts_by_slice.setdefault(slice_ref, [])
                if gnb_name not in ran_hosts:
                    ran_hosts.append(gnb_name)
                gnb_payload = gnb_payloads.setdefault(gnb_name, {"name": gnb_name, "slices": []})
                if slice_ref not in gnb_payload["slices"]:
                    gnb_payload["slices"].append(slice_ref)
                continue
            upf_name = upf_name_by_key.get(target_key)
            if upf_name is not None:
                core_hosts = core_hosts_by_slice.setdefault(slice_ref, [])
                if upf_name not in core_hosts:
                    core_hosts.append(upf_name)

    for app_payload in app_payloads.values():
        app_flow_ids = [str(item) for item in app_payload.get("flow_ids", ())]
        for flow_id, flow_payload in flow_payloads.items():
            if flow_payload.get("app_id") == app_payload.get("app_id") and flow_id not in app_flow_ids:
                app_flow_ids.append(flow_id)
        app_payload["flow_ids"] = tuple(app_flow_ids)

    sessions_by_ue: dict[str, list[dict[str, object]]] = {}
    preferred_gnbs_by_ue: dict[str, list[str]] = {}

    for session_payload in session_payloads_by_key.values():
        ue_name = _string(session_payload.get("ue_name"))
        slice_ref = _string(session_payload.get("slice_ref"))
        if ue_name is None or slice_ref is None:
            continue
        sessions_by_ue.setdefault(ue_name, []).append(
            {
                "slice_ref": slice_ref,
                "session_ref": _string(session_payload.get("session_ref")),
                "apn": _string(session_payload.get("apn"), "internet"),
                "type": _string(session_payload.get("type"), "IPv4"),
                "five_qi": int(session_payload.get("five_qi", 9)),
                "app_id": _string(session_payload.get("app_id")),
            }
        )

    for flow_payload in flow_payloads.values():
        ue_name = _string(flow_payload.get("ue_name"))
        slice_ref = _string(flow_payload.get("slice_ref"))
        if ue_name is None or slice_ref is None:
            continue
        derived_session = {
            "slice_ref": slice_ref,
            "session_ref": _string(flow_payload.get("session_ref")),
            "apn": _string(flow_payload.get("dnn"), "internet"),
            "type": "IPv4",
            "five_qi": int(flow_payload.get("five_qi", 9)),
            "app_id": _string(flow_payload.get("app_id"), flow_payload.get("flow_id")),
        }
        existing_sessions = sessions_by_ue.setdefault(ue_name, [])
        session_ref = _string(derived_session.get("session_ref"))
        if session_ref is not None:
            matched = next(
                (
                    item
                    for item in existing_sessions
                    if _string(item.get("session_ref")) == session_ref
                ),
                None,
            )
            if matched is not None:
                for key, value in derived_session.items():
                    if key not in matched and value is not None:
                        matched[key] = value
                continue
        existing_sessions.append(derived_session)
        for gnb_name in ran_hosts_by_slice.get(slice_ref, []):
            preferred_list = preferred_gnbs_by_ue.setdefault(ue_name, [])
            if gnb_name not in preferred_list:
                preferred_list.append(gnb_name)

    for ue_name, ue_payload in ue_payloads.items():
        sessions = sessions_by_ue.get(ue_name, [])
        if sessions:
            ue_payload["sessions"] = _dedupe_sessions(sessions)
        preferred_gnbs = preferred_gnbs_by_ue.get(ue_name, [])
        if preferred_gnbs:
            ue_payload.setdefault("gnb", preferred_gnbs[0])
            ue_payload["free5gc_policy"] = {
                "target_gnb": preferred_gnbs[0],
                "preferred_gnbs": preferred_gnbs,
            }

    for gnb_name, gnb_payload in gnb_payloads.items():
        candidate_upfs: list[str] = []
        for slice_ref in gnb_payload.get("slices", []):
            for upf_name in core_hosts_by_slice.get(str(slice_ref), []):
                if upf_name not in candidate_upfs:
                    candidate_upfs.append(upf_name)
        if candidate_upfs and gnb_payload.get("backhaul_upf") is None:
            gnb_payload["backhaul_upf"] = candidate_upfs[0]

    return {
        "slices": slice_payloads,
        "upfs": upf_payloads,
        "gnbs": gnb_payloads,
        "ues": ue_payloads,
        "apps": app_payloads,
        "flows": flow_payloads,
    }


def merge_semantic_graph_payload(
    payload: dict[str, Any],
    graph_summary: dict[str, Any],
) -> dict[str, Any]:
    graph_payload = _align_semantic_graph_payload(payload, _build_semantic_graph_payload(graph_summary))
    merged = copy.deepcopy(payload)
    merged["slices"] = _merge_slice_items(
        [item for item in merged.get("slices", []) if isinstance(item, dict)],
        graph_payload["slices"],
    )
    merged["upfs"] = _merge_named_items(
        [item for item in merged.get("upfs", []) if isinstance(item, dict)],
        graph_payload["upfs"],
        merge_item=_merge_upf_item,
    )
    merged["gnbs"] = _merge_named_items(
        [item for item in merged.get("gnbs", []) if isinstance(item, dict)],
        graph_payload["gnbs"],
        merge_item=_merge_gnb_item,
    )
    merged["ues"] = _merge_named_items(
        [item for item in merged.get("ues", []) if isinstance(item, dict)],
        graph_payload["ues"],
        merge_item=_merge_ue_item,
    )
    merged["apps"] = _merge_graph_items(
        [item for item in merged.get("apps", []) if isinstance(item, dict)],
        graph_payload["apps"],
        key_field="app_id",
        merge_item=_merge_app_item,
    )
    merged["flows"] = _merge_graph_items(
        [item for item in merged.get("flows", []) if isinstance(item, dict)],
        graph_payload["flows"],
        key_field="flow_id",
        merge_item=_merge_flow_item,
    )
    return merged


def load_graph_snapshot_payload(graph_db_url: str, snapshot_id: str) -> dict[str, Any]:
    from bridge.writer.postgres_graph_store import PostgresGraphStore

    store = PostgresGraphStore(graph_db_url)
    return store.load_graph_snapshot(snapshot_id)