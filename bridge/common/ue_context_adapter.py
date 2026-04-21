"""Load and merge flow-level UE context policy from a relational table."""

from __future__ import annotations

import copy
import json
import re
from typing import Any


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\.]*$")
_JSON_CONTEXT_KEYS = (
    "context_json",
    "context",
    "payload",
    "ue_context",
    "policy",
    "policy_json",
)


def _load_psycopg() -> Any:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "ue_context policy loading requires psycopg. Install project dependencies first."
        ) from exc
    return psycopg, dict_row


def _validate_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier}")
    return identifier


def _decode_json_like(value: object) -> object:
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    if stripped[0] not in "[{":
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def load_ue_context_rows(
    db_url: str,
    *,
    table_name: str = "ue_context",
    query: str | None = None,
) -> list[dict[str, object]]:
    psycopg, dict_row = _load_psycopg()
    sql = query
    if sql is None:
        sql = f"SELECT * FROM {_validate_identifier(table_name)}"
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]


def _as_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    return str(value)


def _as_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _as_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _string_list(value: object) -> list[str]:
    decoded = _decode_json_like(value)
    if isinstance(decoded, str):
        if not decoded:
            return []
        return [item.strip() for item in decoded.split(",") if item.strip()]
    if isinstance(decoded, (list, tuple)):
        return [str(item) for item in decoded if str(item)]
    return []


def _payload_list(value: object) -> list[dict[str, object]]:
    decoded = _decode_json_like(value)
    if not isinstance(decoded, list):
        return []
    return [dict(item) for item in decoded if isinstance(item, dict)]


def _build_flow_from_row(row: dict[str, object]) -> dict[str, object] | None:
    flow_id = _as_string(row.get("flow_id") or row.get("id"))
    slice_ref = _as_string(row.get("slice_ref"))
    supi = _as_string(row.get("supi") or row.get("ue_id"))
    app_id = _as_string(row.get("app_id"))
    if not (flow_id and slice_ref and supi and app_id):
        return None
    flow: dict[str, object] = {
        "flow_id": flow_id,
        "name": _as_string(row.get("flow_name") or row.get("name")) or flow_id,
        "supi": supi,
        "ue_name": _as_string(row.get("ue_name")),
        "app_id": app_id,
        "app_name": _as_string(row.get("app_name")),
        "slice_ref": slice_ref,
        "session_ref": _as_string(row.get("session_ref")),
        "dnn": _as_string(row.get("dnn") or row.get("apn")),
        "five_qi": _as_int(row.get("five_qi") or row.get("5qi")),
        "service_type": _as_string(row.get("service_type")),
        "service_type_id": _as_int(row.get("service_type_id")),
        "packet_size_bytes": _as_float(row.get("packet_size_bytes")),
        "arrival_rate_pps": _as_float(row.get("arrival_rate_pps")),
        "current_slice_snssai": _as_string(row.get("current_slice_snssai") or row.get("snssai")),
        "allocated_bandwidth_dl_mbps": _as_float(row.get("allocated_bandwidth_dl_mbps")),
        "allocated_bandwidth_ul_mbps": _as_float(row.get("allocated_bandwidth_ul_mbps")),
        "optimize_requested": row.get("optimize_requested"),
        "policy_filter": _as_string(row.get("filter") or row.get("flow_filter")),
        "precedence": _as_int(row.get("precedence")),
        "qos_ref": _as_int(row.get("qos_ref") or row.get("qosRef") or row.get("qfi")),
        "charging_method": _as_string(row.get("charging_method") or row.get("chargingMethod")),
        "quota": _as_string(row.get("quota")),
        "unit_cost": _as_string(row.get("unit_cost") or row.get("unitCost")),
        "sla_target": {
            key: value
            for key, value in {
                "latency_ms": _as_float(row.get("latency_ms") or row.get("latency")),
                "jitter_ms": _as_float(row.get("jitter_ms") or row.get("jitter")),
                "loss_rate": _as_float(row.get("loss_rate")),
                "bandwidth_dl_mbps": _as_float(row.get("bandwidth_dl_mbps") or row.get("bandwidth_dl")),
                "bandwidth_ul_mbps": _as_float(row.get("bandwidth_ul_mbps") or row.get("bandwidth_ul")),
                "guaranteed_bandwidth_dl_mbps": _as_float(
                    row.get("guaranteed_bandwidth_dl_mbps") or row.get("guaranteed_bandwidth_dl") or row.get("gbr_dl_mbps")
                ),
                "guaranteed_bandwidth_ul_mbps": _as_float(
                    row.get("guaranteed_bandwidth_ul_mbps") or row.get("guaranteed_bandwidth_ul") or row.get("gbr_ul_mbps")
                ),
                "priority": _as_int(row.get("priority")),
                "processing_delay_ms": _as_float(row.get("processing_delay_ms")),
            }.items()
            if value is not None
        },
    }
    return {key: value for key, value in flow.items() if value is not None and value != {}}


def _normalize_context_row(row: dict[str, object]) -> dict[str, object] | None:
    for key in _JSON_CONTEXT_KEYS:
        if key not in row:
            continue
        decoded = _decode_json_like(row[key])
        if isinstance(decoded, dict):
            return copy.deepcopy(decoded)

    supi = _as_string(row.get("supi") or row.get("ue_id") or row.get("ueId"))
    ue_name = _as_string(row.get("ue_name") or row.get("name"))
    payload: dict[str, object] = {}

    if supi or ue_name:
        ue_payload = {
            "name": ue_name or supi,
            "supi": supi,
        }
        gnb = _as_string(row.get("gnb") or row.get("target_gnb"))
        if gnb:
            ue_payload["gnb"] = gnb
        preferred = _string_list(row.get("preferred_gnbs") or row.get("preferredGnbs"))
        policy_payload = {
            key: value
            for key, value in {
                "target_gnb": _as_string(row.get("target_gnb") or row.get("targetGnb")),
                "preferred_gnbs": preferred,
            }.items()
            if value not in (None, [], ())
        }
        if policy_payload:
            ue_payload["free5gc_policy"] = policy_payload

        sessions = _payload_list(row.get("sessions"))
        if not sessions:
            session_slice = _as_string(row.get("slice_ref"))
            session_apn = _as_string(row.get("apn") or row.get("dnn"))
            if session_slice and session_apn:
                session_payload = {
                    "slice_ref": session_slice,
                    "session_ref": _as_string(row.get("session_ref")),
                    "apn": session_apn,
                    "type": _as_string(row.get("session_type") or row.get("type")) or "IPv4",
                    "five_qi": _as_int(row.get("five_qi") or row.get("5qi")) or 9,
                    "app_id": _as_string(row.get("app_id")) or f"app-{ue_name or supi}",
                }
                sessions = [session_payload]
        if sessions:
            ue_payload["sessions"] = sessions

        payload["ues"] = [
            {
                key: value
                for key, value in ue_payload.items()
                if value not in (None, [], ())
            }
        ]

    apps = _payload_list(row.get("apps"))
    if not apps and row.get("app_id") is not None:
        app_id = _as_string(row.get("app_id"))
        if app_id is not None:
            apps = [
                {
                    "app_id": app_id,
                    "name": _as_string(row.get("app_name")) or app_id,
                    "supi": supi,
                    "ue_name": ue_name,
                    "flow_ids": [flow_id] if (flow_id := _as_string(row.get("flow_id"))) else [],
                }
            ]
    if apps:
        payload["apps"] = apps

    flows = _payload_list(row.get("flows"))
    if not flows:
        flow = _build_flow_from_row(row)
        if flow is not None:
            flows = [flow]
    if flows:
        payload["flows"] = flows

    return payload or None


def _merge_named_items(
    existing: list[dict[str, object]],
    incoming: list[dict[str, object]],
    *,
    key_field: str,
) -> list[dict[str, object]]:
    merged = [copy.deepcopy(item) for item in existing]
    index_by_key = {
        str(item.get(key_field)): index
        for index, item in enumerate(merged)
        if item.get(key_field) is not None
    }
    for item in incoming:
        key = item.get(key_field)
        if key is None:
            merged.append(copy.deepcopy(item))
            continue
        existing_index = index_by_key.get(str(key))
        if existing_index is None:
            index_by_key[str(key)] = len(merged)
            merged.append(copy.deepcopy(item))
            continue
        updated = copy.deepcopy(merged[existing_index])
        updated.update({k: copy.deepcopy(v) for k, v in item.items() if v is not None})
        if isinstance(merged[existing_index].get("free5gc_policy"), dict) or isinstance(item.get("free5gc_policy"), dict):
            current_policy = dict(updated.get("free5gc_policy", {}))
            incoming_policy = dict(item.get("free5gc_policy", {}))
            if incoming_policy.get("target_gnb"):
                current_policy["target_gnb"] = incoming_policy["target_gnb"]
            preferred = []
            for value in list(current_policy.get("preferred_gnbs", [])) + list(incoming_policy.get("preferred_gnbs", [])):
                if value not in preferred:
                    preferred.append(value)
            if preferred:
                current_policy["preferred_gnbs"] = preferred
            updated["free5gc_policy"] = current_policy
        if isinstance(merged[existing_index].get("sessions"), list) or isinstance(item.get("sessions"), list):
            sessions_by_key: dict[tuple[object, object, object], dict[str, object]] = {}
            for session in list(merged[existing_index].get("sessions", [])) + list(item.get("sessions", [])):
                if not isinstance(session, dict):
                    continue
                session_key = (session.get("slice_ref"), session.get("apn"), session.get("app_id"))
                sessions_by_key.setdefault(session_key, {}).update(copy.deepcopy(session))
            updated["sessions"] = list(sessions_by_key.values())
        merged[existing_index] = updated
    return merged


def merge_ue_context_payload(
    payload: dict[str, Any],
    context_rows: list[dict[str, object]],
) -> dict[str, Any]:
    merged = copy.deepcopy(payload)
    normalized = [
        context_payload
        for row in context_rows
        if isinstance(row, dict)
        for context_payload in [_normalize_context_row(row)]
        if context_payload is not None
    ]
    for context_payload in normalized:
        merged["ues"] = _merge_named_items(
            [item for item in merged.get("ues", []) if isinstance(item, dict)],
            [item for item in context_payload.get("ues", []) if isinstance(item, dict)],
            key_field="name",
        )
        merged["apps"] = _merge_named_items(
            [item for item in merged.get("apps", []) if isinstance(item, dict)],
            [item for item in context_payload.get("apps", []) if isinstance(item, dict)],
            key_field="app_id",
        )
        merged["flows"] = _merge_named_items(
            [item for item in merged.get("flows", []) if isinstance(item, dict)],
            [item for item in context_payload.get("flows", []) if isinstance(item, dict)],
            key_field="flow_id",
        )
    return merged