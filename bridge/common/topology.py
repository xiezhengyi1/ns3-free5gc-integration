"""Resolve scenario topology and graph-derived entities."""

from __future__ import annotations

import copy
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from bridge.common.scenario import ScenarioConfig, UeConfig


_SLICE_REF_RE = re.compile(r"^slice-(?P<sst>\d+)-(?P<sd>[0-9a-fA-F]+)$")


@dataclass(slots=True, frozen=True)
class NodePosition:
    x: float
    y: float
    z: float

    def to_tuple(self) -> tuple[float, float, float]:
        return (self.x, self.y, self.z)


@dataclass(slots=True, frozen=True)
class ResolvedScenarioTopology:
    ue_to_gnb: dict[str, str]
    gnb_to_upf: dict[str, str]
    gnb_positions: dict[str, NodePosition]
    ue_positions: dict[str, NodePosition]
    source_graph_file: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "source_graph_file": self.source_graph_file,
            "ue_to_gnb": dict(self.ue_to_gnb),
            "gnb_to_upf": dict(self.gnb_to_upf),
            "gnb_positions": {
                name: asdict(position) for name, position in self.gnb_positions.items()
            },
            "ue_positions": {
                name: asdict(position) for name, position in self.ue_positions.items()
            },
        }


@dataclass(slots=True, frozen=True)
class TopologyGraphData:
    source_path: str
    slices: dict[str, dict[str, object]]
    upfs: dict[str, dict[str, object]]
    gnbs: dict[str, dict[str, object]]
    ues: dict[str, dict[str, object]]
    ue_to_gnb: dict[str, str]
    gnb_to_upf: dict[str, str]
    gnb_positions: dict[str, NodePosition]
    ue_positions: dict[str, NodePosition]


def _yaml_or_json_load(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"expected mapping in topology graph: {path}")
    return payload


def _attributes(record: dict[str, object]) -> dict[str, object]:
    attributes = record.get("attributes")
    return dict(attributes) if isinstance(attributes, dict) else {}


def _string_candidates(node: dict[str, object]) -> list[str]:
    attributes = _attributes(node)
    candidates: list[str] = []
    for value in (
        attributes.get("name"),
        attributes.get("alias"),
        attributes.get("supi"),
        node.get("label"),
        node.get("id"),
    ):
        if not isinstance(value, str):
            continue
        if value not in candidates:
            candidates.append(value)
    return candidates


def _extract_position(node: dict[str, object]) -> NodePosition | None:
    attributes = _attributes(node)
    position = attributes.get("position")
    if isinstance(position, dict):
        try:
            return NodePosition(
                x=float(position.get("x", 0.0)),
                y=float(position.get("y", 0.0)),
                z=float(position.get("z", 0.0)),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid position payload in topology node {node.get('id')}") from exc
    if all(key in attributes for key in ("x", "y", "z")):
        try:
            return NodePosition(
                x=float(attributes["x"]),
                y=float(attributes["y"]),
                z=float(attributes["z"]),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid coordinate payload in topology node {node.get('id')}") from exc
    return None


def _match_known_name(candidates: list[str], known_names: set[str]) -> str | None:
    for candidate in candidates:
        if candidate in known_names:
            return candidate
    return None


def _coerce_hex_sd(value: object) -> str:
    return str(value).lower().removeprefix("0x")


def _slice_ref_from_values(sst: int, sd: str) -> str:
    return f"slice-{sst}-{sd.lower()}"


def _parse_slice_ref(value: str) -> tuple[int, str] | None:
    match = _SLICE_REF_RE.match(value)
    if match is None:
        return None
    return int(match.group("sst")), match.group("sd").lower()


def _graph_entity_name(node: dict[str, object], fallback_prefix: str) -> str:
    attributes = _attributes(node)
    for candidate in (
        attributes.get("name"),
        node.get("label"),
        node.get("id"),
    ):
        if isinstance(candidate, str) and candidate:
            return candidate
    raise ValueError(f"{fallback_prefix} node must define id, label, or attributes.name: {node}")


def _coerce_string_list(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if isinstance(item, (str, int))]
    return []


def _merge_string_lists(existing: list[str], incoming: list[str]) -> list[str]:
    merged = list(existing)
    for item in incoming:
        if item not in merged:
            merged.append(item)
    return merged


def _register_slice_payload(
    slice_payloads: dict[str, dict[str, object]],
    descriptor: object,
    *,
    default_label: str | None = None,
) -> str:
    slice_ref: str | None = None
    payload: dict[str, object] = {}

    if isinstance(descriptor, str):
        slice_ref = descriptor
        parsed = _parse_slice_ref(slice_ref)
        if parsed is not None:
            payload = {"sst": parsed[0], "sd": parsed[1]}
    elif isinstance(descriptor, dict):
        if descriptor.get("slice_ref"):
            slice_ref = str(descriptor["slice_ref"])
        elif descriptor.get("sst") is not None and descriptor.get("sd") is not None:
            sst = int(descriptor["sst"])
            sd = _coerce_hex_sd(descriptor["sd"])
            slice_ref = _slice_ref_from_values(sst, sd)
            payload = {"sst": sst, "sd": sd}
        if not payload and slice_ref is not None:
            parsed = _parse_slice_ref(slice_ref)
            if parsed is not None:
                payload = {"sst": parsed[0], "sd": parsed[1]}
        if payload and descriptor.get("label"):
            payload["label"] = str(descriptor["label"])
    else:
        raise ValueError(f"unsupported slice descriptor: {descriptor!r}")

    if slice_ref is None:
        raise ValueError(f"could not resolve slice reference from {descriptor!r}")
    if default_label and "label" not in payload:
        payload["label"] = default_label

    existing = slice_payloads.get(slice_ref)
    if existing is None:
        if not payload:
            parsed = _parse_slice_ref(slice_ref)
            if parsed is None:
                raise ValueError(
                    f"slice {slice_ref!r} must define sst/sd explicitly or use slice-<sst>-<sd> format"
                )
            payload = {"sst": parsed[0], "sd": parsed[1]}
            if default_label:
                payload["label"] = default_label
        slice_payloads[slice_ref] = payload
        return slice_ref

    if "label" not in existing and payload.get("label"):
        existing["label"] = payload["label"]
    if "sst" not in existing and payload.get("sst") is not None:
        existing["sst"] = payload["sst"]
    if "sd" not in existing and payload.get("sd") is not None:
        existing["sd"] = payload["sd"]
    return slice_ref


def _normalize_session_payload(
    ue_name: str,
    raw_session: object,
    *,
    slice_payloads: dict[str, dict[str, object]],
    default_app_id: str,
) -> dict[str, object]:
    if isinstance(raw_session, str):
        slice_ref = _register_slice_payload(slice_payloads, raw_session)
        return {
            "slice_ref": slice_ref,
            "apn": "internet",
            "type": "IPv4",
            "five_qi": 9,
            "app_id": default_app_id,
        }

    if not isinstance(raw_session, dict):
        raise ValueError(f"unsupported UE session descriptor for {ue_name}: {raw_session!r}")

    slice_descriptor = raw_session.get("slice") if raw_session.get("slice") is not None else raw_session
    slice_ref = _register_slice_payload(slice_payloads, slice_descriptor)
    return {
        "slice_ref": slice_ref,
        "apn": str(raw_session.get("apn", "internet")),
        "type": str(raw_session.get("type", raw_session.get("session_type", "IPv4"))),
        "five_qi": int(raw_session.get("five_qi", 9)),
        "app_id": str(raw_session.get("app_id", default_app_id)),
    }


def _merge_policy_payload(
    existing: dict[str, object] | None,
    incoming: dict[str, object] | None,
) -> dict[str, object] | None:
    if not incoming:
        return existing
    merged = dict(existing or {})
    if incoming.get("target_gnb") and not merged.get("target_gnb"):
        merged["target_gnb"] = incoming["target_gnb"]
    merged["preferred_gnbs"] = _merge_string_lists(
        _coerce_string_list(merged.get("preferred_gnbs")),
        _coerce_string_list(incoming.get("preferred_gnbs")),
    )
    return merged


def _dedupe_sessions(existing: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    index_by_key: dict[tuple[object, object, object], int] = {}
    for session in existing:
        key = (session.get("slice_ref"), session.get("apn"), session.get("app_id"))
        index = index_by_key.get(key)
        if index is None:
            index_by_key[key] = len(merged)
            merged.append(dict(session))
            continue
        current = merged[index]
        for field in ("type", "five_qi"):
            if field not in current and field in session:
                current[field] = session[field]
    return merged


def _extract_policy_payload(attributes: dict[str, object]) -> dict[str, object] | None:
    policy_payload = attributes.get("free5gc_policy")
    target_gnb: str | None = None
    preferred_gnbs: list[str] = []

    if isinstance(policy_payload, dict):
        target_value = policy_payload.get("target_gnb")
        if target_value is None:
            target_value = policy_payload.get("targetGnb")
        if isinstance(target_value, str) and target_value:
            target_gnb = target_value
        preferred_value = policy_payload.get("preferred_gnbs")
        if preferred_value is None:
            preferred_value = policy_payload.get("preferredGnbs")
        preferred_gnbs = _coerce_string_list(preferred_value)

    direct_target = attributes.get("target_gnb")
    if target_gnb is None and isinstance(direct_target, str) and direct_target:
        target_gnb = direct_target
    preferred_gnbs = _merge_string_lists(
        preferred_gnbs,
        _coerce_string_list(attributes.get("preferred_gnbs")),
    )

    if target_gnb is None and not preferred_gnbs:
        return None
    return {
        "target_gnb": target_gnb,
        "preferred_gnbs": preferred_gnbs,
    }


def load_topology_graph(path: str | Path) -> TopologyGraphData:
    source_path = str(Path(path).expanduser().resolve())
    payload = _yaml_or_json_load(Path(source_path))
    nodes = payload.get("nodes")
    links = payload.get("links")
    if not isinstance(nodes, list) or not isinstance(links, list):
        raise ValueError("topology graph must define nodes and links arrays")

    slice_payloads: dict[str, dict[str, object]] = {}
    upf_payloads: dict[str, dict[str, object]] = {}
    gnb_payloads: dict[str, dict[str, object]] = {}
    ue_payloads: dict[str, dict[str, object]] = {}
    ue_to_gnb: dict[str, str] = {}
    gnb_to_upf: dict[str, str] = {}
    gnb_positions: dict[str, NodePosition] = {}
    ue_positions: dict[str, NodePosition] = {}

    gnb_node_ids: dict[str, str] = {}
    ue_node_ids: dict[str, str] = {}
    upf_node_ids: dict[str, str] = {}
    slice_node_ids: dict[str, str] = {}

    for raw_node in nodes:
        if not isinstance(raw_node, dict):
            continue
        node_type = raw_node.get("type")
        node_id = raw_node.get("id")
        if not isinstance(node_id, str):
            continue

        attributes = _attributes(raw_node)
        position = _extract_position(raw_node)

        if node_type == "slice":
            slice_descriptor: dict[str, object] = {}
            if attributes.get("slice_ref"):
                slice_descriptor["slice_ref"] = str(attributes["slice_ref"])
            if attributes.get("sst") is not None:
                slice_descriptor["sst"] = int(attributes["sst"])
            if attributes.get("sd") is not None:
                slice_descriptor["sd"] = _coerce_hex_sd(attributes["sd"])
            label = attributes.get("label") or raw_node.get("label")
            if isinstance(label, str) and label:
                slice_descriptor["label"] = label
            if not slice_descriptor and isinstance(raw_node.get("label"), str):
                slice_descriptor["slice_ref"] = str(raw_node["label"])
            if not slice_descriptor:
                slice_descriptor["slice_ref"] = node_id
            slice_ref = _register_slice_payload(
                slice_payloads,
                slice_descriptor,
                default_label=str(raw_node.get("label")) if isinstance(raw_node.get("label"), str) else None,
            )
            slice_node_ids[node_id] = slice_ref
            continue

        if node_type == "core_node":
            upf_name = _graph_entity_name(raw_node, "core_node")
            entry = upf_payloads.setdefault(upf_name, {"name": upf_name})
            if isinstance(attributes.get("role"), str) and attributes["role"]:
                entry.setdefault("role", str(attributes["role"]))
            if isinstance(attributes.get("dnn"), str) and attributes["dnn"]:
                entry.setdefault("dnn", str(attributes["dnn"]))
            upf_node_ids[node_id] = upf_name
            continue

        if node_type == "ran_node":
            gnb_name = _graph_entity_name(raw_node, "ran_node")
            entry = gnb_payloads.setdefault(gnb_name, {"name": gnb_name})
            if isinstance(attributes.get("alias"), str) and attributes["alias"]:
                entry.setdefault("alias", str(attributes["alias"]))
            if attributes.get("tac") is not None:
                entry.setdefault("tac", int(attributes["tac"]))
            if attributes.get("nci") is not None:
                entry.setdefault("nci", str(attributes["nci"]))
            if isinstance(attributes.get("backhaul_upf"), str) and attributes["backhaul_upf"]:
                entry.setdefault("backhaul_upf", str(attributes["backhaul_upf"]))
            slices = entry.setdefault("slices", [])
            for descriptor in attributes.get("slices", []) if isinstance(attributes.get("slices"), list) else []:
                slice_ref = _register_slice_payload(slice_payloads, descriptor)
                if slice_ref not in slices:
                    slices.append(slice_ref)
            if position is not None:
                gnb_positions[gnb_name] = position
            gnb_node_ids[node_id] = gnb_name
            continue

        if node_type == "ue":
            ue_name = _graph_entity_name(raw_node, "ue")
            entry = ue_payloads.setdefault(ue_name, {"name": ue_name})
            for field in ("supi", "key", "op"):
                if isinstance(attributes.get(field), str) and attributes[field]:
                    entry.setdefault(field, str(attributes[field]))
            if isinstance(attributes.get("op_type"), str) and attributes["op_type"]:
                entry.setdefault("op_type", str(attributes["op_type"]))
            if attributes.get("amf") is not None:
                entry.setdefault("amf", str(attributes["amf"]))
            if isinstance(attributes.get("gnb"), str) and attributes["gnb"]:
                entry.setdefault("gnb", str(attributes["gnb"]))
            policy_payload = _extract_policy_payload(attributes)
            if policy_payload is not None:
                entry["free5gc_policy"] = _merge_policy_payload(
                    entry.get("free5gc_policy") if isinstance(entry.get("free5gc_policy"), dict) else None,
                    policy_payload,
                )
            sessions = entry.setdefault("sessions", [])
            raw_sessions = attributes.get("sessions")
            if isinstance(raw_sessions, list):
                for session_index, raw_session in enumerate(raw_sessions, start=1):
                    sessions.append(
                        _normalize_session_payload(
                            ue_name,
                            raw_session,
                            slice_payloads=slice_payloads,
                            default_app_id=f"{ue_name}-app-{session_index}",
                        )
                    )
            if position is not None:
                ue_positions[ue_name] = position
            ue_node_ids[node_id] = ue_name

    for raw_link in links:
        if not isinstance(raw_link, dict):
            continue
        edge_type = raw_link.get("type")
        source = raw_link.get("source")
        target = raw_link.get("target")
        if not isinstance(source, str) or not isinstance(target, str):
            continue
        attributes = _attributes(raw_link)

        if edge_type == "attached_to":
            if source in ue_node_ids and target in gnb_node_ids:
                ue_to_gnb[ue_node_ids[source]] = gnb_node_ids[target]
            elif target in ue_node_ids and source in gnb_node_ids:
                ue_to_gnb[ue_node_ids[target]] = gnb_node_ids[source]
            continue

        if edge_type == "tunneled_via":
            if source in gnb_node_ids and target in upf_node_ids:
                gnb_to_upf[gnb_node_ids[source]] = upf_node_ids[target]
            elif target in gnb_node_ids and source in upf_node_ids:
                gnb_to_upf[gnb_node_ids[target]] = upf_node_ids[source]
            continue

        if edge_type == "serves_slice":
            gnb_name: str | None = None
            slice_ref: str | None = None
            if source in gnb_node_ids and target in slice_node_ids:
                gnb_name = gnb_node_ids[source]
                slice_ref = slice_node_ids[target]
            elif target in gnb_node_ids and source in slice_node_ids:
                gnb_name = gnb_node_ids[target]
                slice_ref = slice_node_ids[source]
            if gnb_name is not None and slice_ref is not None:
                entry = gnb_payloads.setdefault(gnb_name, {"name": gnb_name})
                entry["slices"] = _merge_string_lists(
                    list(entry.get("slices", [])),
                    [slice_ref],
                )
            continue

        if edge_type == "uses_slice":
            ue_name: str | None = None
            slice_ref: str | None = None
            if source in ue_node_ids and target in slice_node_ids:
                ue_name = ue_node_ids[source]
                slice_ref = slice_node_ids[target]
            elif target in ue_node_ids and source in slice_node_ids:
                ue_name = ue_node_ids[target]
                slice_ref = slice_node_ids[source]
            if ue_name is not None and slice_ref is not None:
                entry = ue_payloads.setdefault(ue_name, {"name": ue_name})
                sessions = list(entry.get("sessions", []))
                session_payload = _normalize_session_payload(
                    ue_name,
                    {
                        **attributes,
                        "slice_ref": slice_ref,
                    },
                    slice_payloads=slice_payloads,
                    default_app_id=f"{ue_name}-app-{len(sessions) + 1}",
                )
                sessions.append(session_payload)
                entry["sessions"] = sessions

    for ue_name, gnb_name in ue_to_gnb.items():
        ue_payloads.setdefault(ue_name, {"name": ue_name}).setdefault("gnb", gnb_name)
    for gnb_name, upf_name in gnb_to_upf.items():
        gnb_payloads.setdefault(gnb_name, {"name": gnb_name}).setdefault("backhaul_upf", upf_name)

    for entry in ue_payloads.values():
        if isinstance(entry.get("sessions"), list):
            entry["sessions"] = _dedupe_sessions(entry["sessions"])
    for entry in gnb_payloads.values():
        if isinstance(entry.get("slices"), list):
            entry["slices"] = _merge_string_lists([], [str(item) for item in entry["slices"]])

    return TopologyGraphData(
        source_path=source_path,
        slices=slice_payloads,
        upfs=upf_payloads,
        gnbs=gnb_payloads,
        ues=ue_payloads,
        ue_to_gnb=ue_to_gnb,
        gnb_to_upf=gnb_to_upf,
        gnb_positions=gnb_positions,
        ue_positions=ue_positions,
    )


def _merge_named_items(
    existing: list[dict[str, object]],
    derived: dict[str, dict[str, object]],
    *,
    merge_item: Any,
) -> list[dict[str, object]]:
    merged = [copy.deepcopy(item) for item in existing]
    index_by_name = {
        str(item.get("name")): index
        for index, item in enumerate(merged)
        if isinstance(item.get("name"), str)
    }
    for name, payload in derived.items():
        if name in index_by_name:
            merged[index_by_name[name]] = merge_item(merged[index_by_name[name]], payload)
            continue
        merged.append(copy.deepcopy(payload))
    return merged


def _merge_slice_items(
    existing: list[dict[str, object]],
    derived: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    merged = [copy.deepcopy(item) for item in existing]
    index_by_ref: dict[str, int] = {}
    for index, item in enumerate(merged):
        slice_ref = None
        if item.get("sst") is not None and item.get("sd") is not None:
            slice_ref = _slice_ref_from_values(int(item["sst"]), _coerce_hex_sd(item["sd"]))
        elif isinstance(item.get("slice_ref"), str):
            slice_ref = str(item["slice_ref"])
        if slice_ref is not None:
            index_by_ref[slice_ref] = index

    for slice_ref, payload in derived.items():
        index = index_by_ref.get(slice_ref)
        if index is None:
            merged.append(copy.deepcopy(payload))
            continue
        item = merged[index]
        if item.get("sst") is None:
            item["sst"] = payload["sst"]
        if item.get("sd") is None:
            item["sd"] = payload["sd"]
        if item.get("label") is None and payload.get("label") is not None:
            item["label"] = payload["label"]
    return merged


def _merge_upf_item(existing: dict[str, object], derived: dict[str, object]) -> dict[str, object]:
    merged = copy.deepcopy(existing)
    for field in ("role", "dnn"):
        if field not in merged and field in derived:
            merged[field] = copy.deepcopy(derived[field])
    return merged


def _merge_gnb_item(existing: dict[str, object], derived: dict[str, object]) -> dict[str, object]:
    merged = copy.deepcopy(existing)
    for field in ("alias", "tac", "nci", "backhaul_upf"):
        if field not in merged and field in derived:
            merged[field] = copy.deepcopy(derived[field])
    merged["slices"] = _merge_string_lists(
        _coerce_string_list(merged.get("slices")),
        _coerce_string_list(derived.get("slices")),
    )
    return merged


def _merge_ue_item(existing: dict[str, object], derived: dict[str, object]) -> dict[str, object]:
    merged = copy.deepcopy(existing)
    for field in ("supi", "gnb", "key", "op", "op_type", "amf"):
        if field not in merged and field in derived:
            merged[field] = copy.deepcopy(derived[field])
    merged_policy = _merge_policy_payload(
        merged.get("free5gc_policy") if isinstance(merged.get("free5gc_policy"), dict) else None,
        derived.get("free5gc_policy") if isinstance(derived.get("free5gc_policy"), dict) else None,
    )
    if merged_policy:
        merged["free5gc_policy"] = merged_policy
    merged["sessions"] = _dedupe_sessions(
        [
            *[copy.deepcopy(item) for item in merged.get("sessions", []) if isinstance(item, dict)],
            *[copy.deepcopy(item) for item in derived.get("sessions", []) if isinstance(item, dict)],
        ]
    )
    return merged


def _align_graph_data_to_payload(
    graph: TopologyGraphData,
    payload: dict[str, Any],
) -> TopologyGraphData:
    gnb_name_by_alias = {
        str(item.get("alias")): str(item["name"])
        for item in payload.get("gnbs", [])
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("alias"), str)
    }
    gnb_name_set = {
        str(item["name"])
        for item in payload.get("gnbs", [])
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    ue_name_by_supi = {
        str(item.get("supi")): str(item["name"])
        for item in payload.get("ues", [])
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("supi"), str)
    }
    ue_name_set = {
        str(item["name"])
        for item in payload.get("ues", [])
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }

    gnb_rename_map: dict[str, str] = {}
    for gnb_name, gnb_payload in graph.gnbs.items():
        if gnb_name in gnb_name_set:
            continue
        alias = gnb_payload.get("alias")
        if isinstance(alias, str) and alias in gnb_name_by_alias:
            gnb_rename_map[gnb_name] = gnb_name_by_alias[alias]
            continue
        if gnb_name in gnb_name_by_alias:
            gnb_rename_map[gnb_name] = gnb_name_by_alias[gnb_name]

    ue_rename_map: dict[str, str] = {}
    for ue_name, ue_payload in graph.ues.items():
        if ue_name in ue_name_set:
            continue
        supi = ue_payload.get("supi")
        if isinstance(supi, str) and supi in ue_name_by_supi:
            ue_rename_map[ue_name] = ue_name_by_supi[supi]
            continue
        if ue_name in ue_name_by_supi:
            ue_rename_map[ue_name] = ue_name_by_supi[ue_name]

    if not gnb_rename_map and not ue_rename_map:
        return graph

    def rename_payload_map(
        source: dict[str, dict[str, object]],
        rename_map: dict[str, str],
        *,
        merge_item: Any,
    ) -> dict[str, dict[str, object]]:
        renamed: dict[str, dict[str, object]] = {}
        for key, value in source.items():
            target_key = rename_map.get(key, key)
            payload_value = copy.deepcopy(value)
            payload_value["name"] = target_key
            if target_key in renamed:
                renamed[target_key] = merge_item(renamed[target_key], payload_value)
                continue
            renamed[target_key] = payload_value
        return renamed

    renamed_gnbs = rename_payload_map(graph.gnbs, gnb_rename_map, merge_item=_merge_gnb_item)
    renamed_ues = rename_payload_map(graph.ues, ue_rename_map, merge_item=_merge_ue_item)
    renamed_gnb_positions = {
        gnb_rename_map.get(name, name): position
        for name, position in graph.gnb_positions.items()
    }
    renamed_ue_positions = {
        ue_rename_map.get(name, name): position
        for name, position in graph.ue_positions.items()
    }
    renamed_ue_to_gnb = {
        ue_rename_map.get(ue_name, ue_name): gnb_rename_map.get(gnb_name, gnb_name)
        for ue_name, gnb_name in graph.ue_to_gnb.items()
    }
    renamed_gnb_to_upf = {
        gnb_rename_map.get(gnb_name, gnb_name): upf_name
        for gnb_name, upf_name in graph.gnb_to_upf.items()
    }

    return TopologyGraphData(
        source_path=graph.source_path,
        slices=copy.deepcopy(graph.slices),
        upfs=copy.deepcopy(graph.upfs),
        gnbs=renamed_gnbs,
        ues=renamed_ues,
        ue_to_gnb=renamed_ue_to_gnb,
        gnb_to_upf=renamed_gnb_to_upf,
        gnb_positions=renamed_gnb_positions,
        ue_positions=renamed_ue_positions,
    )


def merge_topology_graph_payload(
    payload: dict[str, Any],
    graph_file: str | Path,
) -> dict[str, Any]:
    graph = _align_graph_data_to_payload(load_topology_graph(graph_file), payload)
    merged = copy.deepcopy(payload)
    merged["slices"] = _merge_slice_items(
        [item for item in merged.get("slices", []) if isinstance(item, dict)],
        graph.slices,
    )
    merged["upfs"] = _merge_named_items(
        [item for item in merged.get("upfs", []) if isinstance(item, dict)],
        graph.upfs,
        merge_item=_merge_upf_item,
    )
    merged["gnbs"] = _merge_named_items(
        [item for item in merged.get("gnbs", []) if isinstance(item, dict)],
        graph.gnbs,
        merge_item=_merge_gnb_item,
    )
    merged["ues"] = _merge_named_items(
        [item for item in merged.get("ues", []) if isinstance(item, dict)],
        graph.ues,
        merge_item=_merge_ue_item,
    )
    return merged


def _find_policy_target(ue: UeConfig) -> str | None:
    if ue.free5gc_policy.target_gnb is not None:
        return ue.free5gc_policy.target_gnb
    if ue.free5gc_policy.preferred_gnbs:
        return ue.free5gc_policy.preferred_gnbs[0]
    return None


def resolve_scenario_topology(scenario: ScenarioConfig) -> ResolvedScenarioTopology:
    gnb_names = {gnb.name for gnb in scenario.gnbs}
    upf_names = {upf.name for upf in scenario.upfs}

    ue_to_gnb = {
        ue.name: ue.gnb
        for ue in scenario.ues
        if ue.gnb is not None
    }
    gnb_to_upf = {
        gnb.name: gnb.backhaul_upf
        for gnb in scenario.gnbs
        if gnb.backhaul_upf is not None
    }
    gnb_positions: dict[str, NodePosition] = {}
    ue_positions: dict[str, NodePosition] = {}

    source_graph_file = scenario.topology.graph_file
    if source_graph_file is not None:
        graph = _align_graph_data_to_payload(
            load_topology_graph(source_graph_file),
            {
                "gnbs": [
                    {"name": gnb.name, "alias": gnb.alias}
                    for gnb in scenario.gnbs
                ],
                "ues": [
                    {"name": ue.name, "supi": ue.supi}
                    for ue in scenario.ues
                ],
            },
        )
        for gnb_name, position in graph.gnb_positions.items():
            if gnb_name in gnb_names:
                gnb_positions[gnb_name] = position
        for ue_name, position in graph.ue_positions.items():
            if ue_name in {ue.name for ue in scenario.ues}:
                ue_positions[ue_name] = position
        for ue_name, gnb_name in graph.ue_to_gnb.items():
            if ue_name in {ue.name for ue in scenario.ues} and gnb_name in gnb_names:
                ue_to_gnb[ue_name] = gnb_name
        for gnb_name, upf_name in graph.gnb_to_upf.items():
            if gnb_name in gnb_names and upf_name in upf_names:
                gnb_to_upf[gnb_name] = upf_name

    for ue in scenario.ues:
        policy_target = _find_policy_target(ue)
        if policy_target is not None:
            ue_to_gnb[ue.name] = policy_target

    resolved_gnb_to_upf: dict[str, str] = {}
    for gnb in scenario.gnbs:
        candidate = gnb_to_upf.get(gnb.name)
        if candidate is None and len(scenario.upfs) == 1:
            candidate = scenario.upfs[0].name
        if candidate is None:
            raise ValueError(f"gNB {gnb.name} has no resolved backhaul UPF")
        if candidate not in upf_names:
            raise ValueError(f"gNB {gnb.name} resolved unknown UPF {candidate}")
        resolved_gnb_to_upf[gnb.name] = candidate

    resolved_ue_to_gnb: dict[str, str] = {}
    for ue in scenario.ues:
        target_gnb = ue_to_gnb.get(ue.name)
        if target_gnb is None:
            raise ValueError(f"UE {ue.name} has no resolved target gNB")
        if target_gnb not in gnb_names:
            raise ValueError(f"UE {ue.name} resolved unknown gNB {target_gnb}")
        resolved_ue_to_gnb[ue.name] = target_gnb

    return ResolvedScenarioTopology(
        ue_to_gnb=resolved_ue_to_gnb,
        gnb_to_upf=resolved_gnb_to_upf,
        gnb_positions=gnb_positions,
        ue_positions=ue_positions,
        source_graph_file=source_graph_file,
    )