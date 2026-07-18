"""Session state gate that activates split-mode ns-3 flows from control-plane events."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from bridge.common.scenario import ScenarioConfig, load_scenario

from .config import SplitModeConfig, load_split_mode_config


@dataclass(slots=True)
class SessionState:
    session_ref: str
    ue_name: str
    supi: str
    psi: int
    flow_ids: tuple[str, ...]
    registered: bool = False
    pdu_established: bool = False
    active: bool = False
    last_event_type: str | None = None
    last_tick_index: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_ref": self.session_ref,
            "ue_name": self.ue_name,
            "supi": self.supi,
            "psi": self.psi,
            "flow_ids": list(self.flow_ids),
            "registered": self.registered,
            "pdu_established": self.pdu_established,
            "active": self.active,
            "last_event_type": self.last_event_type,
            "last_tick_index": self.last_tick_index,
        }


def _load_split_or_base(path: Path) -> tuple[ScenarioConfig, SplitModeConfig | None]:
    raw_text = path.read_text(encoding="utf-8")
    if "base_scenario" in raw_text:
        split = load_split_mode_config(path)
        return split.control_plane_scenario, split
    return load_scenario(path), None


def _build_state_maps(scenario: ScenarioConfig) -> tuple[dict[str, SessionState], dict[str, str], dict[tuple[str, int], str]]:
    flow_ids_by_session: dict[str, list[str]] = {}
    for flow in scenario.flows:
        if not flow.session_ref:
            raise ValueError(f"flow {flow.flow_id} missing session_ref for split-mode")
        flow_ids_by_session.setdefault(flow.session_ref, []).append(flow.flow_id)

    session_by_ref: dict[str, SessionState] = {}
    supi_to_ue: dict[str, str] = {}
    psi_map: dict[tuple[str, int], str] = {}
    for ue in scenario.ues:
        supi_to_ue[ue.supi] = ue.name
        for psi, session in enumerate(ue.sessions, start=1):
            session_by_ref[session.session_ref] = SessionState(
                session_ref=session.session_ref,
                ue_name=ue.name,
                supi=ue.supi,
                psi=psi,
                flow_ids=tuple(flow_ids_by_session.get(session.session_ref, ())),
            )
            psi_map[(ue.name, psi)] = session.session_ref
    return session_by_ref, supi_to_ue, psi_map


def _parse_entity_session(entity_id: str) -> tuple[str, int] | None:
    if ":psi-" not in entity_id:
        return None
    ue_name, psi_text = entity_id.split(":psi-", 1)
    return ue_name, int(psi_text)


def _resolve_ueransim_ue_name(entity_id: str, ue_names: set[str]) -> str | None:
    """Map either a Compose service name or a prefixed container name to a UE.

    Compose prefixes `container_name` with the project name for isolated worker
    instances (for example, `nrint-w1-ue-ue1`).  The event schema intentionally
    preserves that source identity, so readiness and session activation must
    normalize it at the consumer boundary.
    """
    if entity_id in ue_names:
        return entity_id
    matches = [
        ue_name
        for ue_name in ue_names
        if entity_id == f"ue-{ue_name}" or entity_id.endswith(f"-ue-{ue_name}")
    ]
    return matches[0] if len(matches) == 1 else None


def _resolve_session_ref_from_payload(
    payload: dict[str, Any],
    supi_to_ue: dict[str, str],
    psi_map: dict[tuple[str, int], str],
) -> str | None:
    supi = str(payload.get("supi", "")).strip()
    psi_value = payload.get("psi")
    if not supi or psi_value is None:
        return None
    ue_name = supi_to_ue.get(supi)
    if not ue_name:
        return None
    try:
        psi = int(psi_value)
    except (TypeError, ValueError):
        return None
    return psi_map.get((ue_name, psi))


def _read_event_rows(connection: sqlite3.Connection, last_id: int) -> list[sqlite3.Row]:
    connection.row_factory = sqlite3.Row
    return list(
        connection.execute(
            """
            SELECT id, tick_index, event_type, entity_id, payload_json
            FROM sim_event
            WHERE id > ?
            ORDER BY id ASC
            """,
            (last_id,),
        )
    )


def _load_payload(raw_value: str) -> dict[str, Any]:
    loaded = json.loads(raw_value)
    if not isinstance(loaded, dict):
        raise ValueError("sim_event.payload_json must be a JSON object")
    return loaded


def _rewrite_flow_profile(flow_profile_file: Path, session_by_ref: dict[str, SessionState]) -> None:
    lines = flow_profile_file.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError(f"flow profile file is empty: {flow_profile_file}")
    header = lines[0].split("\t")
    if "session_ref" not in header or "enabled" not in header:
        raise ValueError("split flow profile requires session_ref and enabled columns")
    session_index = header.index("session_ref")
    enabled_index = header.index("enabled")
    updated_lines = [lines[0]]
    for line in lines[1:]:
        if not line.strip():
            continue
        columns = line.split("\t")
        if len(columns) < len(header):
            columns.extend([""] * (len(header) - len(columns)))
        session_ref = columns[session_index]
        state = session_by_ref.get(session_ref)
        if state is None:
            raise ValueError(f"flow profile references unknown session_ref {session_ref}")
        columns[enabled_index] = "true" if state.active else "false"
        updated_lines.append("\t".join(columns))
    temp_path = flow_profile_file.with_suffix(flow_profile_file.suffix + ".tmp")
    temp_path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
    temp_path.replace(flow_profile_file)


def _write_runtime_state(path: Path, *, last_event_id: int, session_by_ref: dict[str, SessionState]) -> None:
    sessions = [state.to_dict() for state in session_by_ref.values()]
    payload = {
        "kpi_authority": "ns3",
        "last_event_id": last_event_id,
        "active_session_count": sum(1 for item in sessions if item["active"]),
        "registered_ue_count": len({item["ue_name"] for item in sessions if item["registered"]}),
        "sessions": sessions,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temp_path.replace(path)


def _apply_event(
    event_type: str,
    payload: dict[str, Any],
    entity_id: str,
    tick_index: int,
    session_by_ref: dict[str, SessionState],
    supi_to_ue: dict[str, str],
    psi_map: dict[tuple[str, int], str],
) -> bool:
    changed = False
    ue_names = set(supi_to_ue.values())
    if event_type in {"ueransim.registration_success", "free5gc.registration_complete"}:
        ue_name = (
            _resolve_ueransim_ue_name(entity_id, ue_names)
            if event_type.startswith("ueransim.")
            else supi_to_ue.get(str(payload.get("supi", "")))
        )
        if not ue_name:
            raise ValueError(f"unable to resolve UE for event {event_type}")
        for state in session_by_ref.values():
            if state.ue_name != ue_name:
                continue
            new_active = state.pdu_established
            changed = changed or state.registered is not True or state.active != new_active
            state.registered = True
            state.active = new_active
            state.last_event_type = event_type
            state.last_tick_index = tick_index
        return changed
    if event_type in {
        "ueransim.registration_failure",
        "free5gc.registration_reject",
        "free5gc.authentication_failure",
        "ueransim.error",
        "free5gc.error",
    }:
        ue_name = (
            _resolve_ueransim_ue_name(entity_id, ue_names)
            if event_type.startswith("ueransim.")
            else supi_to_ue.get(str(payload.get("supi", "")))
        )
        if not ue_name:
            return False
        for state in session_by_ref.values():
            if state.ue_name != ue_name:
                continue
            changed = changed or state.registered or state.pdu_established or state.active
            state.registered = False
            state.pdu_established = False
            state.active = False
            state.last_event_type = event_type
            state.last_tick_index = tick_index
        return changed
    if event_type in {
        "ueransim.pdu_session_established",
        "ueransim.pdu_session_failure",
        "ueransim.tun_setup_success",
        "free5gc.pdu_session_establishment",
    }:
        session_ref: str | None = None
        parsed = _parse_entity_session(entity_id)
        if parsed is not None:
            ue_name, psi = parsed
            resolved_ue_name = _resolve_ueransim_ue_name(ue_name, ue_names)
            if resolved_ue_name is not None:
                session_ref = psi_map.get((resolved_ue_name, psi))
        if session_ref is None:
            session_ref = _resolve_session_ref_from_payload(payload, supi_to_ue, psi_map)
        if session_ref is None:
            raise ValueError(f"unknown session mapping for event {event_type} entity_id={entity_id}")
        state = session_by_ref[session_ref]
        next_pdu = event_type in {"ueransim.pdu_session_established", "ueransim.tun_setup_success", "free5gc.pdu_session_establishment"}
        next_active = state.registered and next_pdu
        changed = state.pdu_established != next_pdu or state.active != next_active
        state.pdu_established = next_pdu
        state.active = next_active
        state.last_event_type = event_type
        state.last_tick_index = tick_index
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Activate split-mode ns-3 flows from control-plane events")
    parser.add_argument("--split-scenario", required=True)
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--flow-profile-file", required=True)
    parser.add_argument("--runtime-state-file", required=True)
    parser.add_argument("--poll-ms", type=int, default=200)
    args = parser.parse_args(argv)

    scenario, _ = _load_split_or_base(Path(args.split_scenario).expanduser().resolve())
    session_by_ref, supi_to_ue, psi_map = _build_state_maps(scenario)
    flow_profile_file = Path(args.flow_profile_file).expanduser().resolve()
    runtime_state_file = Path(args.runtime_state_file).expanduser().resolve()
    last_id = 0
    _rewrite_flow_profile(flow_profile_file, session_by_ref)
    _write_runtime_state(runtime_state_file, last_event_id=last_id, session_by_ref=session_by_ref)
    print(
        json.dumps(
            {
                "component": "split-gate",
                "status": "started",
                "flow_profile_file": str(flow_profile_file),
                "runtime_state_file": str(runtime_state_file),
                "sessions": len(session_by_ref),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    state_db = Path(args.state_db).expanduser().resolve()
    while True:
        if not state_db.exists():
            time.sleep(max(0.05, args.poll_ms / 1000.0))
            continue
        with sqlite3.connect(state_db) as connection:
            rows = _read_event_rows(connection, last_id)
        changed = False
        for row in rows:
            payload = _load_payload(str(row["payload_json"]))
            event_type = str(row["event_type"])
            entity_id = str(row["entity_id"])
            tick_index = int(row["tick_index"])
            event_changed = _apply_event(
                event_type,
                payload,
                entity_id,
                tick_index,
                session_by_ref,
                supi_to_ue,
                psi_map,
            )
            changed = event_changed or changed
            print(
                json.dumps(
                    {
                        "component": "split-gate",
                        "event_id": int(row["id"]),
                        "tick_index": tick_index,
                        "event_type": event_type,
                        "entity_id": entity_id,
                        "changed": event_changed,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            last_id = int(row["id"])
        if changed:
            _rewrite_flow_profile(flow_profile_file, session_by_ref)
            _write_runtime_state(runtime_state_file, last_event_id=last_id, session_by_ref=session_by_ref)
            print(
                json.dumps(
                    {
                        "component": "split-gate",
                        "status": "updated",
                        "last_event_id": last_id,
                        "active_session_count": sum(1 for state in session_by_ref.values() if state.active),
                        "registered_ue_count": len({state.ue_name for state in session_by_ref.values() if state.registered}),
                        "sessions": [state.to_dict() for state in session_by_ref.values()],
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        time.sleep(max(0.05, args.poll_ms / 1000.0))


if __name__ == "__main__":
    raise SystemExit(main())
