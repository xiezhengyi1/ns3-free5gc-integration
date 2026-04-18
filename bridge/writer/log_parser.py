"""Semantic parsers for free5GC and UERANSIM log streams."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import time

from bridge.common.schema import SimEvent


_COMPOSE_PREFIX_RE = re.compile(r"^(?P<service>[A-Za-z0-9_.-]+)\s+\|\s?(?P<body>.*)$")
_DOCKER_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T[^\s]+\s+")
_FREE5GC_LEVEL_RE = re.compile(r"\[(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\]")
_FREE5GC_NF_RE = re.compile(r"\[(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\]\[(?P<nf>[A-Z0-9-]+)\]")
_UERANSIM_META_RE = re.compile(
    r"^\[[^\]]+\]\s+\[(?P<component>[^\]]+)\]\s+\[(?P<level>[^\]]+)\]\s+(?P<message>.*)$"
)
_SUPI_RE = re.compile(r"(imsi-\d+)")
_PSI_RE = re.compile(r"(?:PSI|PDU [Ss]ession)\[(?P<psi>\d+)\]")
_UPF_RE = re.compile(r"UPF(?:\[|\s+)(?P<upf>[^\]\s,]+)")
_TUN_RE = re.compile(r"TUN interface\[(?P<ifname>[^,\]]+),\s*(?P<ip>[^\]]+)\]")

_FREE5GC_RULES: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"Handle Registration Request", re.IGNORECASE), "free5gc.registration_request", "ue"),
    (re.compile(r"Registration complete", re.IGNORECASE), "free5gc.registration_complete", "ue"),
    (re.compile(r"Send Registration Reject|Registration Reject", re.IGNORECASE), "free5gc.registration_reject", "ue"),
    (re.compile(r"Authentication procedure failed", re.IGNORECASE), "free5gc.authentication_failure", "ue"),
    (re.compile(r"Authenticate Request Error|GenerateAuthDataApi error", re.IGNORECASE), "free5gc.authentication_backend_error", "nf"),
    (re.compile(r"UE Security Context is not Available", re.IGNORECASE), "free5gc.security_context_missing", "ue"),
    (re.compile(r"Receive Create SM Context Request", re.IGNORECASE), "free5gc.pdu_session_create_request", "pdu_session"),
    (re.compile(r"PDU Session Establishment", re.IGNORECASE), "free5gc.pdu_session_establishment", "pdu_session"),
    (re.compile(r"Sending PFCP Association Request", re.IGNORECASE), "free5gc.pfcp_association_request", "upf"),
    (re.compile(r"Received PFCP Association Setup Accepted Response|Handle PFCP Association Setup Request", re.IGNORECASE), "free5gc.pfcp_association_ready", "upf"),
    (re.compile(r"PFCP Heartbeat", re.IGNORECASE), "free5gc.pfcp_heartbeat", "upf"),
    (re.compile(r"NG Setup", re.IGNORECASE), "free5gc.ng_setup", "gnb"),
]

_UERANSIM_RULES: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"SCTP connection established", re.IGNORECASE), "ueransim.gnb_sctp_connected", "gnb"),
    (re.compile(r"NG Setup procedure is successful", re.IGNORECASE), "ueransim.ng_setup_success", "gnb"),
    (re.compile(r"NG Setup procedure is failed", re.IGNORECASE), "ueransim.ng_setup_failure", "gnb"),
    (re.compile(r"Cell selection failure", re.IGNORECASE), "ueransim.cell_selection_failure", "ue"),
    (re.compile(r"RRC connection established", re.IGNORECASE), "ueransim.rrc_connection_established", "ue"),
    (re.compile(r"Registration is successful|Initial Registration is successful|Mobility Registration is successful", re.IGNORECASE), "ueransim.registration_success", "ue"),
    (re.compile(r"Registration failed|Initial Registration failed|Registration Reject", re.IGNORECASE), "ueransim.registration_failure", "ue"),
    (re.compile(r"PDU Session establishment is successful", re.IGNORECASE), "ueransim.pdu_session_established", "pdu_session"),
    (re.compile(r"PDU Session Establishment Reject|PDU session establishment could not be triggered", re.IGNORECASE), "ueransim.pdu_session_failure", "pdu_session"),
    (re.compile(r"Connection setup for PDU session\[\d+\] is successful", re.IGNORECASE), "ueransim.tun_setup_success", "pdu_session"),
    (re.compile(r"UE switches to state", re.IGNORECASE), "ueransim.ue_state_change", "ue"),
    (re.compile(r"Selected cell plmn", re.IGNORECASE), "ueransim.selected_cell", "ue"),
]


@dataclass(slots=True)
class ObservationClock:
    tick_ms: int
    started_monotonic: float = field(default_factory=time.monotonic)

    def elapsed_ms(self) -> int:
        return max(0, int((time.monotonic() - self.started_monotonic) * 1000))

    def current_tick(self) -> int:
        return self.elapsed_ms() // max(1, self.tick_ms)


def _split_compose_line(raw_line: str) -> tuple[str, str]:
    text = raw_line.rstrip("\n")
    match = _COMPOSE_PREFIX_RE.match(text)
    if not match:
        return "unknown", text
    return match.group("service"), match.group("body")


def _strip_docker_timestamp(body: str) -> str:
    return _DOCKER_TIMESTAMP_RE.sub("", body, count=1)


def _build_event(
    *,
    run_id: str,
    tick_index: int,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload_json: dict[str, object],
) -> SimEvent:
    return SimEvent(
        run_id=run_id,
        tick_index=tick_index,
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        payload_json=payload_json,
    )


def _event_identity(
    *,
    default_id: str,
    entity_type: str,
    service: str,
    supi: str | None = None,
    psi: str | None = None,
    upf: str | None = None,
) -> str:
    if entity_type == "ue" and supi:
        return supi
    if entity_type == "pdu_session" and psi:
        return f"{default_id}:psi-{psi}"
    if entity_type == "upf" and upf:
        return upf
    if entity_type == "nf":
        return service
    return default_id


def parse_free5gc_compose_line(
    raw_line: str,
    *,
    run_id: str,
    scenario_id: str,
    tick_index: int,
) -> list[SimEvent]:
    service, body = _split_compose_line(raw_line)
    message = _strip_docker_timestamp(body)
    if not message:
        return []

    level_match = _FREE5GC_LEVEL_RE.search(message)
    nf_match = _FREE5GC_NF_RE.search(message)
    supi_match = _SUPI_RE.search(message)
    psi_match = _PSI_RE.search(message)
    upf_match = _UPF_RE.search(message)

    level = level_match.group(1).lower() if level_match else None
    nf = nf_match.group("nf").lower() if nf_match else None
    supi = supi_match.group(1) if supi_match else None
    psi = psi_match.group("psi") if psi_match else None
    upf = upf_match.group("upf") if upf_match else None

    payload = {
        "scenario_id": scenario_id,
        "source": "free5gc",
        "service": service,
        "nf": nf,
        "level": level,
        "message": message,
        "raw_line": raw_line.rstrip("\n"),
    }
    if supi:
        payload["supi"] = supi
    if psi:
        payload["psi"] = int(psi)
    if upf:
        payload["upf"] = upf

    for pattern, event_type, entity_type in _FREE5GC_RULES:
        if not pattern.search(message):
            continue
        entity_id = _event_identity(
            default_id=service,
            entity_type=entity_type,
            service=service,
            supi=supi,
            psi=psi,
            upf=upf,
        )
        return [
            _build_event(
                run_id=run_id,
                tick_index=tick_index,
                event_type=event_type,
                entity_type=entity_type,
                entity_id=entity_id,
                payload_json=payload,
            )
        ]

    if level in {"warn", "error", "fatal"}:
        return [
            _build_event(
                run_id=run_id,
                tick_index=tick_index,
                event_type=f"free5gc.{level}",
                entity_type="nf",
                entity_id=service,
                payload_json=payload,
            )
        ]

    return []


def _ueransim_default_identity(service: str) -> tuple[str, str]:
    if service.startswith("ue-"):
        return "ue", service.removeprefix("ue-")
    if service.startswith("ueransim-"):
        return "gnb", service.removeprefix("ueransim-")
    return "gnb", service


def parse_ueransim_compose_line(
    raw_line: str,
    *,
    run_id: str,
    scenario_id: str,
    tick_index: int,
) -> list[SimEvent]:
    service, body = _split_compose_line(raw_line)
    message = _strip_docker_timestamp(body)
    if not message:
        return []

    default_entity_type, default_entity_id = _ueransim_default_identity(service)
    meta_match = _UERANSIM_META_RE.match(message)
    component = None
    level = None
    semantic_message = message
    if meta_match:
        component = meta_match.group("component")
        level = meta_match.group("level").lower()
        semantic_message = meta_match.group("message")

    psi_match = _PSI_RE.search(semantic_message)
    tun_match = _TUN_RE.search(semantic_message)
    psi = psi_match.group("psi") if psi_match else None

    payload = {
        "scenario_id": scenario_id,
        "source": "ueransim",
        "service": service,
        "component": component,
        "level": level,
        "message": semantic_message,
        "raw_line": raw_line.rstrip("\n"),
    }
    if psi:
        payload["psi"] = int(psi)
    if tun_match:
        payload["tun_ifname"] = tun_match.group("ifname")
        payload["ip_address"] = tun_match.group("ip")

    for pattern, event_type, entity_type in _UERANSIM_RULES:
        if not pattern.search(semantic_message):
            continue
        entity_id = _event_identity(
            default_id=default_entity_id,
            entity_type=entity_type,
            service=service,
            psi=psi,
        )
        if entity_type == "ue" and default_entity_type == "gnb":
            entity_id = service
        return [
            _build_event(
                run_id=run_id,
                tick_index=tick_index,
                event_type=event_type,
                entity_type=entity_type,
                entity_id=entity_id,
                payload_json=payload,
            )
        ]

    if level in {"warn", "error", "fatal", "err"}:
        normalized_level = "error" if level in {"error", "fatal", "err"} else "warn"
        return [
            _build_event(
                run_id=run_id,
                tick_index=tick_index,
                event_type=f"ueransim.{normalized_level}",
                entity_type=default_entity_type,
                entity_id=default_entity_id,
                payload_json=payload,
            )
        ]

    return []