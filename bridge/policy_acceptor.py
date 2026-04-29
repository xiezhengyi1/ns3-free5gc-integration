"""Unified policy execution gateway for real PCF dispatch and ns-3 application."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import socket
import tempfile
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import requests


EXECUTION_PATH = "/policy-executions"
HEALTHCHECK_PATH = f"{EXECUTION_PATH}/launch-healthcheck"
LEGACY_PATHS = {
    "/pcf/policies",
    "/npcf-am-policy-control/v1/policies",
    "/monitor/status",
}
AM_POLICY_TYPE = "PcfAmPolicyControlPolicyAssociation"
URSP_POLICY_TYPE = "UrspRuleRequest"
QOS_TOLERANCE_RATIO = 0.10


def _emit_policy_event(event: str, *, policy_id: str, policy_type: str, detail: str) -> None:
    print(
        f"[policy] event={event} policy_id={policy_id} policy_type={policy_type} detail={detail}",
        flush=True,
    )


class PolicyError(ValueError):
    """Raised when a policy request cannot be executed."""

    def __init__(self, message: str, *, status_code: int = 400, phase: str = "validation") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.phase = phase


class UpstreamPcfDispatcher(Protocol):
    def dispatch(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Dispatch one policy to the upstream PCF."""

    def healthcheck(self) -> tuple[bool, str]:
        """Return whether the upstream PCF is reachable enough for experiment startup."""


class RequestsUpstreamPcfDispatcher:
    """Best-effort HTTP adapter for the live free5GC PCF SBI."""

    def __init__(
        self,
        *,
        base_url: str,
        request_timeout_sec: float = 5.0,
        callback_base_url: str = "",
        request_retry_count: int = 3,
        retry_backoff_sec: float = 1.0,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.request_timeout_sec = max(0.1, float(request_timeout_sec or 5.0))
        self.callback_base_url = str(callback_base_url or "").rstrip("/")
        self.request_retry_count = max(1, int(request_retry_count or 1))
        self.retry_backoff_sec = max(0.0, float(retry_backoff_sec or 0.0))
        self._session = requests.Session()
        self._session.trust_env = False

    def dispatch(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url:
            raise PolicyError("upstream PCF base URL is not configured", status_code=500, phase="upstream_pcf")

        policy_type = payload["policy_type"]
        if policy_type == URSP_POLICY_TYPE:
            raise PolicyError(
                "UrspRuleRequest is not supported because free5GC Npcf_UEPolicyControl is not implemented",
                status_code=501,
                phase="unsupported_policy_type",
            )

        path, body = self._build_request(policy_type, payload)
        endpoint = f"{self.base_url}{path}"
        last_error: PolicyError | None = None
        for attempt in range(1, self.request_retry_count + 1):
            try:
                response = self._session.post(endpoint, json=body, timeout=self.request_timeout_sec)
            except requests.exceptions.RequestException as exc:
                last_error = PolicyError(f"upstream PCF request failed: {exc}", status_code=502, phase="upstream_pcf")
                if attempt >= self.request_retry_count:
                    raise last_error from exc
                time.sleep(self.retry_backoff_sec)
                continue

            try:
                response_payload = response.json()
            except ValueError:
                response_payload = {"raw_response": response.text}

            summary = {
                "status": "success" if response.ok else "failed",
                "endpoint": endpoint,
                "request_body": body,
                "response_code": response.status_code,
                "response_body": response_payload,
            }
            if response.ok:
                return summary

            error = (
                response_payload.get("detail")
                if isinstance(response_payload, dict) and response_payload.get("detail")
                else response_payload.get("error")
                if isinstance(response_payload, dict) and response_payload.get("error")
                else response.text
            )
            last_error = PolicyError(
                f"upstream PCF rejected policy: {str(error or '').strip() or 'unknown upstream error'}",
                status_code=502,
                phase="upstream_pcf",
            )
            if response.status_code < 500 or attempt >= self.request_retry_count:
                raise last_error
            time.sleep(self.retry_backoff_sec)

        if last_error is not None:
            raise last_error
        raise PolicyError("upstream PCF request failed without a response", status_code=502, phase="upstream_pcf")

    def healthcheck(self) -> tuple[bool, str]:
        if not self.base_url:
            return False, "upstream PCF base URL is not configured"
        try:
            response = self._session.get(self.base_url, timeout=self.request_timeout_sec)
            status_code = int(response.status_code)
            reason = str(response.reason or "").strip()
            response.close()
        except requests.exceptions.RequestException as exc:
            return False, f"upstream request failed: {exc}"
        detail = f"http {status_code}{(' ' + reason) if reason else ''}"
        return (200 <= status_code < 500), detail

    def _build_request(self, policy_type: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        if policy_type == AM_POLICY_TYPE:
            request = payload["policy_details"].get("request")
            if not isinstance(request, dict):
                raise PolicyError("AM policy payload requires policy_details.request", status_code=422)
            body = dict(request)
            if not str(body.get("notificationUri") or "").strip():
                body["notificationUri"] = self._default_callback_uri(payload["policy_id"], "am")
            return "/npcf-am-policy-control/v1/policies", body

        if policy_type != "SmPolicyDecision":
            raise PolicyError(f"unsupported policy_type: {policy_type}", status_code=422)

        policy_details = payload["policy_details"]
        body = dict(policy_details.get("upstreamSmPolicyContextData")) if isinstance(policy_details.get("upstreamSmPolicyContextData"), dict) else {}
        if not body:
            raise PolicyError("SmPolicyDecision requires policy_details.upstreamSmPolicyContextData", status_code=422)
        if not str(body.get("notificationUri") or "").strip():
            body["notificationUri"] = self._default_callback_uri(payload["policy_id"], "sm")
        return "/npcf-smpolicycontrol/v1/sm-policies", body

    def _default_callback_uri(self, policy_id: str, policy_scope: str) -> str:
        if self.callback_base_url:
            return f"{self.callback_base_url}/callbacks/{policy_scope}/{policy_id}"
        return f"http://127.0.0.1/callbacks/{policy_scope}/{policy_id}"


@dataclass(slots=True)
class PolicyRuntime:
    flow_profile_file: Path
    latest_snapshot_file: Path | None = None
    state_file: Path | None = None
    upstream_dispatcher: UpstreamPcfDispatcher | None = None
    default_timeout_ms: int = 10000
    poll_interval_ms: int = 200
    _lock: threading.RLock = field(init=False, repr=False)
    _execution_cache: dict[str, dict[str, Any]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.flow_profile_file = Path(self.flow_profile_file).expanduser().resolve()
        self.latest_snapshot_file = (
            Path(self.latest_snapshot_file).expanduser().resolve()
            if self.latest_snapshot_file is not None
            else None
        )
        self.state_file = Path(self.state_file).expanduser().resolve() if self.state_file is not None else None
        self._lock = threading.RLock()
        self._execution_cache = self._load_state()

    def execute_policy(self, raw_payload: dict[str, Any]) -> dict[str, Any]:
        payload = _validate_execution_payload(raw_payload, default_timeout_ms=self.default_timeout_ms)
        _emit_policy_event(
            "accepted",
            policy_id=payload["policy_id"],
            policy_type=payload["policy_type"],
            detail=f"request_id={payload['request_id']}",
        )
        try:
            with self._lock:
                rows, fieldnames = _read_flow_profiles(self.flow_profile_file)
                if not rows:
                    raise PolicyError("flow profile file is empty", status_code=409, phase="ns3_apply")
                baseline_snapshot = self._load_latest_snapshot()
                baseline_tick = int(baseline_snapshot.get("tick_index", -1)) if baseline_snapshot else -1
                mutation = _build_mutation(payload, rows)
                record = self._new_record(payload, mutation, baseline_tick)
                self._execution_cache[payload["policy_id"]] = record
                self._save_state()
        except PolicyError as exc:
            return _failure_response(payload, phase=exc.phase, error=str(exc), status_code=exc.status_code)

        try:
            _emit_policy_event(
                "dispatching",
                policy_id=payload["policy_id"],
                policy_type=payload["policy_type"],
                detail="sending policy to upstream PCF",
            )
            upstream_result = self._dispatch_upstream(payload)
        except PolicyError as exc:
            with self._lock:
                failed = self._mark_failed(record, phase=exc.phase, error=str(exc), status_code=exc.status_code)
                self._save_state()
            _emit_policy_event(
                "result",
                policy_id=payload["policy_id"],
                policy_type=payload["policy_type"],
                detail=f"status=failed phase={failed['phase']} status_code={failed['status_code']}",
            )
            return _record_to_response(failed)
        except Exception as exc:
            with self._lock:
                failed = self._mark_failed(record, phase="upstream_pcf", error=str(exc), status_code=502)
                self._save_state()
            _emit_policy_event(
                "result",
                policy_id=payload["policy_id"],
                policy_type=payload["policy_type"],
                detail=f"status=failed phase={failed['phase']} status_code={failed['status_code']}",
            )
            return _record_to_response(failed)

        try:
            with self._lock:
                updated_rows = _apply_mutation(rows, mutation)
                _write_flow_profiles(self.flow_profile_file, fieldnames, updated_rows)
                record["upstream"] = upstream_result
                record["mutation_summary"] = {
                    "policy_scope": mutation["policy_scope"],
                    "target_flow_ids": mutation["target_flow_ids"],
                    "requested_state": mutation["requested_state"],
                }
                record["phase"] = "waiting_for_ns3"
                self._execution_cache[payload["policy_id"]] = record
                self._save_state()
        except PolicyError as exc:
            with self._lock:
                failed = self._mark_failed(record, phase=exc.phase, error=str(exc), status_code=exc.status_code)
                failed["upstream"] = upstream_result
                self._save_state()
            return _record_to_response(failed)

        final_record = self._wait_for_ns3(record)
        _emit_policy_event(
            "result",
            policy_id=payload["policy_id"],
            policy_type=payload["policy_type"],
            detail=(
                f"status={final_record['status']} phase={final_record['phase']} "
                f"status_code={final_record['status_code']}"
            ),
        )
        with self._lock:
            self._execution_cache[payload["policy_id"]] = final_record
            self._save_state()
        return _record_to_response(final_record)

    def get_execution(self, policy_id: str) -> dict[str, Any]:
        normalized_policy_id = str(policy_id or "").strip()
        if not normalized_policy_id:
            return _failure_response({}, phase="validation", error="policy_id is required", status_code=400)
        with self._lock:
            record = self._execution_cache.get(normalized_policy_id)
        if record is None:
            return _failure_response(
                {"policy_id": normalized_policy_id},
                phase="query",
                error="policy_id not found",
                status_code=404,
            )
        return _record_to_response(record)

    def launch_healthcheck(self) -> dict[str, Any]:
        flow_profile_exists = self.flow_profile_file.exists()
        snapshot_exists = self.latest_snapshot_file.exists() if self.latest_snapshot_file is not None else True
        upstream_ok = True
        upstream_detail = "not configured"
        if self.upstream_dispatcher is not None and hasattr(self.upstream_dispatcher, "healthcheck"):
            upstream_ok, upstream_detail = self.upstream_dispatcher.healthcheck()
        healthy = flow_profile_exists and snapshot_exists and upstream_ok
        return {
            "status": "ok" if healthy else "failed",
            "healthy": healthy,
            "flow_profile_exists": flow_profile_exists,
            "latest_snapshot_exists": snapshot_exists,
            "upstream_ok": upstream_ok,
            "upstream_detail": upstream_detail,
        }

    def _dispatch_upstream(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.upstream_dispatcher is None:
            raise PolicyError("upstream dispatcher is not configured", status_code=500, phase="upstream_pcf")
        return self.upstream_dispatcher.dispatch(payload)

    def _wait_for_ns3(self, record: dict[str, Any]) -> dict[str, Any]:
        deadline = time.time() + float(record["timeout_ms"]) / 1000.0
        latest_snapshot: dict[str, Any] | None = None
        saw_newer_snapshot = False
        last_observation: dict[str, Any] | None = None
        while time.time() < deadline:
            try:
                latest_snapshot = self._load_latest_snapshot()
            except PolicyError as exc:
                return self._mark_failed(record, phase=exc.phase, error=str(exc), status_code=exc.status_code)

            if latest_snapshot is None:
                time.sleep(self.poll_interval_ms / 1000.0)
                continue

            latest_tick = int(latest_snapshot.get("tick_index", -1))
            if latest_tick <= int(record.get("baseline_tick", -1)):
                time.sleep(self.poll_interval_ms / 1000.0)
                continue

            saw_newer_snapshot = True

            execution_status, compliance_status, monitoring_data = _evaluate_record(record, latest_snapshot)
            record["applied_tick"] = latest_tick
            record["execution_status"] = execution_status
            record["compliance_status"] = compliance_status
            record["monitoring_data"] = _monitoring_payload(record, latest_snapshot, monitoring_data)
            if execution_status == "APPLIED" and compliance_status == "COMPLIANT":
                record["status"] = "success"
                record["phase"] = "completed"
                record["message"] = "Policy dispatched to the upstream PCF and applied by ns-3."
                record["status_code"] = 201
                return record

            last_observation = {
                "execution_status": execution_status,
                "compliance_status": compliance_status,
                "monitoring_data": dict(record.get("monitoring_data") or {}),
            }
            time.sleep(self.poll_interval_ms / 1000.0)

        if saw_newer_snapshot and last_observation is not None:
            record["execution_status"] = str(last_observation.get("execution_status") or "FAILED")
            record["compliance_status"] = str(last_observation.get("compliance_status") or "VIOLATED")
            record["monitoring_data"] = dict(last_observation.get("monitoring_data") or {})
            return self._mark_failed(
                record,
                phase="ns3_apply",
                error="ns-3 observed state did not converge to the requested policy before timeout",
                status_code=409,
            )

        latest_tick = int(latest_snapshot.get("tick_index", -1)) if isinstance(latest_snapshot, dict) else None
        record["monitoring_data"] = _monitoring_payload(record, latest_snapshot, {"latest_tick": latest_tick})
        return self._mark_failed(
            record,
            phase="ns3_apply_timeout",
            error=f"ns-3 did not apply the policy within {record['timeout_ms']} ms",
            status_code=504,
        )

    def _new_record(self, payload: dict[str, Any], mutation: dict[str, Any], baseline_tick: int) -> dict[str, Any]:
        return {
            "status": "pending",
            "status_code": 202,
            "phase": "validated",
            "request_id": payload["request_id"],
            "session_id": payload["session_id"],
            "snapshot_id": payload["snapshot_id"],
            "policy_id": payload["policy_id"],
            "policy_type": payload["policy_type"],
            "target_type": payload["target_type"],
            "flow_id": payload.get("flow_id", ""),
            "supi": payload.get("supi", ""),
            "timeout_ms": payload["timeout_ms"],
            "policy_details": payload["policy_details"],
            "baseline_tick": baseline_tick,
            "applied_tick": None,
            "execution_status": "PENDING",
            "compliance_status": "PENDING",
            "monitoring_data": {"baseline_tick": baseline_tick},
            "error": "",
            "mutation": mutation,
            "mutation_summary": {},
            "upstream": {},
            "message": "Policy execution is pending.",
            "updated_at": time.time(),
        }

    def _mark_failed(
        self,
        record: dict[str, Any],
        *,
        phase: str,
        error: str,
        status_code: int,
    ) -> dict[str, Any]:
        record["status"] = "failed"
        record["status_code"] = status_code
        record["phase"] = phase
        record["error"] = str(error or "").strip() or "policy execution failed"
        record["message"] = record["error"]
        record["updated_at"] = time.time()
        if record.get("execution_status") == "PENDING" and phase != "upstream_pcf":
            record["execution_status"] = "FAILED"
        if record.get("compliance_status") == "PENDING" and phase.startswith("ns3_apply"):
            record["compliance_status"] = "VIOLATED"
        return record

    def _load_latest_snapshot(self) -> dict[str, Any] | None:
        if self.latest_snapshot_file is None or not self.latest_snapshot_file.exists():
            return None
        try:
            payload = json.loads(self.latest_snapshot_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise PolicyError(f"latest snapshot is not valid JSON: {exc}", status_code=500, phase="snapshot") from exc
        if not isinstance(payload, dict):
            raise PolicyError("latest snapshot payload must be a JSON object", status_code=500, phase="snapshot")
        return payload

    def _load_state(self) -> dict[str, dict[str, Any]]:
        if self.state_file is None or not self.state_file.exists():
            return {}
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        executions = payload.get("executions")
        if not isinstance(executions, dict):
            return {}
        return {str(key): value for key, value in executions.items() if isinstance(value, dict)}

    def _save_state(self) -> None:
        if self.state_file is None:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": time.time(),
            "flow_profile_file": str(self.flow_profile_file),
            "latest_snapshot_file": str(self.latest_snapshot_file) if self.latest_snapshot_file is not None else "",
            "executions": self._execution_cache,
        }
        _atomic_write_json(self.state_file, payload)


def _coerce_non_empty(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise PolicyError(f"{field_name} is required")
    return text


def _coerce_optional(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _values_close(left: Any, right: Any, *, tolerance: float = 1e-6) -> bool:
    left_number = _to_float(left)
    right_number = _to_float(right)
    if left_number is None or right_number is None:
        return str(left) == str(right)
    return math.isclose(left_number, right_number, rel_tol=tolerance, abs_tol=tolerance)


def _within_ratio_tolerance(left: Any, right: Any, *, tolerance_ratio: float = QOS_TOLERANCE_RATIO) -> bool:
    left_number = _to_float(left)
    right_number = _to_float(right)
    if left_number is None or right_number is None:
        return str(left) == str(right)
    baseline = max(abs(right_number), 1e-9)
    return abs(left_number - right_number) <= baseline * max(0.0, float(tolerance_ratio))


def _within_upper_bound(left: Any, right: Any, *, tolerance_ratio: float = QOS_TOLERANCE_RATIO) -> bool:
    left_number = _to_float(left)
    right_number = _to_float(right)
    if left_number is None or right_number is None:
        return str(left) == str(right)
    return left_number <= right_number * (1.0 + max(0.0, float(tolerance_ratio)))


def _normalize_snssai(candidate: Any) -> str:
    if isinstance(candidate, dict):
        sst = candidate.get("sst")
        sd = candidate.get("sd")
        if sst is None or sd in (None, ""):
            return ""
        return f"{int(sst):02d}{str(sd).lower()}"
    text = str(candidate or "").strip().lower()
    if len(text) == 8 and text.isalnum():
        return text
    return ""


def _extract_flow_id(policy_details: dict[str, Any]) -> str:
    direct_flow_id = _coerce_optional(policy_details.get("flow_id") or policy_details.get("flowId"))
    if direct_flow_id:
        return direct_flow_id

    for bucket_name, id_field, prefixes in (
        ("pccRules", "pccRuleId", ("pcc-",)),
        ("qosDecs", "qosId", ("qos-",)),
        ("sessRules", "sessRuleId", ("sess-",)),
    ):
        bucket = policy_details.get(bucket_name)
        if not isinstance(bucket, dict) or not bucket:
            continue
        first_key = str(next(iter(bucket.keys()))).strip()
        first_value = bucket[first_key]
        if isinstance(first_value, dict):
            candidate = _coerce_optional(first_value.get("flow_id") or first_value.get("flowId") or first_value.get(id_field))
            stripped = _strip_known_prefix(candidate, prefixes)
            if stripped:
                return stripped
        stripped_key = _strip_known_prefix(first_key, prefixes)
        if stripped_key:
            return stripped_key
    return ""


def _strip_known_prefix(candidate: Any, prefixes: tuple[str, ...]) -> str:
    text = _coerce_optional(candidate)
    for prefix in prefixes:
        if text.startswith(prefix) and len(text) > len(prefix):
            return text[len(prefix) :]
    return text


def _resolve_qos_ref(qos_payload: dict[str, Any], qos_keys: list[str], target_row: dict[str, str]) -> int | None:
    candidates: list[Any] = [
        qos_payload.get("qosRef"),
        qos_payload.get("qos_ref"),
        qos_payload.get("qosId"),
        *qos_keys,
        target_row.get("qos_ref"),
    ]
    for candidate in candidates:
        parsed = _to_int(candidate)
        if parsed is not None:
            return parsed
    return 0 if any(_coerce_optional(candidate) for candidate in candidates) else None


def _extract_policy_filter(policy_details: dict[str, Any], flow_id: str) -> str:
    pcc_rules = policy_details.get("pccRules")
    if isinstance(pcc_rules, dict) and pcc_rules:
        for item in pcc_rules.values():
            if not isinstance(item, dict):
                continue
            for flow_info in item.get("flowInfos") or []:
                if not isinstance(flow_info, dict):
                    continue
                description = _coerce_optional(flow_info.get("flowDescription"))
                if description:
                    return description
    traffic_desc = policy_details.get("trafficDesc")
    if isinstance(traffic_desc, dict):
        for item in traffic_desc.get("contVers") or []:
            if not isinstance(item, dict):
                continue
            description = _coerce_optional(item.get("flowDescription"))
            if description:
                return description
        app_ids = traffic_desc.get("appIds")
        if isinstance(app_ids, list) and app_ids:
            return ",".join(str(item) for item in app_ids if str(item).strip())
    return flow_id


def _validate_execution_payload(payload: dict[str, Any], *, default_timeout_ms: int) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise PolicyError("request body must be a JSON object")

    request_id = _coerce_non_empty(payload.get("request_id"), "request_id")
    policy_id = _coerce_non_empty(payload.get("policy_id"), "policy_id")
    policy_type = _coerce_non_empty(payload.get("policy_type"), "policy_type")
    policy_details = payload.get("policy_details")
    if not isinstance(policy_details, dict):
        raise PolicyError("policy_details must be an object")

    nested_policy_id = _coerce_optional(policy_details.get("policy_id"))
    if nested_policy_id and nested_policy_id != policy_id:
        raise PolicyError("top-level policy_id does not match policy_details.policy_id")

    target_type = _coerce_optional(payload.get("target_type") or policy_details.get("target_type"))
    if not target_type:
        target_type = "ue" if policy_type == AM_POLICY_TYPE else "flow"

    flow_id = _coerce_optional(payload.get("flow_id") or _extract_flow_id(policy_details))
    supi = ""
    if policy_type == AM_POLICY_TYPE:
        request = policy_details.get("request")
        if not isinstance(request, dict):
            raise PolicyError("AM policy payload requires policy_details.request")
        supi = _coerce_non_empty(request.get("supi"), "request.supi")
    else:
        supi = _coerce_optional(payload.get("supi") or policy_details.get("supi"))

    if target_type == "flow" and not flow_id:
        raise PolicyError("flow-scoped policy requires flow_id")

    timeout_ms = _to_int(payload.get("timeout_ms"))
    if timeout_ms is None:
        timeout_ms = int(default_timeout_ms)
    if timeout_ms <= 0:
        raise PolicyError("timeout_ms must be positive")

    return {
        "request_id": request_id,
        "session_id": _coerce_optional(payload.get("session_id")),
        "snapshot_id": _coerce_optional(payload.get("snapshot_id")),
        "policy_id": policy_id,
        "policy_type": policy_type,
        "policy_details": policy_details,
        "target_type": target_type,
        "flow_id": flow_id,
        "supi": supi,
        "timeout_ms": timeout_ms,
    }


def _read_flow_profiles(flow_profile_file: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not flow_profile_file.exists():
        raise PolicyError(f"flow profile file not found: {flow_profile_file}", status_code=404, phase="ns3_apply")
    with flow_profile_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames:
            raise PolicyError("flow profile file is missing header", status_code=409, phase="ns3_apply")
        rows = [dict(row) for row in reader]
    return rows, list(reader.fieldnames)


def _write_flow_profiles(flow_profile_file: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    flow_profile_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=flow_profile_file.parent) as handle:
        temp_path = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    temp_path.replace(flow_profile_file)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    temp_path.replace(path)


def _slice_snssai_to_ref(rows: list[dict[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in rows:
        snssai = _coerce_optional(row.get("slice_snssai")).lower()
        slice_ref = _coerce_optional(row.get("slice_ref"))
        if snssai and slice_ref and snssai not in mapping:
            mapping[snssai] = slice_ref
    return mapping


def _build_mutation(payload: dict[str, Any], rows: list[dict[str, str]]) -> dict[str, Any]:
    policy_type = payload["policy_type"]
    if policy_type == "SmPolicyDecision":
        return _build_sm_mutation(payload, rows)
    if policy_type == URSP_POLICY_TYPE:
        raise PolicyError(
            "UrspRuleRequest is not supported because free5GC Npcf_UEPolicyControl is not implemented",
            status_code=501,
            phase="unsupported_policy_type",
        )
    if policy_type == AM_POLICY_TYPE:
        return _build_am_mutation(payload, rows)
    raise PolicyError(f"unsupported policy_type: {policy_type}", status_code=422)


def _build_sm_mutation(payload: dict[str, Any], rows: list[dict[str, str]]) -> dict[str, Any]:
    flow_id = payload["flow_id"]
    target_row = next((row for row in rows if _coerce_optional(row.get("flow_id")) == flow_id), None)
    if target_row is None:
        raise PolicyError(f"flow_id not found in flow profile: {flow_id}", status_code=404)

    policy_details = payload["policy_details"]
    qos_decs = policy_details.get("qosDecs")
    if not isinstance(qos_decs, dict) or not qos_decs:
        raise PolicyError("SmPolicyDecision requires qosDecs", status_code=422)
    qos_payload = next((item for item in qos_decs.values() if isinstance(item, dict)), None)
    if qos_payload is None:
        raise PolicyError("SmPolicyDecision qosDecs must contain an object", status_code=422)

    pcc_rules = policy_details.get("pccRules")
    pcc_payload = next((item for item in pcc_rules.values() if isinstance(item, dict)), None) if isinstance(pcc_rules, dict) else None
    qos_keys = [str(key) for key in qos_decs.keys()]
    allocated_dl = _to_float(qos_payload.get("maxbrDl")) or _to_float(qos_payload.get("gbrDl"))
    allocated_ul = _to_float(qos_payload.get("maxbrUl")) or _to_float(qos_payload.get("gbrUl"))
    if allocated_dl is None and allocated_ul is None:
        raise PolicyError("SmPolicyDecision requires maxbrDl/maxbrUl or gbrDl/gbrUl", status_code=422)

    updates: dict[str, str] = {
        "optimize_requested": str(True),
        "policy_filter": _extract_policy_filter(policy_details, flow_id),
    }
    requested_state: dict[str, Any] = {"flow_id": flow_id}
    if allocated_dl is not None:
        updates["allocated_bandwidth_dl_mbps"] = str(allocated_dl)
        updates["bandwidth_dl_mbps"] = str(allocated_dl)
        requested_state["allocated_bandwidth_dl_mbps"] = allocated_dl
    if allocated_ul is not None:
        updates["allocated_bandwidth_ul_mbps"] = str(allocated_ul)
        updates["bandwidth_ul_mbps"] = str(allocated_ul)
        requested_state["allocated_bandwidth_ul_mbps"] = allocated_ul

    guaranteed_dl = _to_float(qos_payload.get("gbrDl"))
    guaranteed_ul = _to_float(qos_payload.get("gbrUl"))
    if guaranteed_dl is not None:
        updates["guaranteed_bandwidth_dl_mbps"] = str(guaranteed_dl)
        requested_state["guaranteed_bandwidth_dl_mbps"] = guaranteed_dl
    if guaranteed_ul is not None:
        updates["guaranteed_bandwidth_ul_mbps"] = str(guaranteed_ul)
        requested_state["guaranteed_bandwidth_ul_mbps"] = guaranteed_ul

    latency_ms = _to_int(qos_payload.get("packetDelayBudget"))
    jitter_ms = _to_float(qos_payload.get("jitterReq"))
    loss_rate = _to_float(qos_payload.get("packetErrorRate"))
    priority = _to_int(qos_payload.get("priorityLevel"))
    precedence = _to_int(pcc_payload.get("precedence")) if isinstance(pcc_payload, dict) else None
    qos_ref = _resolve_qos_ref(qos_payload, qos_keys, target_row)

    if latency_ms is not None:
        updates["latency_ms"] = str(latency_ms)
        requested_state["latency_ms"] = latency_ms
    if jitter_ms is not None:
        updates["jitter_ms"] = str(jitter_ms)
        requested_state["jitter_ms"] = jitter_ms
    if loss_rate is not None:
        updates["loss_rate"] = str(loss_rate)
        requested_state["loss_rate"] = loss_rate
    if priority is not None:
        updates["priority"] = str(priority)
        requested_state["priority"] = priority
    if precedence is not None:
        updates["precedence"] = str(precedence)
        requested_state["precedence"] = precedence
    if qos_ref is not None:
        updates["qos_ref"] = str(qos_ref)
        requested_state["qos_ref"] = qos_ref

    return {
        "policy_scope": "flow",
        "target_flow_ids": [flow_id],
        "updates_by_flow_id": {flow_id: updates},
        "requested_state": requested_state,
    }


def _build_am_mutation(payload: dict[str, Any], rows: list[dict[str, str]]) -> dict[str, Any]:
    request = payload["policy_details"].get("request")
    if not isinstance(request, dict):
        raise PolicyError("AM policy payload requires request object", status_code=422)
    supi = _coerce_non_empty(request.get("supi"), "request.supi")
    candidate_snssais = [
        _normalize_snssai(item)
        for item in (request.get("allowedSnssais") or [])
        if _normalize_snssai(item)
    ]
    candidate_snssais.extend(
        _normalize_snssai(item)
        for item in (request.get("targetSnssais") or [])
        if _normalize_snssai(item)
    )
    target_snssai = next((item for item in candidate_snssais if item), "")
    if not target_snssai:
        raise PolicyError("AM policy requires allowedSnssais or targetSnssais", status_code=422)

    slice_ref_map = _slice_snssai_to_ref(rows)
    if target_snssai not in slice_ref_map:
        raise PolicyError(f"target SNSSAI is not present in current flow profile: {target_snssai}", status_code=422)

    target_flow_ids = [
        _coerce_optional(row.get("flow_id"))
        for row in rows
        if _coerce_optional(row.get("supi")) == supi
    ]
    target_flow_ids = [flow_id for flow_id in target_flow_ids if flow_id]
    if not target_flow_ids:
        raise PolicyError(f"no flows found for SUPI: {supi}", status_code=404)

    updates = {
        flow_id: {
            "slice_snssai": target_snssai,
            "slice_ref": slice_ref_map[target_snssai],
            "optimize_requested": str(True),
            "policy_filter": f"am-association:{payload['policy_id']}",
        }
        for flow_id in target_flow_ids
    }
    return {
        "policy_scope": "ue",
        "target_flow_ids": target_flow_ids,
        "updates_by_flow_id": updates,
        "requested_state": {
            "supi": supi,
            "slice_snssai": target_snssai,
        },
    }


def _apply_mutation(rows: list[dict[str, str]], mutation: dict[str, Any]) -> list[dict[str, str]]:
    updates_by_flow_id = mutation["updates_by_flow_id"]
    updated_rows: list[dict[str, str]] = []
    for row in rows:
        flow_id = _coerce_optional(row.get("flow_id"))
        if flow_id in updates_by_flow_id:
            next_row = dict(row)
            next_row.update(updates_by_flow_id[flow_id])
            updated_rows.append(next_row)
            continue
        updated_rows.append(dict(row))
    return updated_rows


def _monitoring_payload(
    record: dict[str, Any],
    snapshot: dict[str, Any] | None,
    monitoring_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    monitoring = {"baseline_tick": record.get("baseline_tick", -1)}
    if snapshot is not None:
        monitoring["latest_tick"] = snapshot.get("tick_index")
        monitoring["run_id"] = snapshot.get("run_id")
    if monitoring_data:
        monitoring.update(monitoring_data)
    return monitoring


def _evaluate_record(record: dict[str, Any], snapshot: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    mutation = record.get("mutation") or {}
    requested_state = mutation.get("requested_state") or {}
    policy_type = record.get("policy_type")
    if policy_type == "SmPolicyDecision":
        return _evaluate_sm_record(record, snapshot, requested_state)
    if policy_type == AM_POLICY_TYPE:
        return _evaluate_am_record(record, snapshot, requested_state)
    return "FAILED", "VIOLATED", {"reason": f"unsupported policy_type: {policy_type}"}


def _flow_by_id(snapshot: dict[str, Any], flow_id: str) -> dict[str, Any] | None:
    for item in snapshot.get("flows") or []:
        if isinstance(item, dict) and _coerce_optional(item.get("flow_id")) == flow_id:
            return item
    return None


def _slice_map(snapshot: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in snapshot.get("slices") or []:
        if not isinstance(item, dict):
            continue
        slice_id = _coerce_optional(item.get("slice_id"))
        snssai = _normalize_snssai({"sst": item.get("sst"), "sd": item.get("sd")})
        if slice_id and snssai:
            mapping[slice_id] = snssai
    return mapping


def _evaluate_sm_record(
    record: dict[str, Any],
    snapshot: dict[str, Any],
    requested_state: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    flow = _flow_by_id(snapshot, _coerce_optional(record.get("flow_id")))
    if flow is None:
        return "FAILED", "VIOLATED", {"reason": "flow not present in latest snapshot"}

    allocation = flow.get("allocation") if isinstance(flow.get("allocation"), dict) else {}
    telemetry = flow.get("telemetry") if isinstance(flow.get("telemetry"), dict) else {}
    applied = True
    if "allocated_bandwidth_dl_mbps" in requested_state:
        applied = applied and _within_ratio_tolerance(
            allocation.get("allocated_bandwidth_dl"),
            requested_state["allocated_bandwidth_dl_mbps"],
        )
    if "allocated_bandwidth_ul_mbps" in requested_state:
        applied = applied and _within_ratio_tolerance(
            allocation.get("allocated_bandwidth_ul"),
            requested_state["allocated_bandwidth_ul_mbps"],
        )

    compliant = applied
    if "latency_ms" in requested_state:
        compliant = compliant and _within_upper_bound(telemetry.get("latency"), requested_state["latency_ms"])
    if "jitter_ms" in requested_state:
        compliant = compliant and _within_upper_bound(telemetry.get("jitter"), requested_state["jitter_ms"])
    if "loss_rate" in requested_state:
        compliant = compliant and _within_upper_bound(telemetry.get("loss_rate"), requested_state["loss_rate"])

    return (
        "APPLIED" if applied else "FAILED",
        "COMPLIANT" if compliant else "VIOLATED",
        {
            "observed_flow_id": flow.get("flow_id"),
            "observed_allocation": allocation,
            "observed_telemetry": telemetry,
        },
    )


def _evaluate_am_record(
    record: dict[str, Any],
    snapshot: dict[str, Any],
    requested_state: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    supi = _coerce_optional(requested_state.get("supi") or record.get("supi"))
    expected_snssai = _coerce_optional(requested_state.get("slice_snssai")).lower()
    slice_map = _slice_map(snapshot)
    observed_ues: list[dict[str, Any]] = []
    for item in snapshot.get("ues") or []:
        if not isinstance(item, dict):
            continue
        if _coerce_optional(item.get("supi")) != supi:
            continue
        slice_id = _coerce_optional(item.get("slice_id"))
        observed_ues.append(
            {
                "ue_id": item.get("ue_id"),
                "slice_id": slice_id,
                "slice_snssai": slice_map.get(slice_id, ""),
            }
        )
    if not observed_ues:
        return "FAILED", "VIOLATED", {"reason": "UE not present in latest snapshot"}
    applied = all(_coerce_optional(item.get("slice_snssai")).lower() == expected_snssai for item in observed_ues)
    return (
        "APPLIED" if applied else "FAILED",
        "COMPLIANT" if applied else "VIOLATED",
        {
            "observed_supi": supi,
            "observed_ues": observed_ues,
        },
    )


def _failure_response(
    payload: dict[str, Any],
    *,
    phase: str,
    error: str,
    status_code: int,
) -> dict[str, Any]:
    return {
        "status": "failed",
        "status_code": status_code,
        "phase": phase,
        "error": str(error or "").strip() or "policy execution failed",
        "request_id": str(payload.get("request_id") or ""),
        "session_id": str(payload.get("session_id") or ""),
        "snapshot_id": str(payload.get("snapshot_id") or ""),
        "policy_id": str(payload.get("policy_id") or ""),
        "policy_type": str(payload.get("policy_type") or ""),
        "execution_status": "FAILED" if phase != "upstream_pcf" else "PENDING",
        "compliance_status": "VIOLATED" if phase.startswith("ns3_apply") else "PENDING",
    }


def _record_to_response(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": record.get("status", "failed"),
        "status_code": int(record.get("status_code", 500) or 500),
        "phase": record.get("phase", ""),
        "request_id": record.get("request_id", ""),
        "session_id": record.get("session_id", ""),
        "snapshot_id": record.get("snapshot_id", ""),
        "policy_id": record.get("policy_id", ""),
        "policy_type": record.get("policy_type", ""),
        "flow_id": record.get("flow_id", ""),
        "execution_status": record.get("execution_status", "PENDING"),
        "compliance_status": record.get("compliance_status", "PENDING"),
        "baseline_tick": record.get("baseline_tick"),
        "applied_tick": record.get("applied_tick"),
        "upstream": record.get("upstream", {}),
        "mutation_summary": record.get("mutation_summary", {}),
        "monitoring_data": record.get("monitoring_data", {}),
        "message": record.get("message", ""),
        "error": record.get("error", ""),
    }


class PolicyAcceptorHandler(BaseHTTPRequestHandler):
    runtime: PolicyRuntime | None = None

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        normalized_path = self._normalized_path()
        if normalized_path in LEGACY_PATHS:
            self._send_json(
                410,
                {
                    "status": "failed",
                    "error": "legacy policy endpoint is no longer supported",
                    "replacement": EXECUTION_PATH,
                },
            )
            return
        if normalized_path != EXECUTION_PATH:
            self._send_json(404, {"status": "failed", "error": "not found"})
            return
        if self.runtime is None:
            self._send_json(500, {"status": "failed", "error": "runtime is not configured"})
            return

        try:
            payload = self._read_json_body()
        except PolicyError as exc:
            self._send_json(exc.status_code, {"status": "failed", "error": str(exc)})
            return
        except json.JSONDecodeError as exc:
            self._send_json(400, {"status": "failed", "error": f"invalid JSON body: {exc}"})
            return

        response = self.runtime.execute_policy(payload)
        status_code = int(response.get("status_code", 500) or 500)
        self._send_json(status_code, response)

    def do_GET(self) -> None:
        normalized_path = self._normalized_path()
        if normalized_path in LEGACY_PATHS or normalized_path.startswith("/monitor/status/"):
            self._send_json(
                410,
                {
                    "status": "failed",
                    "error": "legacy monitoring endpoint is no longer supported",
                    "replacement": f"{EXECUTION_PATH}/{{policy_id}}",
                },
            )
            return
        if normalized_path == HEALTHCHECK_PATH:
            if self.runtime is None:
                self._send_json(500, {"status": "failed", "error": "runtime is not configured"})
                return
            response = self.runtime.launch_healthcheck()
            self._send_json(200 if response.get("healthy") else 503, response)
            return
        if not normalized_path.startswith(f"{EXECUTION_PATH}/"):
            self._send_json(404, {"status": "failed", "error": "not found"})
            return
        if self.runtime is None:
            self._send_json(500, {"status": "failed", "error": "runtime is not configured"})
            return

        policy_id = normalized_path.rsplit("/", 1)[-1].strip()
        response = self.runtime.get_execution(policy_id)
        self._send_json(int(response.get("status_code", 500) or 500), response)

    def _normalized_path(self) -> str:
        raw_path = self.path or "/"
        parsed = urlsplit(raw_path)
        return (parsed.path or raw_path.split("?", 1)[0] or "/").rstrip("/") or "/"

    def _read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length)
        if not raw:
            raise PolicyError("request body is empty")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise PolicyError("request body must be a JSON object")
        return payload

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified policy execution gateway for the ns-3/free5GC integration bed.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--flow-profile-file", required=True)
    parser.add_argument("--latest-snapshot-file")
    parser.add_argument("--state-file")
    parser.add_argument("--upstream-pcf-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--upstream-timeout-sec", type=float, default=5.0)
    parser.add_argument("--upstream-retry-count", type=int, default=3)
    parser.add_argument("--upstream-retry-backoff-sec", type=float, default=1.0)
    parser.add_argument("--callback-base-url", default="")
    parser.add_argument("--default-timeout-ms", type=int, default=10000)
    parser.add_argument("--poll-interval-ms", type=int, default=200)
    return parser


def _decode_proc_net_address(value: str) -> tuple[str, int]:
    host_hex, port_hex = value.split(":", 1)
    port = int(port_hex, 16)
    if len(host_hex) == 8:
        host = socket.inet_ntop(socket.AF_INET, bytes.fromhex(host_hex)[::-1])
        return host, port
    if len(host_hex) == 32:
        packed = bytes.fromhex(host_hex)
        host = socket.inet_ntop(
            socket.AF_INET6,
            b"".join(packed[index : index + 4][::-1] for index in range(0, len(packed), 4)),
        )
        return host, port
    raise ValueError(f"unsupported proc net address: {value}")


def _address_matches_bind_target(socket_host: str, bind_host: str) -> bool:
    wildcard_hosts = {"0.0.0.0", "::", "::0", "0000:0000:0000:0000:0000:0000:0000:0000"}
    normalized_bind_host = str(bind_host or "0.0.0.0")
    if normalized_bind_host in wildcard_hosts:
        return True
    return socket_host == normalized_bind_host or socket_host in wildcard_hosts


def _iter_listening_socket_inodes() -> dict[tuple[str, int], set[str]]:
    listeners: dict[tuple[str, int], set[str]] = {}
    for proc_path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = proc_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines[1:]:
            columns = line.split()
            if len(columns) < 10 or columns[3] != "0A":
                continue
            try:
                host, port = _decode_proc_net_address(columns[1])
            except ValueError:
                continue
            listeners.setdefault((host, port), set()).add(columns[9])
    return listeners


def _find_processes_listening_on_port(host: str, port: int) -> set[int]:
    matching_inodes: set[str] = set()
    for (socket_host, socket_port), inodes in _iter_listening_socket_inodes().items():
        if socket_port != port:
            continue
        if _address_matches_bind_target(socket_host, host):
            matching_inodes.update(inodes)
    if not matching_inodes:
        return set()

    pids: set[int] = set()
    proc_root = Path("/proc")
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        try:
            for fd_entry in fd_dir.iterdir():
                try:
                    target = os.readlink(fd_entry)
                except OSError:
                    continue
                if not target.startswith("socket:[") or not target.endswith("]"):
                    continue
                if target[8:-1] in matching_inodes:
                    pids.add(int(entry.name))
                    break
        except OSError:
            continue
    return pids


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _clear_port_binding(host: str, port: int, *, grace_period_sec: float = 2.0) -> None:
    pids = sorted(pid for pid in _find_processes_listening_on_port(host, port) if pid != os.getpid())
    if not pids:
        return

    for pid in pids:
        print(f"Terminating existing listener pid={pid} on {host}:{port}", flush=True)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue

    deadline = time.monotonic() + max(0.0, grace_period_sec)
    remaining = [pid for pid in pids if _pid_is_alive(pid)]
    while remaining and time.monotonic() < deadline:
        time.sleep(0.05)
        remaining = [pid for pid in remaining if _pid_is_alive(pid)]

    for pid in remaining:
        print(f"Force killing existing listener pid={pid} on {host}:{port}", flush=True)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            continue


def run_server(args: argparse.Namespace) -> None:
    runtime = PolicyRuntime(
        flow_profile_file=Path(args.flow_profile_file),
        latest_snapshot_file=Path(args.latest_snapshot_file) if args.latest_snapshot_file else None,
        state_file=Path(args.state_file) if args.state_file else None,
        upstream_dispatcher=RequestsUpstreamPcfDispatcher(
            base_url=args.upstream_pcf_base_url,
            request_timeout_sec=args.upstream_timeout_sec,
            callback_base_url=args.callback_base_url,
            request_retry_count=args.upstream_retry_count,
            retry_backoff_sec=args.upstream_retry_backoff_sec,
        ),
        default_timeout_ms=args.default_timeout_ms,
        poll_interval_ms=args.poll_interval_ms,
    )
    PolicyAcceptorHandler.runtime = runtime
    _clear_port_binding(args.host, args.port)
    server = ThreadingHTTPServer((args.host, args.port), PolicyAcceptorHandler)
    print(f"Policy execution gateway listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    run_server(args)


if __name__ == "__main__":
    main()
