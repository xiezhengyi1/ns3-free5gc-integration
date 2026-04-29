"""Render and apply free5GC subscriber bootstrap payloads."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import yaml

from bridge.common.scenario import AppConfig, FlowConfig, ScenarioConfig, SessionConfig, UeConfig
from bridge.common.topology import ResolvedScenarioTopology, resolve_scenario_topology


_DEFAULT_SEQUENCE_NUMBER = "000000000023"
_DEFAULT_AMBR = "1000 Kbps"
_DEFAULT_PRIORITY_LEVEL = 8
_PDU_SESSION_TYPE_MAP = {
    "ipv4": "IPV4",
    "ipv6": "IPV6",
    "ipv4v6": "IPV4V6",
    "unstructured": "UNSTRUCTURED",
    "ethernet": "ETHERNET",
}
_LOCAL_ONLY_PAYLOAD_KEYS = {"LocalPolicyData"}
_WEBUI_OPENER = request.build_opener(request.ProxyHandler({}))


@dataclass(slots=True)
class SubscriberBootstrapAssets:
    payload_files: list[Path]
    webui_base_url: str
    serving_plmn_id: str


def _yaml_load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected YAML mapping at {path}")
    return payload


def _snssai_key(sst: int, sd: str) -> str:
    return f"{sst:02d}{sd.lower()}"


def _build_msisdn(ue: UeConfig) -> str:
    digits = "".join(character for character in ue.supi if character.isdigit())
    return f"msisdn-{digits[-10:].zfill(10)}"


def _build_web_auth_subscription(ue: UeConfig) -> dict[str, object]:
    payload: dict[str, object] = {
        "authenticationMethod": "5G_AKA",
        "permanentKey": {
            "permanentKeyValue": ue.key,
            "encryptionKey": 0,
            "encryptionAlgorithm": 0,
        },
        "sequenceNumber": _DEFAULT_SEQUENCE_NUMBER,
        "authenticationManagementField": ue.amf,
    }
    op_type = ue.op_type.upper()
    if op_type == "OPC":
        payload["opc"] = {
            "opcValue": ue.op,
            "encryptionKey": 0,
            "encryptionAlgorithm": 0,
        }
        return payload
    if op_type == "OP":
        payload["milenage"] = {
            "op": {
                "opValue": ue.op,
                "encryptionKey": 0,
                "encryptionAlgorithm": 0,
            }
        }
        return payload
    raise ValueError(f"unsupported UE op_type {ue.op_type!r} for {ue.name}")


def _normalize_pdu_session_type(session_type: str) -> str:
    normalized = session_type.replace("-", "").replace("_", "").lower()
    if normalized not in _PDU_SESSION_TYPE_MAP:
        raise ValueError(f"unsupported PDU session type {session_type!r}")
    return _PDU_SESSION_TYPE_MAP[normalized]


def _build_dnn_configuration(session: SessionConfig, downlink_ambr: str, uplink_ambr: str) -> dict[str, object]:
    session_type = _normalize_pdu_session_type(session.session_type)
    return {
        "pduSessionTypes": {
            "defaultSessionType": session_type,
            "allowedSessionTypes": [session_type],
        },
        "sscModes": {
            "defaultSscMode": "SSC_MODE_1",
            "allowedSscModes": ["SSC_MODE_1"],
        },
        "sessionAmbr": {
            "downlink": downlink_ambr,
            "uplink": uplink_ambr,
        },
        "5gQosProfile": {
            "5qi": session.five_qi,
            "arp": {
                "priorityLevel": _DEFAULT_PRIORITY_LEVEL,
            },
            "priorityLevel": _DEFAULT_PRIORITY_LEVEL,
        },
    }


def _slice_snssai_payload(scenario: ScenarioConfig, slice_ref: str) -> dict[str, object]:
    slice_config = scenario.slice_map()[slice_ref]
    return {
        "sst": slice_config.sst,
        "sd": slice_config.sd.lower(),
    }


def _flows_for_ue(scenario: ScenarioConfig, ue: UeConfig) -> tuple[FlowConfig, ...]:
    return tuple(
        flow
        for flow in scenario.flows
        if flow.supi == ue.supi or flow.ue_name == ue.name
    )


def _apps_for_ue(scenario: ScenarioConfig, ue: UeConfig, flows: tuple[FlowConfig, ...]) -> tuple[AppConfig, ...]:
    apps = [app for app in scenario.apps if app.supi == ue.supi or app.ue_name == ue.name]
    if apps:
        return tuple(apps)
    flow_ids_by_app: dict[str, list[str]] = {}
    app_name_by_id: dict[str, str] = {}
    for flow in flows:
        flow_ids_by_app.setdefault(flow.app_id, []).append(flow.flow_id)
        app_name_by_id.setdefault(flow.app_id, flow.app_name or flow.app_id)
    return tuple(
        AppConfig(
            app_id=app_id,
            name=app_name_by_id[app_id],
            supi=ue.supi,
            ue_name=ue.name,
            flow_ids=tuple(flow_ids),
        )
        for app_id, flow_ids in sorted(flow_ids_by_app.items())
    )


def _sla_target_payload(flow: FlowConfig) -> dict[str, object]:
    payload = {
        "latencyMs": flow.sla_target.latency_ms,
        "jitterMs": flow.sla_target.jitter_ms,
        "lossRate": flow.sla_target.loss_rate,
        "bandwidthDlMbps": flow.sla_target.bandwidth_dl_mbps,
        "bandwidthUlMbps": flow.sla_target.bandwidth_ul_mbps,
        "guaranteedBandwidthDlMbps": flow.sla_target.guaranteed_bandwidth_dl_mbps,
        "guaranteedBandwidthUlMbps": flow.sla_target.guaranteed_bandwidth_ul_mbps,
        "priority": flow.sla_target.priority,
        "processingDelayMs": flow.sla_target.processing_delay_ms,
    }
    return {
        key: value
        for key, value in payload.items()
        if value is not None
    }


def _slice_snssai_string(scenario: ScenarioConfig, slice_ref: str) -> str:
    slice_config = scenario.slice_map()[slice_ref]
    return f"{slice_config.sst:02d}{slice_config.sd.lower()}"


def _resolve_flow_session(scenario: ScenarioConfig, ue: UeConfig, flow: FlowConfig) -> SessionConfig:
    return scenario.resolve_flow_session(ue, flow)


def _format_bandwidth_string(value_mbps: float | None) -> str | None:
    if value_mbps is None or value_mbps <= 0:
        return None
    return f"{value_mbps:g} Mbps"


def _flow_qos_ref(flow: FlowConfig, fallback_index: int) -> int:
    if flow.qos_ref is not None:
        return flow.qos_ref
    return fallback_index


def _flow_precedence(flow: FlowConfig, fallback_index: int) -> int:
    if flow.precedence is not None:
        return flow.precedence
    return fallback_index


def _build_flow_rule_payload(
    scenario: ScenarioConfig,
    ue: UeConfig,
    flow: FlowConfig,
    qos_ref: int,
    precedence: int,
) -> dict[str, object]:
    session = _resolve_flow_session(scenario, ue, flow)
    payload = {
        "flowId": flow.flow_id,
        "appId": flow.app_id,
        "snssai": _slice_snssai_string(scenario, flow.slice_ref),
        "dnn": flow.dnn or session.apn,
        "qosRef": qos_ref,
        "precedence": precedence,
    }
    if flow.policy_filter:
        payload["filter"] = flow.policy_filter
    return payload


def _build_qos_flow_payload(
    scenario: ScenarioConfig,
    ue: UeConfig,
    flow: FlowConfig,
    qos_ref: int,
) -> dict[str, object]:
    session = _resolve_flow_session(scenario, ue, flow)
    payload = {
        "flowId": flow.flow_id,
        "appId": flow.app_id,
        "snssai": _slice_snssai_string(scenario, flow.slice_ref),
        "dnn": flow.dnn or session.apn,
        "qosRef": qos_ref,
        "5qi": flow.five_qi,
        "mbrUL": _format_bandwidth_string(flow.sla_target.bandwidth_ul_mbps),
        "mbrDL": _format_bandwidth_string(flow.sla_target.bandwidth_dl_mbps),
        "gbrUL": _format_bandwidth_string(flow.sla_target.guaranteed_bandwidth_ul_mbps),
        "gbrDL": _format_bandwidth_string(flow.sla_target.guaranteed_bandwidth_dl_mbps),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _build_charging_payload(
    scenario: ScenarioConfig,
    ue: UeConfig,
    flow: FlowConfig,
    qos_ref: int,
) -> dict[str, object]:
    if all(
        value is None
        for value in (flow.charging_method, flow.quota, flow.unit_cost)
    ):
        return {}

    session = _resolve_flow_session(scenario, ue, flow)
    payload = {
        "flowId": flow.flow_id,
        "appId": flow.app_id,
        "snssai": _slice_snssai_string(scenario, flow.slice_ref),
        "dnn": flow.dnn or session.apn,
        "qosRef": qos_ref,
        "filter": flow.policy_filter,
        "chargingMethod": flow.charging_method,
        "quota": flow.quota,
        "unitCost": flow.unit_cost,
    }
    return {key: value for key, value in payload.items() if value is not None}


def build_subscriber_payload(
    scenario: ScenarioConfig,
    ue: UeConfig,
    serving_plmn_id: str,
    *,
    resolved_target_gnb: str | None = None,
) -> dict[str, object]:
    slice_map = scenario.slice_map()
    ue_flows = _flows_for_ue(scenario, ue)
    ue_apps = _apps_for_ue(scenario, ue, ue_flows)
    ordered_slice_refs: list[str] = []
    sessions_by_slice: dict[str, dict[str, SessionConfig]] = {}

    for session in ue.sessions:
        if session.slice_ref not in ordered_slice_refs:
            ordered_slice_refs.append(session.slice_ref)
        sessions_by_slice.setdefault(session.slice_ref, {})[session.apn] = session

    nssai = [
        {
            "sst": slice_map[slice_ref].sst,
            "sd": slice_map[slice_ref].sd.lower(),
        }
        for slice_ref in ordered_slice_refs
    ]

    sm_data = []
    smf_selection: dict[str, dict[str, object]] = {}
    sm_policy: dict[str, dict[str, object]] = {}
    for slice_ref in ordered_slice_refs:
        slice_config = slice_map[slice_ref]
        snssai_key = _snssai_key(slice_config.sst, slice_config.sd)
        dnn_configurations = {}
        for apn, session in sessions_by_slice[slice_ref].items():
            session_flows = [
                flow
                for flow in ue_flows
                if _resolve_flow_session(scenario, ue, flow).session_ref == session.session_ref
            ]
            downlink_mbps = sum(
                flow.sla_target.bandwidth_dl_mbps or flow.allocated_bandwidth_dl_mbps or 0.0
                for flow in session_flows
            )
            uplink_mbps = sum(
                flow.sla_target.bandwidth_ul_mbps or flow.allocated_bandwidth_ul_mbps or 0.0
                for flow in session_flows
            )
            if slice_config.resource is not None:
                downlink_mbps = min(downlink_mbps, slice_config.resource.capacity_dl_mbps)
                uplink_mbps = min(uplink_mbps, slice_config.resource.capacity_ul_mbps)
            dnn_configurations[apn] = _build_dnn_configuration(
                session,
                _format_bandwidth_string(downlink_mbps) or _DEFAULT_AMBR,
                _format_bandwidth_string(uplink_mbps) or _DEFAULT_AMBR,
            )
        sm_data.append(
            {
                "singleNssai": {
                    "sst": slice_config.sst,
                    "sd": slice_config.sd.lower(),
                },
                "dnnConfigurations": dnn_configurations,
            }
        )
        smf_selection[snssai_key] = {
            "dnnInfos": [{"dnn": apn} for apn in dnn_configurations],
        }
        sm_policy[snssai_key] = {
            "snssai": {
                "sst": slice_config.sst,
                "sd": slice_config.sd.lower(),
            },
            "smPolicyDnnData": {
                apn: {"dnn": apn} for apn in dnn_configurations
            },
        }

    flow_rules = []
    qos_flows = []
    charging_datas = []
    for index, flow in enumerate(ue_flows, start=1):
        qos_ref = _flow_qos_ref(flow, index)
        precedence = _flow_precedence(flow, index)
        flow_rules.append(_build_flow_rule_payload(scenario, ue, flow, qos_ref, precedence))
        qos_flows.append(_build_qos_flow_payload(scenario, ue, flow, qos_ref))
        charging_datas.append(_build_charging_payload(scenario, ue, flow, qos_ref))

    payload = {
        "plmnID": serving_plmn_id,
        "ueId": ue.supi,
        "AuthenticationSubscription": _build_web_auth_subscription(ue),
        "AccessAndMobilitySubscriptionData": {
            "gpsis": [_build_msisdn(ue)],
            "nssai": {
                "defaultSingleNssais": nssai,
                "singleNssais": nssai,
            },
            "subscribedUeAmbr": {
                "downlink": _DEFAULT_AMBR,
                "uplink": _DEFAULT_AMBR,
            },
        },
        "SessionManagementSubscriptionData": sm_data,
        "SmfSelectionSubscriptionData": {
            "subscribedSnssaiInfos": smf_selection,
        },
        "AmPolicyData": {
            "subscCats": ["free5gc"],
        },
        "SmPolicyData": {
            "smPolicySnssaiData": sm_policy,
        },
        "FlowRules": flow_rules,
        "QosFlows": qos_flows,
        "ChargingDatas": [item for item in charging_datas if item],
    }
    local_policy_data: dict[str, object] = {}
    if ue.free5gc_policy.target_gnb is not None or ue.free5gc_policy.preferred_gnbs:
        local_policy_data["free5gcRanPolicy"] = {
            "targetGnb": ue.free5gc_policy.target_gnb,
            "preferredGnbs": list(ue.free5gc_policy.preferred_gnbs),
            "resolvedTargetGnb": resolved_target_gnb,
        }
    if ue_apps or ue_flows:
        local_policy_data["applications"] = [
            {
                "appId": app.app_id,
                "name": app.name,
                "flowIds": list(app.flow_ids),
            }
            for app in ue_apps
        ]
        local_policy_data["flows"] = [
            {
                "flowId": flow.flow_id,
                "name": flow.name,
                "appId": flow.app_id,
                "appName": flow.app_name,
                "sliceRef": flow.slice_ref,
                "sessionRef": _resolve_flow_session(scenario, ue, flow).session_ref,
                "5qi": flow.five_qi,
                "serviceType": flow.service_type,
                "packetSizeBytes": flow.packet_size_bytes,
                "arrivalRatePps": flow.arrival_rate_pps,
                "slaTarget": _sla_target_payload(flow),
            }
            for flow in ue_flows
        ]
    if local_policy_data:
        payload["LocalPolicyData"] = local_policy_data
    return payload


def _sanitize_payload_for_webui(payload: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in payload.items()
        if key not in _LOCAL_ONLY_PAYLOAD_KEYS
    }


def _resolve_serving_plmn_id(generated_config_dir: Path, scenario: ScenarioConfig) -> str:
    if not scenario.gnbs:
        raise ValueError("scenario must define at least one gNB")
    gnb_payload = _yaml_load(generated_config_dir / f"{scenario.gnbs[0].name}-gnbcfg.yaml")
    mcc = str(gnb_payload["mcc"])
    mnc = str(gnb_payload["mnc"])
    return f"{mcc}{mnc}"


def _resolve_webui_base_url(
    scenario: ScenarioConfig,
    compose_payload: dict[str, Any],
) -> str:
    webui_config = _yaml_load(Path(scenario.free5gc.config_root) / "webuicfg.yaml")
    configuration = webui_config.get("configuration", {})
    web_server = configuration.get("webServer", {})
    scheme = str(web_server.get("scheme", "http"))
    container_port = str(web_server.get("port", 5000))

    service = compose_payload.get("services", {}).get("free5gc-webui", {})
    for item in service.get("ports", []):
        if not isinstance(item, str):
            continue
        fragments = item.split(":")
        if len(fragments) < 2:
            continue
        published = fragments[-2]
        target = fragments[-1].split("/", 1)[0]
        if target == container_port:
            return f"{scheme}://127.0.0.1:{published}"

    return f"{scheme}://127.0.0.1:{container_port}"


def render_subscriber_bootstrap_assets(
    scenario: ScenarioConfig,
    generated_config_dir: Path,
    compose_payload: dict[str, Any],
    output_dir: Path,
    resolved_topology: ResolvedScenarioTopology | None = None,
) -> SubscriberBootstrapAssets:
    resolved_topology = resolved_topology or resolve_scenario_topology(scenario)
    output_dir.mkdir(parents=True, exist_ok=True)
    serving_plmn_id = _resolve_serving_plmn_id(generated_config_dir, scenario)
    payload_files: list[Path] = []
    for ue in scenario.ues:
        payload = build_subscriber_payload(
            scenario,
            ue,
            serving_plmn_id,
            resolved_target_gnb=resolved_topology.ue_to_gnb.get(ue.name),
        )
        payload_path = output_dir / f"{ue.name}-subscriber.json"
        payload_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        payload_files.append(payload_path)

    return SubscriberBootstrapAssets(
        payload_files=payload_files,
        webui_base_url=_resolve_webui_base_url(scenario, compose_payload),
        serving_plmn_id=serving_plmn_id,
    )


def _read_payload(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"subscriber payload must be a JSON object: {path}")
    return payload


def _subscriber_endpoint(base_url: str, ue_id: str, plmn_id: str) -> str:
    return (
        f"{base_url.rstrip('/')}/api/subscriber/"
        f"{parse.quote(ue_id, safe='')}/{parse.quote(plmn_id, safe='')}"
    )


def _put_subscriber(base_url: str, payload: dict[str, object]) -> int:
    sanitized_payload = _sanitize_payload_for_webui(payload)
    ue_id = str(sanitized_payload["ueId"])
    plmn_id = str(sanitized_payload["plmnID"])
    target_url = _subscriber_endpoint(base_url, ue_id, plmn_id)
    request_body = json.dumps(sanitized_payload, ensure_ascii=False).encode("utf-8")
    http_request = request.Request(
        target_url,
        data=request_body,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    # Ignore host HTTP(S) proxy settings; WebUI is expected to be local to the run.
    with _WEBUI_OPENER.open(http_request, timeout=10) as response:
        return response.status


def _get_subscriber(base_url: str, payload: dict[str, object]) -> dict[str, object]:
    sanitized_payload = _sanitize_payload_for_webui(payload)
    target_url = _subscriber_endpoint(
        base_url,
        str(sanitized_payload["ueId"]),
        str(sanitized_payload["plmnID"]),
    )
    http_request = request.Request(
        target_url,
        headers={"Accept": "application/json"},
        method="GET",
    )
    with _WEBUI_OPENER.open(http_request, timeout=10) as response:
        response_payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(response_payload, dict):
        raise ValueError(f"subscriber readback returned non-object payload: {target_url}")
    return response_payload


def _verify_sm_policy_readback(payload: dict[str, object], subscriber_payload: dict[str, object]) -> None:
    expected = payload.get("SmPolicyData")
    if not isinstance(expected, dict):
        return
    expected_snssai = expected.get("smPolicySnssaiData")
    if not isinstance(expected_snssai, dict) or not expected_snssai:
        return

    observed = subscriber_payload.get("SmPolicyData")
    if not isinstance(observed, dict):
        raise ValueError("subscriber readback is missing SmPolicyData")
    observed_snssai = observed.get("smPolicySnssaiData")
    if not isinstance(observed_snssai, dict):
        raise ValueError("subscriber readback is missing SmPolicyData.smPolicySnssaiData")

    # Confirm every expected SNSSAI->DNN entry is visible through WebUI readback
    # before the run continues. This prevents races where the PUT returns first
    # but PCF/UDR still cannot read the subscriber's SM policy data.
    missing_entries: list[str] = []
    for snssai_key, snssai_payload in expected_snssai.items():
        if not isinstance(snssai_payload, dict):
            continue
        expected_dnn_data = snssai_payload.get("smPolicyDnnData")
        if not isinstance(expected_dnn_data, dict) or not expected_dnn_data:
            continue
        observed_snssai_payload = observed_snssai.get(snssai_key)
        if not isinstance(observed_snssai_payload, dict):
            missing_entries.extend(f"{snssai_key}:{dnn}" for dnn in expected_dnn_data)
            continue
        observed_dnn_data = observed_snssai_payload.get("smPolicyDnnData")
        if not isinstance(observed_dnn_data, dict):
            missing_entries.extend(f"{snssai_key}:{dnn}" for dnn in expected_dnn_data)
            continue
        for dnn_name in expected_dnn_data:
            if dnn_name not in observed_dnn_data:
                missing_entries.append(f"{snssai_key}:{dnn_name}")

    if missing_entries:
        raise ValueError(
            "subscriber readback is missing expected SmPolicyData entries: "
            + ", ".join(missing_entries)
        )


def upsert_subscriber_payloads(
    payload_files: list[Path],
    *,
    base_url: str,
    timeout_seconds: float = 120.0,
    interval_seconds: float = 2.0,
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout_seconds
    results: list[dict[str, object]] = []
    for payload_path in payload_files:
        payload = _read_payload(payload_path)
        last_error: Exception | None = None
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                status = _put_subscriber(base_url, payload)
            except error.HTTPError as exc:
                if exc.code >= 500:
                    last_error = exc
                    time.sleep(interval_seconds)
                    continue
                raise
            except (error.URLError, ConnectionResetError, TimeoutError, OSError) as exc:
                last_error = exc
                time.sleep(interval_seconds)
                continue

            try:
                readback_payload = _get_subscriber(base_url, payload)
                _verify_sm_policy_readback(payload, readback_payload)
            except (error.HTTPError, error.URLError, ConnectionResetError, TimeoutError, OSError, ValueError) as exc:
                last_error = exc
                time.sleep(interval_seconds)
                continue

            result = {
                "payload": str(payload_path),
                "ue_id": payload["ueId"],
                "plmn_id": payload["plmnID"],
                "status": status,
                "attempts": attempts,
                "base_url": base_url,
                "verified_sm_policy_data": True,
            }
            results.append(result)
            break
        else:
            raise TimeoutError(
                f"timed out while upserting/verifying subscriber {payload.get('ueId')} via {base_url}: {last_error}"
            )
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upsert free5GC subscriber payloads")
    parser.add_argument("payload", nargs="+", help="subscriber payload JSON files")
    parser.add_argument("--base-url", required=True, help="free5GC WebUI base URL")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
        help="max time to wait for WebUI readiness",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=2.0,
        help="retry interval while WebUI is unavailable",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    results = upsert_subscriber_payloads(
        [Path(item).expanduser().resolve() for item in args.payload],
        base_url=args.base_url,
        timeout_seconds=args.timeout_seconds,
        interval_seconds=args.interval_seconds,
    )
    for result in results:
        print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
