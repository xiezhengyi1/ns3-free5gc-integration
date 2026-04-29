from __future__ import annotations

import csv
import json
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path

import requests

from bridge.policy_acceptor import PolicyError, PolicyRuntime, RequestsUpstreamPcfDispatcher, _clear_port_binding


FIELDNAMES = [
    "flow_id",
    "flow_name",
    "ue_name",
    "supi",
    "app_id",
    "app_name",
    "session_ref",
    "slice_ref",
    "slice_snssai",
    "dnn",
    "service_type",
    "service_type_id",
    "five_qi",
    "packet_size_bytes",
    "arrival_rate_pps",
    "latency_ms",
    "jitter_ms",
    "loss_rate",
    "bandwidth_dl_mbps",
    "bandwidth_ul_mbps",
    "guaranteed_bandwidth_dl_mbps",
    "guaranteed_bandwidth_ul_mbps",
    "priority",
    "allocated_bandwidth_dl_mbps",
    "allocated_bandwidth_ul_mbps",
    "optimize_requested",
    "policy_filter",
    "precedence",
    "qos_ref",
    "charging_method",
    "quota",
    "unit_cost",
]


def _write_profiles(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _base_row(*, flow_id: str, supi: str, slice_ref: str, slice_snssai: str) -> dict[str, str]:
    return {
        "flow_id": flow_id,
        "flow_name": flow_id,
        "ue_name": "ue-1",
        "supi": supi,
        "app_id": "app-1",
        "app_name": "app-1",
        "session_ref": f"{supi}:app-1:{slice_ref}:internet",
        "slice_ref": slice_ref,
        "slice_snssai": slice_snssai,
        "dnn": "internet",
        "service_type": "eMBB",
        "service_type_id": "1",
        "five_qi": "9",
        "packet_size_bytes": "1200",
        "arrival_rate_pps": "100",
        "latency_ms": "40",
        "jitter_ms": "5",
        "loss_rate": "0.01",
        "bandwidth_dl_mbps": "15",
        "bandwidth_ul_mbps": "10",
        "guaranteed_bandwidth_dl_mbps": "8",
        "guaranteed_bandwidth_ul_mbps": "4",
        "priority": "5",
        "allocated_bandwidth_dl_mbps": "15",
        "allocated_bandwidth_ul_mbps": "10",
        "optimize_requested": "False",
        "policy_filter": "",
        "precedence": "5",
        "qos_ref": "qos-old",
        "charging_method": "flat",
        "quota": "100",
        "unit_cost": "1.0",
    }


class StubDispatcher:
    def __init__(self, result: dict[str, object] | None = None, error: Exception | None = None) -> None:
        self.result = result or {
            "status": "success",
            "endpoint": "http://pcf.example/npcf",
            "response_code": 201,
            "response_body": {"upstream_policy_id": "upstream-1"},
        }
        self.error = error
        self.calls: list[dict[str, object]] = []

    def dispatch(self, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(payload))
        if self.error is not None:
            raise self.error
        return dict(self.result)

    def healthcheck(self) -> tuple[bool, str]:
        return True, "stub-ok"


class PolicyGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.flow_profile_file = self.root / "ns3-flow-profiles.tsv"
        self.latest_snapshot_file = self.root / "latest.json"
        self.state_file = self.root / "policy-state.json"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _build_runtime(self, dispatcher: StubDispatcher) -> PolicyRuntime:
        return PolicyRuntime(
            self.flow_profile_file,
            self.latest_snapshot_file,
            self.state_file,
            upstream_dispatcher=dispatcher,
            default_timeout_ms=300,
            poll_interval_ms=20,
        )

    def _write_snapshot(self, payload: dict[str, object]) -> None:
        self.latest_snapshot_file.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    def _write_snapshot_later(self, payload: dict[str, object], delay_sec: float = 0.05) -> None:
        timer = threading.Timer(delay_sec, self._write_snapshot, args=(payload,))
        timer.daemon = True
        timer.start()

    def test_sm_policy_dispatches_and_waits_for_ns3_success(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        self._write_snapshot({"run_id": "run-1", "tick_index": 3, "flows": [], "ues": [], "slices": []})
        dispatcher = StubDispatcher()
        runtime = self._build_runtime(dispatcher)

        self._write_snapshot_later(
            {
                "run_id": "run-1",
                "tick_index": 4,
                "flows": [
                    {
                        "flow_id": "flow-1",
                        "allocation": {
                            "allocated_bandwidth_dl": 32,
                            "allocated_bandwidth_ul": 14,
                            "current_slice_snssai": "01000001",
                        },
                        "telemetry": {"latency": 12, "jitter": 2, "loss_rate": 0.01},
                    }
                ],
                "ues": [],
                "slices": [],
            }
        )

        response = runtime.execute_policy(
            {
                "request_id": "req-1",
                "session_id": "session-1",
                "snapshot_id": "snapshot-1",
                "policy_id": "smp-app-1-flow-1",
                "policy_type": "SmPolicyDecision",
                "policy_details": {
                    "policy_id": "smp-app-1-flow-1",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "pccRules": {
                        "pcc-flow-1": {
                            "pccRuleId": "pcc-flow-1",
                            "precedence": 2,
                            "flowInfos": [{"flowDescription": "video-priority"}],
                        }
                    },
                    "qosDecs": {
                        "qos-flow-1": {
                            "qosId": "qos-flow-1",
                            "priorityLevel": 2,
                            "packetDelayBudget": 18,
                            "packetErrorRate": "0.03",
                            "jitterReq": 4,
                            "maxbrDl": "32",
                            "maxbrUl": "14",
                            "gbrDl": "20",
                            "gbrUl": "8",
                        }
                    },
                },
            }
        )

        self.assertEqual(response["status"], "success")
        self.assertEqual(response["execution_status"], "APPLIED")
        self.assertEqual(response["compliance_status"], "COMPLIANT")
        self.assertEqual(response["baseline_tick"], 3)
        self.assertEqual(response["applied_tick"], 4)
        self.assertEqual(len(dispatcher.calls), 1)

        with self.flow_profile_file.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(rows[0]["allocated_bandwidth_dl_mbps"], "32.0")
        self.assertEqual(rows[0]["policy_filter"], "video-priority")
        self.assertEqual(rows[0]["qos_ref"], "0")

        status = runtime.get_execution("smp-app-1-flow-1")
        self.assertEqual(status["status"], "success")
        self.assertEqual(status["monitoring_data"]["latest_tick"], 4)

    def test_clear_port_binding_terminates_existing_listener(self) -> None:
        signals: list[tuple[int, int]] = []

        with (
            mock.patch("bridge.policy_acceptor._find_processes_listening_on_port", return_value={1001}),
            mock.patch("bridge.policy_acceptor._pid_is_alive", return_value=True),
            mock.patch("bridge.policy_acceptor.os.getpid", return_value=9999),
            mock.patch("bridge.policy_acceptor.os.kill", side_effect=lambda pid, sig: signals.append((pid, sig))),
        ):
            _clear_port_binding("0.0.0.0", 18080, grace_period_sec=0.0)

        self.assertEqual(signals, [(1001, 15), (1001, 9)])

    def test_am_policy_dispatches_and_waits_for_ns3_success(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [
                _base_row(flow_id="flow-1", supi="imsi-208930000000009", slice_ref="slice-1-000001", slice_snssai="01000001"),
                _base_row(flow_id="flow-2", supi="imsi-208930000000009", slice_ref="slice-1-000001", slice_snssai="01000001"),
                _base_row(flow_id="flow-3", supi="imsi-208930000000010", slice_ref="slice-2-000001", slice_snssai="02000001"),
            ],
        )
        self._write_snapshot({"run_id": "run-2", "tick_index": 9, "flows": [], "ues": [], "slices": []})
        runtime = self._build_runtime(StubDispatcher())
        self._write_snapshot_later(
            {
                "run_id": "run-2",
                "tick_index": 10,
                "flows": [],
                "ues": [{"ue_id": "ue-9", "supi": "imsi-208930000000009", "slice_id": "slice-2-000001"}],
                "slices": [{"slice_id": "slice-2-000001", "sst": 2, "sd": "000001"}],
            }
        )

        response = runtime.execute_policy(
            {
                "request_id": "req-2",
                "session_id": "session-2",
                "snapshot_id": "snapshot-2",
                "policy_id": "amp-imsi-208930000000009",
                "policy_type": "PcfAmPolicyControlPolicyAssociation",
                "policy_details": {
                    "policy_id": "amp-imsi-208930000000009",
                    "request": {
                        "supi": "imsi-208930000000009",
                        "allowedSnssais": [{"sst": 2, "sd": "000001"}],
                    },
                },
            }
        )

        self.assertEqual(response["status"], "success")
        self.assertEqual(response["execution_status"], "APPLIED")
        self.assertEqual(response["compliance_status"], "COMPLIANT")
        self.assertEqual(response["monitoring_data"]["observed_supi"], "imsi-208930000000009")

    def test_ursp_policy_is_rejected(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        runtime = self._build_runtime(StubDispatcher())

        response = runtime.execute_policy(
            {
                "request_id": "req-3",
                "session_id": "session-3",
                "snapshot_id": "snapshot-3",
                "policy_id": "ursp-flow-1",
                "policy_type": "UrspRuleRequest",
                "policy_details": {
                    "policy_id": "ursp-flow-1",
                    "flow_id": "flow-1",
                    "routeSelParamSets": [{"snssai": {"sst": 1, "sd": "000001"}}],
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "unsupported_policy_type")
        self.assertIn("not supported", response["error"])

    def test_upstream_failure_returns_failed_without_ns3_success(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        dispatcher = StubDispatcher(error=RuntimeError("pcf boom"))
        runtime = self._build_runtime(dispatcher)

        response = runtime.execute_policy(
            {
                "request_id": "req-4",
                "session_id": "session-4",
                "snapshot_id": "snapshot-4",
                "policy_id": "smp-upstream-fail",
                "policy_type": "SmPolicyDecision",
                "policy_details": {
                    "policy_id": "smp-upstream-fail",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "upstream_pcf")
        self.assertEqual(response["execution_status"], "PENDING")

    def test_sm_policy_does_not_backfill_upstream_context_in_runtime(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        dispatcher = StubDispatcher(error=RuntimeError("pcf boom"))
        runtime = self._build_runtime(dispatcher)

        response = runtime.execute_policy(
            {
                "request_id": "req-4b",
                "session_id": "session-4b",
                "snapshot_id": "snapshot-4b",
                "policy_id": "smp-upstream-context-backfill",
                "policy_type": "SmPolicyDecision",
                "policy_details": {
                    "policy_id": "smp-upstream-context-backfill",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "upstreamSmPolicyContextData": {
                        "supi": "imsi-208930000000001",
                        "dnn": "internet",
                        "sliceInfo": {"sst": 1, "sd": "000001"},
                        "servingNetwork": {"mcc": "208", "mnc": "93"},
                        "accessType": "3GPP_ACCESS",
                        "ratType": "NR",
                        "subsDefQos": {"5qi": 9, "priorityLevel": 5},
                    },
                    "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "upstream_pcf")
        self.assertEqual(len(dispatcher.calls), 1)

        upstream_context = dispatcher.calls[0]["policy_details"]["upstreamSmPolicyContextData"]
        self.assertEqual(upstream_context["subsDefQos"]["5qi"], 9)
        self.assertEqual(upstream_context["subsDefQos"]["priorityLevel"], 5)
        self.assertEqual(upstream_context["sliceInfo"], {"sst": 1, "sd": "000001"})
        self.assertEqual(upstream_context["servingNetwork"], {"mcc": "208", "mnc": "93"})
        self.assertNotIn("pduSessionId", upstream_context)
        self.assertNotIn("pduSessionType", upstream_context)
        self.assertNotIn("subsSessAmbr", upstream_context)

    def test_requests_dispatcher_requires_explicit_upstream_sm_context(self) -> None:
        dispatcher = RequestsUpstreamPcfDispatcher(base_url="http://pcf.example")

        with self.assertRaises(PolicyError) as raised:
            dispatcher._build_request(
                "SmPolicyDecision",
                {
                    "policy_id": "smp-missing-upstream-context",
                    "policy_type": "SmPolicyDecision",
                    "policy_details": {
                        "policy_id": "smp-missing-upstream-context",
                        "flow_id": "flow-1",
                        "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                    },
                },
            )

        self.assertIn("upstreamSmPolicyContextData", str(raised.exception))

    def test_timeout_returns_failed(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        self._write_snapshot({"run_id": "run-5", "tick_index": 1, "flows": [], "ues": [], "slices": []})
        runtime = self._build_runtime(StubDispatcher())

        response = runtime.execute_policy(
            {
                "request_id": "req-5",
                "session_id": "session-5",
                "snapshot_id": "snapshot-5",
                "policy_id": "smp-timeout",
                "policy_type": "SmPolicyDecision",
                "timeout_ms": 120,
                "policy_details": {
                    "policy_id": "smp-timeout",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "ns3_apply_timeout")
        self.assertIn("did not apply", response["error"])

    def test_failed_snapshot_observation_returns_failed(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        self._write_snapshot({"run_id": "run-6", "tick_index": 7, "flows": [], "ues": [], "slices": []})
        runtime = self._build_runtime(StubDispatcher())
        self._write_snapshot_later(
            {
                "run_id": "run-6",
                "tick_index": 8,
                "flows": [
                    {
                        "flow_id": "flow-1",
                        "allocation": {
                            "allocated_bandwidth_dl": 10,
                            "allocated_bandwidth_ul": 5,
                            "current_slice_snssai": "01000001",
                        },
                        "telemetry": {"latency": 50, "jitter": 10, "loss_rate": 0.1},
                    }
                ],
                "ues": [],
                "slices": [],
            }
        )

        response = runtime.execute_policy(
            {
                "request_id": "req-6",
                "session_id": "session-6",
                "snapshot_id": "snapshot-6",
                "policy_id": "smp-mismatch",
                "policy_type": "SmPolicyDecision",
                "timeout_ms": 150,
                "policy_details": {
                    "policy_id": "smp-mismatch",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "ns3_apply")
        self.assertEqual(response["execution_status"], "FAILED")
        self.assertEqual(response["compliance_status"], "VIOLATED")
        self.assertIn("did not converge", response["error"])

    def test_launch_healthcheck_reports_upstream_state(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        self._write_snapshot({"run_id": "run-hc", "tick_index": 1, "flows": [], "ues": [], "slices": []})
        runtime = self._build_runtime(StubDispatcher())

        response = runtime.launch_healthcheck()

        self.assertTrue(response["healthy"])
        self.assertTrue(response["flow_profile_exists"])
        self.assertTrue(response["latest_snapshot_exists"])
        self.assertTrue(response["upstream_ok"])

    @mock.patch("requests.Session.post")
    def test_requests_dispatcher_retries_transient_transport_error(self, mock_post: mock.Mock) -> None:
        ok_response = mock.Mock()
        ok_response.ok = True
        ok_response.status_code = 201
        ok_response.reason = "Created"
        ok_response.json.return_value = {"policyId": "ok"}
        mock_post.side_effect = [
            requests.exceptions.ConnectionError("connection reset"),
            ok_response,
        ]

        dispatcher = RequestsUpstreamPcfDispatcher(
            base_url="http://pcf.example",
            request_retry_count=2,
            retry_backoff_sec=0.0,
        )

        result = dispatcher.dispatch(
            {
                "policy_id": "am-1",
                "policy_type": "PcfAmPolicyControlPolicyAssociation",
                "policy_details": {
                    "request": {
                        "supi": "imsi-208930000000001",
                        "notificationUri": "http://callback.example/notify",
                        "accessType": "3GPP_ACCESS",
                        "servingPlmn": {"mcc": "208", "mnc": "93"},
                        "guami": {"plmnId": {"mcc": "208", "mnc": "93"}, "amfId": "000001"},
                        "userLoc": {"nrLocation": {"tai": {"plmnId": {"mcc": "208", "mnc": "93"}, "tac": "000001"}}},
                    }
                },
            }
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(mock_post.call_count, 2)

    def test_invalid_snapshot_file_returns_failed(self) -> None:
        _write_profiles(
            self.flow_profile_file,
            [_base_row(flow_id="flow-1", supi="imsi-208930000000001", slice_ref="slice-1-000001", slice_snssai="01000001")],
        )
        self.latest_snapshot_file.write_text("{not-json}\n", encoding="utf-8")
        runtime = self._build_runtime(StubDispatcher())

        response = runtime.execute_policy(
            {
                "request_id": "req-7",
                "session_id": "session-7",
                "snapshot_id": "snapshot-7",
                "policy_id": "smp-bad-snapshot",
                "policy_type": "SmPolicyDecision",
                "policy_details": {
                    "policy_id": "smp-bad-snapshot",
                    "target_type": "flow",
                    "flow_id": "flow-1",
                    "qosDecs": {"qos-flow-1": {"qosId": "qos-flow-1", "maxbrDl": "20", "maxbrUl": "10"}},
                },
            }
        )

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["phase"], "snapshot")
        self.assertIn("not valid JSON", response["error"])

    def test_query_unknown_policy_returns_404(self) -> None:
        runtime = self._build_runtime(StubDispatcher())
        response = runtime.get_execution("missing")
        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["status_code"], 404)

