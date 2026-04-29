from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from adapters.free5gc_ueransim.compose_override import render_compose_for_run
from adapters.free5gc_ueransim.subscriber_bootstrap import (
    _sanitize_payload_for_webui,
    _put_subscriber,
    build_subscriber_payload,
    render_subscriber_bootstrap_assets,
    upsert_subscriber_payloads,
)
from bridge.common.ids import generate_run_id
from bridge.common.scenario import ScenarioConfig, load_scenario
from bridge.orchestrator.config_renderer import render_run_assets
from tests.test_topology import build_semantic_graph_summary


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SubscriberBootstrapTest(unittest.TestCase):
    def test_builds_payload_from_baseline_scenario(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_single_upf.yaml")
        payload = build_subscriber_payload(scenario, scenario.ues[0], "20893")

        self.assertEqual(payload["ueId"], "imsi-208930000000001")
        self.assertEqual(payload["plmnID"], "20893")
        self.assertEqual(
            payload["AuthenticationSubscription"]["opc"]["opcValue"],
            "8e27b6af0e692e750f32667a3b14605d",
        )
        self.assertEqual(
            payload["AuthenticationSubscription"]["sequenceNumber"],
            "000000000023",
        )
        self.assertEqual(
            payload["AccessAndMobilitySubscriptionData"]["nssai"]["defaultSingleNssais"],
            [{"sst": 1, "sd": "010203"}],
        )
        self.assertEqual(
            payload["SessionManagementSubscriptionData"][0]["dnnConfigurations"]["internet"]["pduSessionTypes"]["defaultSessionType"],
            "IPV4",
        )
        self.assertEqual(
            payload["SessionManagementSubscriptionData"][0]["dnnConfigurations"]["internet"]["5gQosProfile"]["5qi"],
            9,
        )
        self.assertIn(
            "01010203",
            payload["SmfSelectionSubscriptionData"]["subscribedSnssaiInfos"],
        )

    def test_renders_payload_file_and_webui_url(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_single_upf.yaml")
        run_id = generate_run_id("testsubs")
        rendered = render_run_assets(PROJECT_ROOT, scenario, run_id)
        try:
            compose_render = render_compose_for_run(scenario, rendered.config_dir)
            assets = render_subscriber_bootstrap_assets(
                scenario,
                rendered.config_dir,
                compose_render.compose_payload,
                rendered.generated_dir / "subscribers-copy",
            )
            payload = json.loads(assets.payload_files[0].read_text(encoding="utf-8"))
            self.assertEqual(assets.serving_plmn_id, "20893")
            self.assertEqual(assets.webui_base_url, "http://127.0.0.1:5000")
            self.assertEqual(payload["ueId"], "imsi-208930000000001")
        finally:
            shutil.rmtree(rendered.run_dir, ignore_errors=True)

    def test_sanitizes_local_policy_before_webui_put(self) -> None:
        sanitized = _sanitize_payload_for_webui(
            {
                "ueId": "imsi-208930000000001",
                "plmnID": "20893",
                "LocalPolicyData": {"free5gcRanPolicy": {"targetGnb": "gnb2"}},
            }
        )

        self.assertEqual(
            sanitized,
            {
                "ueId": "imsi-208930000000001",
                "plmnID": "20893",
            },
        )

    def test_retries_connection_reset_while_upserting(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="subscriber-bootstrap-"))
        try:
            payload_path = root / "ue1-subscriber.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "ueId": "imsi-208930000000001",
                        "plmnID": "20893",
                        "SmPolicyData": {
                            "smPolicySnssaiData": {
                                "01000001": {
                                    "smPolicyDnnData": {
                                        "internet": {"dnn": "internet"},
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch(
                "adapters.free5gc_ueransim.subscriber_bootstrap._put_subscriber",
                side_effect=[ConnectionResetError("reset"), 204],
            ) as patched:
                with mock.patch(
                    "adapters.free5gc_ueransim.subscriber_bootstrap._get_subscriber",
                    return_value={
                        "SmPolicyData": {
                            "smPolicySnssaiData": {
                                "01000001": {
                                    "smPolicyDnnData": {
                                        "internet": {"dnn": "internet"},
                                    }
                                }
                            }
                        }
                    },
                ):
                    results = upsert_subscriber_payloads(
                        [payload_path],
                        base_url="http://127.0.0.1:5000",
                        timeout_seconds=2,
                        interval_seconds=0,
                    )

            self.assertEqual(patched.call_count, 2)
            self.assertEqual(results[0]["status"], 204)
            self.assertEqual(results[0]["attempts"], 2)
            self.assertTrue(results[0]["verified_sm_policy_data"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_retries_until_sm_policy_data_is_visible_in_readback(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="subscriber-bootstrap-"))
        try:
            payload_path = root / "ue1-subscriber.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "ueId": "imsi-208930000000001",
                        "plmnID": "20893",
                        "SmPolicyData": {
                            "smPolicySnssaiData": {
                                "01000001": {
                                    "smPolicyDnnData": {
                                        "internet": {"dnn": "internet"},
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch(
                "adapters.free5gc_ueransim.subscriber_bootstrap._put_subscriber",
                return_value=204,
            ) as put_patched:
                with mock.patch(
                    "adapters.free5gc_ueransim.subscriber_bootstrap._get_subscriber",
                    side_effect=[
                        {"SmPolicyData": {"smPolicySnssaiData": {}}},
                        {
                            "SmPolicyData": {
                                "smPolicySnssaiData": {
                                    "01000001": {
                                        "smPolicyDnnData": {
                                            "internet": {"dnn": "internet"},
                                        }
                                    }
                                }
                            }
                        },
                    ],
                ) as get_patched:
                    results = upsert_subscriber_payloads(
                        [payload_path],
                        base_url="http://127.0.0.1:5000",
                        timeout_seconds=2,
                        interval_seconds=0,
                    )

            self.assertEqual(put_patched.call_count, 2)
            self.assertEqual(get_patched.call_count, 2)
            self.assertEqual(results[0]["attempts"], 2)
            self.assertTrue(results[0]["verified_sm_policy_data"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_put_subscriber_ignores_host_proxy_settings(self) -> None:
        opener = mock.MagicMock()
        opener.open.return_value.__enter__.return_value.status = 204

        with mock.patch("adapters.free5gc_ueransim.subscriber_bootstrap._WEBUI_OPENER", opener):
            status = _put_subscriber(
                "http://127.0.0.1:5000",
                {"ueId": "imsi-208930000000001", "plmnID": "20893"},
            )

        self.assertEqual(status, 204)
        opener.open.assert_called_once()

    def test_renders_local_flow_and_sla_payloads_for_semantic_graph(self) -> None:
        payload = {
            "name": "semantic-graph-snapshot",
            "scenario_id": "semantic-graph-snapshot",
            "tick_ms": 1000,
            "seed": 1,
            "ues": [
                {
                    "name": "ue1",
                    "supi": "imsi-208930000000001",
                    "key": "8baf473f2f8fd09487cccbd7097c6862",
                    "op": "8e27b6af0e692e750f32667a3b14605d",
                    "op_type": "OPC",
                    "amf": "8000",
                }
            ],
            "free5gc": {
                "compose_file": "/home/xiezhengyi/workspace/free5gc-compose/docker-compose.yaml",
                "config_root": "/home/xiezhengyi/workspace/free5gc-compose/config",
            },
            "ns3": {
                "ns3_root": "/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1",
            },
            "writer": {
                "graph_db_url": "postgresql://postgres:123456@localhost:5433/multiagents_db",
            },
            "topology": {
                "graph_snapshot_id": "graph-current-1",
            },
        }
        with mock.patch(
            "bridge.common.scenario.load_graph_snapshot_payload",
            return_value=build_semantic_graph_summary(),
        ):
            scenario = ScenarioConfig.from_dict(payload)

        subscriber_payload = build_subscriber_payload(scenario, scenario.ues[0], "20893")

        self.assertEqual(len(subscriber_payload["FlowRules"]), 1)
        self.assertEqual(subscriber_payload["FlowRules"][0]["flowId"], "flow-7743")
        self.assertEqual(subscriber_payload["QosFlows"][0]["5qi"], 9)
        self.assertEqual(
            subscriber_payload["LocalPolicyData"]["flows"][0]["slaTarget"]["bandwidthDlMbps"],
            18.0,
        )
        self.assertEqual(
            _sanitize_payload_for_webui(subscriber_payload),
            {
                key: value
                for key, value in subscriber_payload.items()
                if key not in {"LocalPolicyData"}
            },
        )

    def test_binds_multi_slice_flows_to_explicit_sessions(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_slice_multi_gnb.yaml")

        ue1_payload = build_subscriber_payload(scenario, scenario.ues[0], "20893")
        ue2_payload = build_subscriber_payload(scenario, scenario.ues[1], "20893")

        self.assertEqual(
            [item["singleNssai"]["sd"] for item in ue1_payload["SessionManagementSubscriptionData"]],
            ["010203", "112233"],
        )
        self.assertEqual(
            ue1_payload["SessionManagementSubscriptionData"][1]["dnnConfigurations"]["enterprise"]["5gQosProfile"]["5qi"],
            7,
        )
        self.assertEqual(
            ue1_payload["FlowRules"],
            [
                {
                    "flowId": "ue1-video-flow",
                    "appId": "ue1-video-app",
                    "snssai": "01010203",
                    "dnn": "internet",
                    "qosRef": 1,
                    "precedence": 1,
                },
                {
                    "flowId": "ue1-control-flow",
                    "appId": "ue1-control-app",
                    "snssai": "01112233",
                    "dnn": "enterprise",
                    "qosRef": 2,
                    "precedence": 2,
                },
            ],
        )
        self.assertEqual(
            [item["sessionRef"] for item in ue1_payload["LocalPolicyData"]["flows"]],
            ["ue1-video-session", "ue1-control-session"],
        )
        self.assertEqual(ue1_payload["ChargingDatas"], [])
        self.assertEqual(ue2_payload["FlowRules"][0]["snssai"], "01112233")
        self.assertEqual(ue2_payload["FlowRules"][0]["dnn"], "enterprise")
        self.assertEqual(ue2_payload["FlowRules"][0]["precedence"], 1)
        self.assertEqual(ue2_payload["LocalPolicyData"]["flows"][0]["sessionRef"], "ue2-telemetry-session")
        self.assertEqual(ue2_payload["ChargingDatas"], [])

    def test_emits_charging_payload_only_when_explicitly_configured(self) -> None:
        scenario = load_scenario(PROJECT_ROOT / "scenarios" / "baseline_ulcl_multi_slice_multi_gnb.yaml")
        charged_flow = replace(
            scenario.flows[0],
            charging_method="ONLINE",
            quota="1GB",
            unit_cost="0.05",
        )
        charged_scenario = replace(scenario, flows=(charged_flow, *scenario.flows[1:]))

        payload = build_subscriber_payload(charged_scenario, charged_scenario.ues[0], "20893")

        self.assertEqual(
            payload["ChargingDatas"],
            [
                {
                    "flowId": "ue1-video-flow",
                    "appId": "ue1-video-app",
                    "snssai": "01010203",
                    "dnn": "internet",
                    "qosRef": 1,
                    "chargingMethod": "ONLINE",
                    "quota": "1GB",
                    "unitCost": "0.05",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
