from __future__ import annotations

import unittest

from bridge.common.graph_adapter import merge_semantic_graph_payload
from bridge.common.schema import TickSnapshot
from bridge.writer.graph_mapper import build_graph_snapshot_bundle
from tests.test_schema import build_payload


class GraphAdapterTest(unittest.TestCase):
    def test_round_trips_multi_slice_session_and_dnn_from_graph_summary(self) -> None:
        payload = build_payload()
        payload["flows"][0]["session_ref"] = "ue1-video-session"
        payload["flows"][0]["service"] = {"dnn": "internet"}
        payload["flows"][0]["traffic"] = {"filter": "permit out ip from 10.0.0.1 to any"}
        payload["flows"][0]["allocation"] = {"qos_ref": 1}
        payload["flows"].append(
            {
                "flow_id": "flow-2",
                "name": "flow-2",
                "supi": "imsi-208930000000001",
                "app_id": "app-2",
                "app_name": "control",
                "src_gnb": "gnb-1",
                "dst_upf": "upf",
                "slice_id": "slice-1-112233",
                "session_ref": "ue1-control-session",
                "5qi": 7,
                "delay_ms": 2.0,
                "jitter_ms": 0.4,
                "loss_rate": 0.0,
                "throughput_ul_mbps": 1.5,
                "throughput_dl_mbps": 4.0,
                "queue_bytes": 0,
                "rlc_buffer_bytes": 0,
                "service": {"dnn": "enterprise", "service_type": "URLLC"},
                "traffic": {"filter": "permit out ip from 10.0.0.2 to any"},
                "allocation": {"qos_ref": 2},
            }
        )
        payload["slices"].append(
            {
                "slice_id": "slice-1-112233",
                "sst": 1,
                "sd": "112233",
                "label": "urllc",
            }
        )

        snapshot = TickSnapshot.from_dict(payload)
        bundle = build_graph_snapshot_bundle(snapshot)

        merged = merge_semantic_graph_payload(
            {
                "ues": [
                    {
                        "name": "ue1",
                        "supi": "imsi-208930000000001",
                        "key": "8baf473f2f8fd09487cccbd7097c6862",
                        "op": "8e27b6af0e692e750f32667a3b14605d",
                        "op_type": "OPC",
                        "amf": "8000",
                    }
                ]
            },
            bundle.snapshot_row["graph_summary"],
        )

        ue_payload = next(item for item in merged["ues"] if item["name"] == "ue1")
        flow_payloads = {item["flow_id"]: item for item in merged["flows"]}

        self.assertEqual(
            sorted(item["session_ref"] for item in ue_payload["sessions"]),
            ["ue1-control-session", "ue1-video-session"],
        )
        self.assertEqual(
            sorted(item["apn"] for item in ue_payload["sessions"]),
            ["enterprise", "internet"],
        )
        self.assertEqual(flow_payloads["flow-1"]["session_ref"], "ue1-video-session")
        self.assertEqual(flow_payloads["flow-2"]["session_ref"], "ue1-control-session")
        self.assertEqual(flow_payloads["flow-2"]["dnn"], "enterprise")
        self.assertEqual(flow_payloads["flow-2"]["policy_filter"], "permit out ip from 10.0.0.2 to any")


if __name__ == "__main__":
    unittest.main()