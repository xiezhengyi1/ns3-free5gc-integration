from __future__ import annotations

import unittest

from bridge.common.schema import TickSnapshot
from bridge.writer.graph_mapper import (
    GRAPH_ROW_DELETED,
    build_delta_graph_snapshot_bundle,
    build_graph_snapshot_bundle,
)
from tests.test_schema import build_payload


class GraphMapperTest(unittest.TestCase):
    def test_graph_bundle_contains_flow_slice_and_app_nodes(self) -> None:
        payload = build_payload()
        payload["flows"][0]["traffic"] = {
            "five_tuple": {
                "protocol": 17,
                "source_ip": "1.0.0.2",
                "source_port": 49153,
                "destination_ip": "10.60.0.1",
                "destination_port": 5000,
            }
        }
        snapshot = TickSnapshot.from_dict(payload)
        bundle = build_graph_snapshot_bundle(snapshot, trigger_event="sim_tick:test:0")

        node_keys = {row["node_key"] for row in bundle.node_rows}
        edge_keys = {row["edge_key"] for row in bundle.edge_rows}
        metric_keys = {(row["owner_key"], row["metric_name"]) for row in bundle.metric_rows}
        summary_nodes = {
            row["node_key"]: row
            for row in bundle.snapshot_row["graph_summary"]["nodes"]
        }

        self.assertIn("flow:imsi-208930000000001:app-1:flow-1", node_keys)
        self.assertIn("slice:01010203", node_keys)
        self.assertIn("app:imsi-208930000000001:app-1", node_keys)
        self.assertIn(
            "flow:imsi-208930000000001:app-1:flow-1->slice:01010203",
            edge_keys,
        )
        self.assertIn(
            ("flow:imsi-208930000000001:app-1:flow-1", "sla.bandwidth_dl"),
            metric_keys,
        )
        self.assertIn(
            ("flow:imsi-208930000000001:app-1:flow-1", "telemetry.throughput_dl"),
            metric_keys,
        )
        self.assertEqual(
            summary_nodes["flow:imsi-208930000000001:app-1:flow-1"]["properties"]["sla"]["bandwidth_dl"],
            10.5,
        )
        self.assertEqual(
            summary_nodes["flow:imsi-208930000000001:app-1:flow-1"]["properties"]["telemetry"]["throughput_dl"],
            10.5,
        )
        self.assertEqual(
            summary_nodes["flow:imsi-208930000000001:app-1:flow-1"]["properties"]["traffic"]["direction"],
            "downlink",
        )
        self.assertIn("nodes", bundle.snapshot_row["graph_summary"])
        self.assertIn("edges", bundle.snapshot_row["graph_summary"])
        self.assertIn("metrics", bundle.snapshot_row["graph_summary"])

    def test_graph_bundle_preserves_uplink_real_flow_direction(self) -> None:
        payload = build_payload()
        payload["flows"][0]["traffic"] = {
            "five_tuple": {
                "protocol": 17,
                "source_ip": "ue_pdu_ip",
                "source_port": 15000,
                "destination_ip": "8.8.8.8",
                "destination_port": 5000,
            },
            "direction": "uplink",
            "source_entity": "ue_pdu_ip",
            "destination_entity": "external_data_network",
        }
        bundle = build_graph_snapshot_bundle(TickSnapshot.from_dict(payload))
        summary_nodes = {
            row["node_key"]: row
            for row in bundle.snapshot_row["graph_summary"]["nodes"]
        }

        traffic = summary_nodes["flow:imsi-208930000000001:app-1:flow-1"]["properties"]["traffic"]
        self.assertEqual(traffic["direction"], "uplink")
        self.assertEqual(traffic["source_entity"], "ue_pdu_ip")
        self.assertEqual(traffic["destination_entity"], "external_data_network")

    def test_graph_bundle_contains_explicit_session_nodes_for_multi_slice_flows(self) -> None:
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
        bundle = build_graph_snapshot_bundle(snapshot, trigger_event="sim_tick:test:0")

        node_keys = {row["node_key"] for row in bundle.node_rows}
        edge_keys = {row["edge_key"] for row in bundle.edge_rows}
        summary_nodes = {
            row["node_key"]: row
            for row in bundle.snapshot_row["graph_summary"]["nodes"]
        }

        self.assertIn("session:imsi-208930000000001:ue1-video-session", node_keys)
        self.assertIn("session:imsi-208930000000001:ue1-control-session", node_keys)
        self.assertIn(
            "ue:imsi-208930000000001->session:imsi-208930000000001:ue1-video-session",
            edge_keys,
        )
        self.assertIn(
            "flow:imsi-208930000000001:app-2:flow-2->session:imsi-208930000000001:ue1-control-session",
            edge_keys,
        )
        self.assertIn(
            "session:imsi-208930000000001:ue1-control-session->slice:01112233",
            edge_keys,
        )
        self.assertEqual(
            summary_nodes["session:imsi-208930000000001:ue1-control-session"]["properties"]["dnn"],
            "enterprise",
        )
        self.assertEqual(
            summary_nodes["session:imsi-208930000000001:ue1-control-session"]["properties"]["slice_ref"],
            "slice-1-112233",
        )
        self.assertEqual(
            summary_nodes["session:imsi-208930000000001:ue1-control-session"]["properties"]["flow_ids"],
            ["flow-2"],
        )

    def test_delta_bundle_skips_unchanged_topology_rows(self) -> None:
        previous_bundle = build_graph_snapshot_bundle(TickSnapshot.from_dict(build_payload()))

        payload = build_payload()
        payload["tick_index"] = 1
        payload["flows"][0]["throughput_dl_mbps"] = 12.5
        current_bundle = build_graph_snapshot_bundle(TickSnapshot.from_dict(payload))

        delta_bundle = build_delta_graph_snapshot_bundle(
            current_bundle,
            {row["node_key"]: row for row in previous_bundle.node_rows},
            {row["edge_key"]: row for row in previous_bundle.edge_rows},
        )

        self.assertEqual(delta_bundle.node_rows, [])
        self.assertEqual(delta_bundle.edge_rows, [])
        self.assertEqual(len(delta_bundle.metric_rows), len(current_bundle.metric_rows))
        self.assertEqual(delta_bundle.snapshot_row["graph_summary"]["write_mode"], "delta")
        self.assertEqual(delta_bundle.snapshot_row["graph_summary"]["delta_node_count"], 0)
        self.assertEqual(delta_bundle.snapshot_row["graph_summary"]["delta_edge_count"], 0)

    def test_delta_bundle_writes_tombstones_for_removed_graph_rows(self) -> None:
        previous_bundle = build_graph_snapshot_bundle(TickSnapshot.from_dict(build_payload()))

        payload = build_payload()
        payload["tick_index"] = 1
        payload["flows"] = []
        current_bundle = build_graph_snapshot_bundle(TickSnapshot.from_dict(payload))

        delta_bundle = build_delta_graph_snapshot_bundle(
            current_bundle,
            {row["node_key"]: row for row in previous_bundle.node_rows},
            {row["edge_key"]: row for row in previous_bundle.edge_rows},
        )

        removed_node_keys = {
            row["node_key"]
            for row in delta_bundle.node_rows
            if row["properties"].get(GRAPH_ROW_DELETED)
        }
        removed_edge_keys = {
            row["edge_key"]
            for row in delta_bundle.edge_rows
            if row["properties"].get(GRAPH_ROW_DELETED)
        }

        self.assertIn("flow:imsi-208930000000001:app-1:flow-1", removed_node_keys)
        self.assertIn("app:imsi-208930000000001:app-1", removed_node_keys)
        self.assertIn(
            "app:imsi-208930000000001:app-1->flow:imsi-208930000000001:app-1:flow-1",
            removed_edge_keys,
        )
        self.assertEqual(delta_bundle.snapshot_row["graph_summary"]["deleted_node_count"], 2)
        self.assertGreaterEqual(delta_bundle.snapshot_row["graph_summary"]["deleted_edge_count"], 1)


if __name__ == "__main__":
    unittest.main()
