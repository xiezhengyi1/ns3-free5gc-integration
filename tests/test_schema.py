from __future__ import annotations

import unittest

from bridge.common.schema import TickSnapshot


def build_payload() -> dict[str, object]:
    return {
        "run_id": "run-1",
        "scenario_id": "scenario-1",
        "tick_index": 0,
        "sim_time_ms": 1000,
        "nodes": [
            {"id": "ran-node-1", "type": "ran_node", "label": "gNB-1"},
            {"id": "ue-node-1", "type": "ue", "label": "UE-1"},
        ],
        "links": [
            {
                "source": "ue-node-1",
                "target": "ran-node-1",
                "type": "attached_to",
            }
        ],
        "gnbs": [
            {
                "gnb_id": "gnb-1",
                "node_id": "ran-node-1",
                "alias": "gnb.free5gc.org",
                "attached_ues": ["ue-1"],
                "dst_upf": "upf",
            }
        ],
        "ues": [
            {
                "ue_id": "ue-1",
                "supi": "imsi-208930000000001",
                "gnb_id": "gnb-1",
                "slice_id": "slice-1-010203",
                "ip_address": "10.60.0.1",
            }
        ],
        "flows": [
            {
                "flow_id": "flow-1",
                "supi": "imsi-208930000000001",
                "app_id": "app-1",
                "src_gnb": "gnb-1",
                "dst_upf": "upf",
                "slice_id": "slice-1-010203",
                "5qi": 9,
                "delay_ms": 1.2,
                "jitter_ms": 0.3,
                "loss_rate": 0.0,
                "throughput_ul_mbps": 0.0,
                "throughput_dl_mbps": 10.5,
                "queue_bytes": 0,
                "rlc_buffer_bytes": 0,
            }
        ],
        "slices": [
            {
                "slice_id": "slice-1-010203",
                "sst": 1,
                "sd": "010203",
                "label": "embb",
            }
        ],
        "kpis": {"throughput_dl_mbps_total": 10.5},
        "reward_inputs": {"throughput_score": 10.5, "delay_penalty": 1.2, "loss_penalty": 0.0},
    }


class TickSnapshotTest(unittest.TestCase):
    def test_accepts_valid_snapshot(self) -> None:
        snapshot = TickSnapshot.from_dict(build_payload())
        self.assertEqual(snapshot.tick_index, 0)
        self.assertEqual(snapshot.flows[0].five_qi, 9)

    def test_rejects_unknown_link_node(self) -> None:
        payload = build_payload()
        payload["links"] = [{"source": "missing", "target": "ran-node-1", "type": "attached_to"}]
        with self.assertRaises(ValueError):
            TickSnapshot.from_dict(payload)


if __name__ == "__main__":
    unittest.main()