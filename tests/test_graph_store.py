from __future__ import annotations

import unittest
from unittest import mock

from bridge.common.schema import TickSnapshot
from bridge.writer.graph_mapper import GraphSnapshotBundle
from bridge.writer.postgres_graph_store import normalize_db_url
from bridge.writer.postgres_graph_store import PostgresGraphStore
from tests.test_schema import build_payload


class GraphStoreTest(unittest.TestCase):
    def test_normalize_plain_postgresql_url(self) -> None:
        self.assertEqual(
            normalize_db_url("postgresql://postgres:123456@localhost:5433/multiagents_db"),
            "postgresql+psycopg://postgres:123456@localhost:5433/multiagents_db",
        )

    def test_preserve_explicit_driver(self) -> None:
        self.assertEqual(
            normalize_db_url("postgresql+psycopg://postgres:123456@localhost:5433/multiagents_db"),
            "postgresql+psycopg://postgres:123456@localhost:5433/multiagents_db",
        )

    def test_persist_bundle_flushes_snapshot_before_dependents(self) -> None:
        operations: list[tuple[str, str | int | None]] = []

        class FakeBase:
            metadata = mock.Mock()

        class FakeSnapshot:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeNode:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeEdge:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeMetric:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeSession:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def add(self, item):
                operations.append(("add", type(item).__name__))

            def flush(self):
                operations.append(("flush", None))

            def add_all(self, items):
                collected = list(items)
                item_name = type(collected[0]).__name__ if collected else "empty"
                operations.append(("add_all", item_name))

            def commit(self):
                operations.append(("commit", None))

        fake_models = {
            "Base": FakeBase,
            "NetworkGraphSnapshot": FakeSnapshot,
            "GraphNode": FakeNode,
            "GraphEdge": FakeEdge,
            "GraphMetric": FakeMetric,
            "Session": lambda engine: FakeSession(),
            "create_engine": lambda db_url, future=True: object(),
        }

        bundle = GraphSnapshotBundle(
            snapshot_row={
                "snapshot_id": "snap-1",
                "base_network_snapshot_id": None,
                "trigger_event": "sim_tick:test:0",
                "graph_summary": {"run_id": "run-1", "tick_index": 0},
                "created_at": None,
            },
            node_rows=[
                {
                    "snapshot_id": "snap-1",
                    "node_key": "node-1",
                    "node_type": "ue",
                    "label": "UE-1",
                    "properties": {},
                }
            ],
            edge_rows=[
                {
                    "snapshot_id": "snap-1",
                    "edge_key": "node-1->node-2",
                    "edge_type": "attached_to",
                    "source_key": "node-1",
                    "target_key": "node-2",
                    "properties": {},
                }
            ],
            metric_rows=[
                {
                    "snapshot_id": "snap-1",
                    "owner_type": "node",
                    "owner_key": "node-1",
                    "metric_name": "delay_ms",
                    "metric_value": 1.0,
                    "observed_at": None,
                }
            ],
        )

        with mock.patch("bridge.writer.postgres_graph_store._build_models", return_value=fake_models):
            store = PostgresGraphStore("postgresql://postgres:123456@localhost:5433/multiagents_db")
            store._persist_bundle(bundle)

        self.assertEqual(
            [name for name, _ in operations],
            ["add", "flush", "add_all", "add_all", "add_all", "commit"],
        )

    def test_ingest_snapshot_persists_only_delta_rows_after_first_snapshot(self) -> None:
        fake_models = {
            "Base": mock.Mock(metadata=mock.Mock()),
            "NetworkGraphSnapshot": mock.Mock(),
            "GraphNode": mock.Mock(),
            "GraphEdge": mock.Mock(),
            "GraphMetric": mock.Mock(),
            "Session": lambda engine: mock.Mock(),
            "create_engine": lambda db_url, future=True: object(),
        }

        with mock.patch("bridge.writer.postgres_graph_store._build_models", return_value=fake_models):
            store = PostgresGraphStore("postgresql://postgres:123456@localhost:5433/multiagents_db")

        previous_snapshot = TickSnapshot.from_dict(build_payload())
        current_payload = build_payload()
        current_payload["tick_index"] = 1
        current_payload["flows"][0]["throughput_dl_mbps"] = 15.0
        current_snapshot = TickSnapshot.from_dict(current_payload)

        with mock.patch.object(store, "_find_existing_snapshot_id", return_value=None), mock.patch.object(
            store, "_find_previous_snapshot_id", return_value="snap-0"
        ), mock.patch.object(
            store,
            "_load_effective_node_rows",
            return_value={row["node_key"]: row for row in store and []},
        ):
            pass

        from bridge.writer.graph_mapper import build_graph_snapshot_bundle

        previous_bundle = build_graph_snapshot_bundle(previous_snapshot)

        with mock.patch.object(store, "_find_existing_snapshot_id", return_value=None), mock.patch.object(
            store, "_find_previous_snapshot_id", return_value="snap-0"
        ), mock.patch.object(
            store,
            "_load_effective_node_rows",
            return_value={row["node_key"]: row for row in previous_bundle.node_rows},
        ), mock.patch.object(
            store,
            "_load_effective_edge_rows",
            return_value={row["edge_key"]: row for row in previous_bundle.edge_rows},
        ), mock.patch.object(store, "_persist_bundle") as persist_bundle:
            result = store.ingest_snapshot(current_snapshot)

        persisted_bundle = persist_bundle.call_args.args[0]
        self.assertEqual(result.write_mode, "delta")
        self.assertEqual(result.node_count, len(previous_bundle.node_rows))
        self.assertEqual(result.edge_count, len(previous_bundle.edge_rows))
        self.assertEqual(result.delta_node_count, 0)
        self.assertEqual(result.delta_edge_count, 0)
        self.assertEqual(len(persisted_bundle.node_rows), 0)
        self.assertEqual(len(persisted_bundle.edge_rows), 0)
        self.assertEqual(result.delta_metric_count, len(persisted_bundle.metric_rows))

    def test_load_graph_snapshot_prefers_embedded_graph_summary(self) -> None:
        fake_models = {
            "Base": mock.Mock(metadata=mock.Mock()),
            "NetworkGraphSnapshot": mock.Mock(),
            "GraphNode": mock.Mock(),
            "GraphEdge": mock.Mock(),
            "GraphMetric": mock.Mock(),
            "Session": lambda engine: mock.Mock(),
            "create_engine": lambda db_url, future=True: object(),
        }

        with mock.patch("bridge.writer.postgres_graph_store._build_models", return_value=fake_models):
            store = PostgresGraphStore("postgresql://postgres:123456@localhost:5433/multiagents_db")

        with mock.patch.object(
            store,
            "_fetch_snapshot_record",
            return_value={
                "snapshot_id": "snap-1",
                "base_network_snapshot_id": None,
                "trigger_event": "Manual-Reset",
                "created_at": None,
                "graph_summary": {
                    "snapshot_id": "snap-1",
                    "trigger_event": "Manual-Reset",
                    "nodes": [{"node_key": "ue:imsi-1", "node_type": "ue", "label": "imsi-1", "properties": {"supi": "imsi-1"}}],
                    "edges": [],
                    "metrics": [],
                },
            },
        ):
            payload = store.load_graph_snapshot("snap-1")

        self.assertEqual(payload["snapshot_id"], "snap-1")
        self.assertEqual(payload["trigger_event"], "Manual-Reset")
        self.assertEqual(payload["nodes"][0]["node_key"], "ue:imsi-1")

    def test_load_graph_snapshot_reconstructs_missing_embedded_graph(self) -> None:
        fake_models = {
            "Base": mock.Mock(metadata=mock.Mock()),
            "NetworkGraphSnapshot": mock.Mock(),
            "GraphNode": mock.Mock(),
            "GraphEdge": mock.Mock(),
            "GraphMetric": mock.Mock(),
            "Session": lambda engine: mock.Mock(),
            "create_engine": lambda db_url, future=True: object(),
        }

        with mock.patch("bridge.writer.postgres_graph_store._build_models", return_value=fake_models):
            store = PostgresGraphStore("postgresql://postgres:123456@localhost:5433/multiagents_db")

        with mock.patch.object(
            store,
            "_fetch_snapshot_record",
            return_value={
                "snapshot_id": "snap-2",
                "base_network_snapshot_id": "snap-1",
                "trigger_event": "sim_tick:run-1:1",
                "created_at": None,
                "graph_summary": {"run_id": "run-1", "tick_index": 1},
            },
        ), mock.patch.object(
            store,
            "_load_effective_node_rows",
            return_value={
                "ue:imsi-1": {"node_key": "ue:imsi-1", "node_type": "ue", "label": "imsi-1", "properties": {"supi": "imsi-1"}}
            },
        ), mock.patch.object(
            store,
            "_load_effective_edge_rows",
            return_value={
                "ue:imsi-1->app:imsi-1:app-1": {
                    "edge_key": "ue:imsi-1->app:imsi-1:app-1",
                    "edge_type": "owns",
                    "source_key": "ue:imsi-1",
                    "target_key": "app:imsi-1:app-1",
                    "properties": {"supi": "imsi-1"},
                }
            },
        ), mock.patch.object(
            store,
            "_load_metric_rows",
            return_value=[{"owner_type": "node", "owner_key": "flow:imsi-1:app-1:flow-1", "metric_name": "sim_latency", "metric_value": 1.2, "observed_at": None}],
        ):
            payload = store.load_graph_snapshot("snap-2")

        self.assertEqual(payload["snapshot_id"], "snap-2")
        self.assertEqual(payload["base_network_snapshot_id"], "snap-1")
        self.assertEqual(payload["node_count"], 1)
        self.assertEqual(payload["edge_count"], 1)
        self.assertEqual(payload["metric_count"], 1)


if __name__ == "__main__":
    unittest.main()