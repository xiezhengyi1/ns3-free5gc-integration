"""PostgreSQL persistence for graph snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bridge.common.schema import TickSnapshot
from bridge.writer.graph_mapper import (
    GraphSnapshotBundle,
    build_delta_graph_snapshot_bundle,
    build_graph_snapshot_bundle,
    is_graph_row_deleted,
)


def normalize_db_url(db_url: str) -> str:
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url


def _serialize_timestamp(value: object) -> object:
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return value


def _has_embedded_graph(summary: dict[str, object]) -> bool:
    return all(isinstance(summary.get(key), list) for key in ("nodes", "edges", "metrics"))


def _load_sqlalchemy() -> dict[str, Any]:
    try:
        from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, text
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.orm import Session, declarative_base
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PostgreSQL graph persistence requires SQLAlchemy and psycopg. "
            "Install project dependencies before using --graph-db-url."
        ) from exc

    return {
        "Column": Column,
        "DateTime": DateTime,
        "ForeignKey": ForeignKey,
        "Integer": Integer,
        "JSONB": JSONB,
        "Session": Session,
        "String": String,
        "UniqueConstraint": UniqueConstraint,
        "create_engine": create_engine,
        "declarative_base": declarative_base,
        "text": text,
    }


def _build_models() -> dict[str, Any]:
    sa = _load_sqlalchemy()
    Base = sa["declarative_base"]()

    class NetworkGraphSnapshot(Base):
        __tablename__ = "network_graph_snapshot"

        id = sa["Column"](sa["Integer"], primary_key=True, autoincrement=True)
        snapshot_id = sa["Column"](sa["String"], unique=True, nullable=False, index=True)
        base_network_snapshot_id = sa["Column"](sa["String"], nullable=True, index=True)
        trigger_event = sa["Column"](sa["String"], nullable=True)
        graph_summary = sa["Column"](sa["JSONB"], nullable=True)
        created_at = sa["Column"](sa["DateTime"], nullable=False)

    class GraphNode(Base):
        __tablename__ = "graph_node"

        id = sa["Column"](sa["Integer"], primary_key=True, autoincrement=True)
        snapshot_id = sa["Column"](
            sa["String"],
            sa["ForeignKey"]("network_graph_snapshot.snapshot_id"),
            nullable=False,
            index=True,
        )
        node_key = sa["Column"](sa["String"], nullable=False)
        node_type = sa["Column"](sa["String"], nullable=False, index=True)
        label = sa["Column"](sa["String"], nullable=True)
        properties = sa["Column"](sa["JSONB"], nullable=True)

        __table_args__ = (
            sa["UniqueConstraint"]("snapshot_id", "node_key", name="uq_graph_node_snapshot_key"),
        )

    class GraphEdge(Base):
        __tablename__ = "graph_edge"

        id = sa["Column"](sa["Integer"], primary_key=True, autoincrement=True)
        snapshot_id = sa["Column"](
            sa["String"],
            sa["ForeignKey"]("network_graph_snapshot.snapshot_id"),
            nullable=False,
            index=True,
        )
        edge_key = sa["Column"](sa["String"], nullable=False)
        edge_type = sa["Column"](sa["String"], nullable=False, index=True)
        source_key = sa["Column"](sa["String"], nullable=False, index=True)
        target_key = sa["Column"](sa["String"], nullable=False, index=True)
        properties = sa["Column"](sa["JSONB"], nullable=True)

        __table_args__ = (
            sa["UniqueConstraint"]("snapshot_id", "edge_key", name="uq_graph_edge_snapshot_key"),
        )

    class GraphMetric(Base):
        __tablename__ = "graph_metric"

        id = sa["Column"](sa["Integer"], primary_key=True, autoincrement=True)
        snapshot_id = sa["Column"](
            sa["String"],
            sa["ForeignKey"]("network_graph_snapshot.snapshot_id"),
            nullable=False,
            index=True,
        )
        owner_type = sa["Column"](sa["String"], nullable=False)
        owner_key = sa["Column"](sa["String"], nullable=False, index=True)
        metric_name = sa["Column"](sa["String"], nullable=False, index=True)
        metric_value = sa["Column"](sa["JSONB"], nullable=True)
        observed_at = sa["Column"](sa["DateTime"], nullable=False, index=True)

    return {
        "Base": Base,
        "NetworkGraphSnapshot": NetworkGraphSnapshot,
        "GraphNode": GraphNode,
        "GraphEdge": GraphEdge,
        "GraphMetric": GraphMetric,
        **sa,
    }


@dataclass(slots=True)
class GraphStoreResult:
    inserted: bool
    snapshot_id: str
    base_network_snapshot_id: str | None
    node_count: int
    edge_count: int
    metric_count: int
    delta_node_count: int = 0
    delta_edge_count: int = 0
    delta_metric_count: int = 0
    write_mode: str = "snapshot"

    def to_dict(self) -> dict[str, object]:
        return {
            "inserted": self.inserted,
            "snapshot_id": self.snapshot_id,
            "base_network_snapshot_id": self.base_network_snapshot_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "metric_count": self.metric_count,
            "delta_node_count": self.delta_node_count,
            "delta_edge_count": self.delta_edge_count,
            "delta_metric_count": self.delta_metric_count,
            "write_mode": self.write_mode,
        }


class PostgresGraphStore:
    def __init__(self, db_url: str, ensure_schema: bool = False) -> None:
        self.db_url = db_url
        self.models = _build_models()
        self.engine = self.models["create_engine"](normalize_db_url(db_url), future=True)
        if ensure_schema:
            self.models["Base"].metadata.create_all(self.engine)

    def _find_existing_snapshot_id(self, run_id: str, tick_index: int) -> str | None:
        statement = self.models["text"](
            """
            SELECT snapshot_id
            FROM network_graph_snapshot
            WHERE graph_summary->>'run_id' = :run_id
              AND graph_summary->>'tick_index' = :tick_index
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        with self.models["Session"](self.engine) as session:
            return session.execute(
                statement,
                {"run_id": run_id, "tick_index": str(tick_index)},
            ).scalar_one_or_none()

    def _find_previous_snapshot_id(self, run_id: str, tick_index: int) -> str | None:
        statement = self.models["text"](
            """
            SELECT snapshot_id
            FROM network_graph_snapshot
            WHERE graph_summary->>'run_id' = :run_id
              AND (graph_summary->>'tick_index')::integer < :tick_index
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        with self.models["Session"](self.engine) as session:
            return session.execute(
                statement,
                {"run_id": run_id, "tick_index": tick_index},
            ).scalar_one_or_none()

    def _fetch_snapshot_record(self, snapshot_id: str) -> dict[str, object] | None:
        statement = self.models["text"](
            """
            SELECT snapshot_id, base_network_snapshot_id, trigger_event, graph_summary, created_at
            FROM network_graph_snapshot
            WHERE snapshot_id = :snapshot_id
            """
        )
        with self.models["Session"](self.engine) as session:
            row = session.execute(statement, {"snapshot_id": snapshot_id}).mappings().one_or_none()
            return dict(row) if row is not None else None

    def _load_effective_node_rows(
        self,
        run_id: str,
        tick_index: int,
    ) -> dict[str, dict[str, object]]:
        statement = self.models["text"](
            """
            SELECT DISTINCT ON (n.node_key)
                n.node_key,
                n.node_type,
                n.label,
                n.properties
            FROM graph_node n
            JOIN network_graph_snapshot s ON s.snapshot_id = n.snapshot_id
            WHERE s.graph_summary->>'run_id' = :run_id
              AND (s.graph_summary->>'tick_index')::integer <= :tick_index
            ORDER BY
                n.node_key,
                (s.graph_summary->>'tick_index')::integer DESC,
                s.created_at DESC,
                n.id DESC
            """
        )
        with self.models["Session"](self.engine) as session:
            rows = session.execute(
                statement,
                {"run_id": run_id, "tick_index": tick_index},
            ).mappings()
            resolved: dict[str, dict[str, object]] = {}
            for row in rows:
                payload = {
                    "node_key": row["node_key"],
                    "node_type": row["node_type"],
                    "label": row["label"],
                    "properties": row["properties"] or {},
                }
                if is_graph_row_deleted(payload):
                    continue
                resolved[str(row["node_key"])] = payload
            return resolved

    def _load_effective_edge_rows(
        self,
        run_id: str,
        tick_index: int,
    ) -> dict[str, dict[str, object]]:
        statement = self.models["text"](
            """
            SELECT DISTINCT ON (e.edge_key)
                e.edge_key,
                e.edge_type,
                e.source_key,
                e.target_key,
                e.properties
            FROM graph_edge e
            JOIN network_graph_snapshot s ON s.snapshot_id = e.snapshot_id
            WHERE s.graph_summary->>'run_id' = :run_id
              AND (s.graph_summary->>'tick_index')::integer <= :tick_index
            ORDER BY
                e.edge_key,
                (s.graph_summary->>'tick_index')::integer DESC,
                s.created_at DESC,
                e.id DESC
            """
        )
        with self.models["Session"](self.engine) as session:
            rows = session.execute(
                statement,
                {"run_id": run_id, "tick_index": tick_index},
            ).mappings()
            resolved: dict[str, dict[str, object]] = {}
            for row in rows:
                payload = {
                    "edge_key": row["edge_key"],
                    "edge_type": row["edge_type"],
                    "source_key": row["source_key"],
                    "target_key": row["target_key"],
                    "properties": row["properties"] or {},
                }
                if is_graph_row_deleted(payload):
                    continue
                resolved[str(row["edge_key"])] = payload
            return resolved

    def _load_metric_rows(self, snapshot_id: str) -> list[dict[str, object]]:
        statement = self.models["text"](
            """
            SELECT owner_type, owner_key, metric_name, metric_value, observed_at
            FROM graph_metric
            WHERE snapshot_id = :snapshot_id
            ORDER BY owner_type, owner_key, metric_name, id
            """
        )
        with self.models["Session"](self.engine) as session:
            rows = session.execute(statement, {"snapshot_id": snapshot_id}).mappings()
            return [
                {
                    "owner_type": row["owner_type"],
                    "owner_key": row["owner_key"],
                    "metric_name": row["metric_name"],
                    "metric_value": row["metric_value"],
                    "observed_at": _serialize_timestamp(row["observed_at"]),
                }
                for row in rows
            ]

    def load_graph_snapshot(self, snapshot_id: str) -> dict[str, object]:
        record = self._fetch_snapshot_record(snapshot_id)
        if record is None:
            raise ValueError(f"unknown snapshot_id: {snapshot_id}")

        summary = dict(record.get("graph_summary") or {})
        payload = dict(summary)

        if not _has_embedded_graph(summary):
            run_id = summary.get("run_id")
            tick_index = summary.get("tick_index")
            if run_id is None or tick_index is None:
                raise ValueError(
                    f"snapshot {snapshot_id} has no embedded graph payload and cannot be reconstructed"
                )
            node_rows = sorted(
                self._load_effective_node_rows(str(run_id), int(tick_index)).values(),
                key=lambda row: (str(row["node_type"]), str(row["node_key"])),
            )
            edge_rows = sorted(
                self._load_effective_edge_rows(str(run_id), int(tick_index)).values(),
                key=lambda row: (str(row["edge_type"]), str(row["edge_key"])),
            )
            payload["nodes"] = node_rows
            payload["edges"] = edge_rows
            payload["metrics"] = self._load_metric_rows(snapshot_id)

        payload.setdefault("snapshot_id", record["snapshot_id"])
        payload.setdefault("base_network_snapshot_id", record["base_network_snapshot_id"])
        payload.setdefault("trigger_event", record["trigger_event"])
        payload.setdefault("created_at", _serialize_timestamp(record["created_at"]))
        payload.setdefault("node_count", len(payload.get("nodes", [])))
        payload.setdefault("edge_count", len(payload.get("edges", [])))
        payload.setdefault("metric_count", len(payload.get("metrics", [])))
        return payload

    def ingest_snapshot(
        self,
        snapshot: TickSnapshot,
        *,
        trigger_event: str | None = None,
    ) -> GraphStoreResult:
        existing_snapshot_id = self._find_existing_snapshot_id(snapshot.run_id, snapshot.tick_index)
        if existing_snapshot_id is not None:
            return GraphStoreResult(
                inserted=False,
                snapshot_id=existing_snapshot_id,
                base_network_snapshot_id=None,
                node_count=0,
                edge_count=0,
                metric_count=0,
                write_mode="existing",
            )

        base_snapshot_id = self._find_previous_snapshot_id(snapshot.run_id, snapshot.tick_index)
        full_bundle = build_graph_snapshot_bundle(
            snapshot,
            base_network_snapshot_id=base_snapshot_id,
            trigger_event=trigger_event,
        )
        previous_node_rows: dict[str, dict[str, object]] = {}
        previous_edge_rows: dict[str, dict[str, object]] = {}
        if base_snapshot_id is not None:
            previous_node_rows = self._load_effective_node_rows(snapshot.run_id, snapshot.tick_index - 1)
            previous_edge_rows = self._load_effective_edge_rows(snapshot.run_id, snapshot.tick_index - 1)
        bundle = build_delta_graph_snapshot_bundle(
            full_bundle,
            previous_node_rows,
            previous_edge_rows,
        )
        self._persist_bundle(bundle)
        graph_summary = dict(bundle.snapshot_row["graph_summary"])
        return GraphStoreResult(
            inserted=True,
            snapshot_id=str(bundle.snapshot_row["snapshot_id"]),
            base_network_snapshot_id=base_snapshot_id,
            node_count=int(graph_summary["node_count"]),
            edge_count=int(graph_summary["edge_count"]),
            metric_count=int(graph_summary["metric_count"]),
            delta_node_count=int(graph_summary["delta_node_count"]),
            delta_edge_count=int(graph_summary["delta_edge_count"]),
            delta_metric_count=int(graph_summary["delta_metric_count"]),
            write_mode=str(graph_summary["write_mode"]),
        )

    def _persist_bundle(self, bundle: GraphSnapshotBundle) -> None:
        NetworkGraphSnapshot = self.models["NetworkGraphSnapshot"]
        GraphNode = self.models["GraphNode"]
        GraphEdge = self.models["GraphEdge"]
        GraphMetric = self.models["GraphMetric"]

        with self.models["Session"](self.engine) as session:
            session.add(NetworkGraphSnapshot(**bundle.snapshot_row))
            # Ensure snapshot row is persisted before dependent edge/node/metric rows.
            session.flush()
            session.add_all(GraphNode(**row) for row in bundle.node_rows)
            session.add_all(GraphEdge(**row) for row in bundle.edge_rows)
            session.add_all(GraphMetric(**row) for row in bundle.metric_rows)
            session.commit()