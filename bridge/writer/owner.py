"""Single-process owner for the local writer SQLite store.

Followers send one JSON request over a Unix-domain socket.  The owner handles
requests serially, so only this process opens SQLite for writes; followers never
contend for WAL/schema locks during reset.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import socket
import socketserver
import time
import zlib
from typing import Any

from bridge.common.schema import SimEvent, TickSnapshot
from bridge.writer.local_store import SnapshotStore


_USE_UNIX_SOCKET = hasattr(socket, "AF_UNIX")


def _endpoint(socket_path: Path) -> str | tuple[str, int]:
    if _USE_UNIX_SOCKET:
        return str(socket_path)
    # Windows Python builds without AF_UNIX use a deterministic loopback port
    # only for local development/tests; Linux deployments use the socket path.
    return ("127.0.0.1", 39000 + (zlib.crc32(str(socket_path).encode("utf-8")) % 2000))


if _USE_UNIX_SOCKET:
    class _LocalStreamServer(socketserver.TCPServer):
        address_family = socket.AF_UNIX

        def server_bind(self) -> None:
            self.socket.bind(self.server_address)
            self.server_address = self.socket.getsockname()
else:
    class _LocalStreamServer(socketserver.TCPServer):
        allow_reuse_address = True


class SnapshotStoreClient:
    """SnapshotStore-compatible RPC client used by writer follower processes."""

    def __init__(self, socket_path: str | Path, *, timeout_seconds: float = 30.0) -> None:
        self.socket_path = Path(socket_path).expanduser().resolve()
        self.timeout_seconds = timeout_seconds

    def _request(self, method: str, **params: Any) -> Any:
        payload = json.dumps({"method": method, "params": params}, ensure_ascii=False) + "\n"
        deadline = time.monotonic() + self.timeout_seconds
        response = b""
        while True:
            try:
                family = socket.AF_UNIX if _USE_UNIX_SOCKET else socket.AF_INET
                with socket.socket(family, socket.SOCK_STREAM) as connection:
                    connection.settimeout(max(0.1, deadline - time.monotonic()))
                    connection.connect(_endpoint(self.socket_path))
                    connection.sendall(payload.encode("utf-8"))
                    while not response.endswith(b"\n"):
                        chunk = connection.recv(65536)
                        if not chunk:
                            raise RuntimeError("writer owner closed the RPC connection")
                        response += chunk
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"writer owner did not become ready: {exc}") from exc
                time.sleep(0.05)
        decoded = json.loads(response.decode("utf-8"))
        if not decoded.get("ok"):
            raise RuntimeError(str(decoded.get("error") or "writer owner request failed"))
        return decoded.get("result")

    def upsert_run(self, run_id: str, scenario_id: str, seed: int | None = None, topology_version: str = "v1", status: str = "running") -> None:
        self._request("upsert_run", run_id=run_id, scenario_id=scenario_id, seed=seed, topology_version=topology_version, status=status)

    def mark_run_status(self, run_id: str, status: str) -> None:
        self._request("mark_run_status", run_id=run_id, status=status)

    def ingest_snapshot(self, snapshot: TickSnapshot, seed: int | None = None, topology_version: str = "v1") -> dict[str, object]:
        return dict(self._request("ingest_snapshot", snapshot=snapshot.to_dict(), seed=seed, topology_version=topology_version) or {})

    def append_event(self, event: SimEvent) -> dict[str, object]:
        return dict(self._request("append_event", event=event.to_dict()) or {})

    def latest_snapshot_tick(self, run_id: str) -> int:
        return int(self._request("latest_snapshot_tick", run_id=run_id))

    def resolve_tick_for_observed_at(self, run_id: str, observed_at: datetime) -> int:
        return int(self._request("resolve_tick_for_observed_at", run_id=run_id, observed_at=observed_at.isoformat()))


class _OwnerRequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            request_payload = json.loads(self.rfile.readline().decode("utf-8"))
            result = self.server.dispatch(request_payload)  # type: ignore[attr-defined]
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": f"{exc.__class__.__name__}: {exc}"}
        self.wfile.write((json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8"))


class SnapshotStoreOwner(_LocalStreamServer):
    allow_reuse_address = True

    def __init__(self, socket_path: str | Path, *, state_db: str | Path, archive_dir: str | Path) -> None:
        self.socket_path = Path(socket_path).expanduser().resolve()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        if _USE_UNIX_SOCKET and self.socket_path.exists():
            self.socket_path.unlink()
        self.store = SnapshotStore(state_db, archive_dir)
        super().__init__(_endpoint(self.socket_path), _OwnerRequestHandler)

    def server_close(self) -> None:
        super().server_close()
        if _USE_UNIX_SOCKET and self.socket_path.exists():
            self.socket_path.unlink()

    def dispatch(self, payload: dict[str, Any]) -> Any:
        method = str(payload.get("method") or "")
        params = payload.get("params") or {}
        if not isinstance(params, dict):
            raise ValueError("writer owner params must be an object")
        if method == "upsert_run":
            return self.store.upsert_run(**params)
        if method == "mark_run_status":
            return self.store.mark_run_status(**params)
        if method == "ingest_snapshot":
            snapshot = TickSnapshot.from_dict(dict(params.pop("snapshot")))
            return self.store.ingest_snapshot(snapshot, **params)
        if method == "append_event":
            event = SimEvent.from_dict(dict(params.pop("event")))
            return self.store.append_event(event)
        if method == "latest_snapshot_tick":
            return self.store.latest_snapshot_tick(**params)
        if method == "resolve_tick_for_observed_at":
            observed_at = datetime.fromisoformat(str(params.pop("observed_at")))
            return self.store.resolve_tick_for_observed_at(observed_at=observed_at, **params)
        raise ValueError(f"unsupported writer owner method: {method}")


def serve_forever(*, socket_path: str | Path, state_db: str | Path, archive_dir: str | Path) -> None:
    with SnapshotStoreOwner(socket_path, state_db=state_db, archive_dir=archive_dir) as owner:
        owner.serve_forever(poll_interval=0.2)
