"""Persistent public proxy for the restartable policy acceptor."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


EXECUTION_PATH = "/policy-executions"


def _operation_id(payload: dict[str, object]) -> str:
    identity = f"{str(payload.get('session_id') or '').strip()}\x00{str(payload.get('policy_id') or '').strip()}"
    return f"op-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:32]}"


def _pending(payload: dict[str, object], operation_id: str, phase: str = "cold_restart_wait") -> dict[str, object]:
    return {
        "status": "pending", "status_code": 202, "phase": phase, "operation_id": operation_id,
        "request_id": str(payload.get("request_id") or ""), "session_id": str(payload.get("session_id") or ""),
        "snapshot_id": str(payload.get("snapshot_id") or ""), "policy_id": str(payload.get("policy_id") or ""),
        "policy_type": str(payload.get("policy_type") or ""), "flow_id": str(payload.get("flow_id") or ""),
        "execution_status": "PENDING", "compliance_status": "PENDING", "baseline_tick": -1,
        "applied_tick": None, "upstream": {}, "mutation_summary": {}, "monitoring_data": {"baseline_tick": -1},
        "qos_violations": [], "message": "Policy execution is pending while the simulator cold-restarts.", "error": "",
    }


class Watchdog:
    def __init__(self, backend_port: int, queue_file: Path, recovery_grace_seconds: float) -> None:
        self.backend_port = backend_port
        self.queue_file = queue_file
        self.recovery_grace_seconds = max(0.0, recovery_grace_seconds)
        self.lock = threading.Lock()
        self.queue: dict[str, dict[str, object]] = self._load()
        self.backend_ready_since: float | None = None

    def _load(self) -> dict[str, dict[str, object]]:
        try:
            raw = json.loads(self.queue_file.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _save(self) -> None:
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        temp = self.queue_file.with_suffix(".tmp")
        temp.write_text(json.dumps(self.queue), encoding="utf-8")
        temp.replace(self.queue_file)

    def forward(self, method: str, path: str, body: bytes, headers: dict[str, str]) -> tuple[int, bytes, dict[str, str]] | None:
        # A freshly restarted acceptor can listen before NS-3 has emitted its
        # first usable snapshot. Keep the public endpoint stable and queue
        # requests for a short, explicit recovery window.
        if not self._backend_ready_for_forwarding():
            return None
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.backend_port, timeout=3)
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            data = response.read()
            result_headers = {key: value for key, value in response.getheaders() if key.lower() in {"content-type", "x-policy-acceptor", "x-policy-acceptor-instance"}}
            status = response.status
            conn.close()
            return status, data, result_headers
        except OSError:
            self.backend_ready_since = None
            return None

    def _backend_ready_for_forwarding(self) -> bool:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.backend_port, timeout=1)
            conn.request("GET", f"{EXECUTION_PATH}/launch-healthcheck", headers={"Accept": "application/json"})
            response = conn.getresponse()
            response.read()
            conn.close()
        except OSError:
            self.backend_ready_since = None
            return False
        now = time.monotonic()
        if self.backend_ready_since is None:
            self.backend_ready_since = now
            return False
        return now - self.backend_ready_since >= self.recovery_grace_seconds

    def queue_post(self, payload: dict[str, object]) -> str:
        operation_id = _operation_id(payload)
        with self.lock:
            self.queue.setdefault(operation_id, payload)
            self._save()
        return operation_id

    def replay(self) -> None:
        with self.lock:
            items = list(self.queue.items())
        for operation_id, payload in items:
            body = json.dumps(payload).encode("utf-8")
            result = self.forward("POST", EXECUTION_PATH, body, {"Content-Type": "application/json", "Accept": "application/json"})
            if result is None:
                return
            if result[0] < 500:
                with self.lock:
                    self.queue.pop(operation_id, None)
                    self._save()


class Handler(BaseHTTPRequestHandler):
    watchdog: Watchdog

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        result = self.watchdog.forward("POST", self.path, body, {"Content-Type": self.headers.get("Content-Type", "application/json"), "Accept": "application/json"})
        if result is not None:
            self._send(*result)
            return
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self._send_json(503, {"status": "failed", "status_code": 503, "phase": "cold_restart", "error": "policy acceptor is restarting"})
            return
        if self.path.rstrip("/") != EXECUTION_PATH or not isinstance(payload, dict):
            self._send_json(503, {"status": "failed", "status_code": 503, "phase": "cold_restart", "error": "policy acceptor is restarting"})
            return
        operation_id = self.watchdog.queue_post(payload)
        self._send_json(202, _pending(payload, operation_id))

    def do_GET(self) -> None:
        result = self.watchdog.forward("GET", self.path, b"", {"Accept": "application/json"})
        if result is not None:
            self._send(*result)
            return
        operation_id = self.path.rstrip("/").rsplit("/", 1)[-1]
        with self.watchdog.lock:
            queued = self.watchdog.queue.get(operation_id, {})
        self._send_json(202, _pending(queued, operation_id))

    def _send(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
        self.send_header("X-Policy-Acceptor", headers.get("X-Policy-Acceptor", "ns3-free5gc-policy-acceptor"))
        if "X-Policy-Acceptor-Instance" in headers:
            self.send_header("X-Policy-Acceptor-Instance", headers["X-Policy-Acceptor-Instance"])
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: dict[str, object]) -> None:
        self._send(status, json.dumps(payload).encode("utf-8"), {})


def main() -> int:
    parser = argparse.ArgumentParser(description="Persistent policy acceptor watchdog")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--backend-port", type=int, required=True)
    parser.add_argument("--queue-file", required=True)
    parser.add_argument("--recovery-grace-seconds", type=float, default=30.0)
    args = parser.parse_args()
    Handler.watchdog = Watchdog(args.backend_port, Path(args.queue_file), args.recovery_grace_seconds)
    thread = threading.Thread(target=lambda: _replay_loop(Handler.watchdog), daemon=True)
    thread.start()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever(poll_interval=0.2)
    return 0


def _replay_loop(watchdog: Watchdog) -> None:
    while True:
        watchdog.replay()
        time.sleep(0.25)


if __name__ == "__main__":
    raise SystemExit(main())
