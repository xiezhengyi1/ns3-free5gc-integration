"""Manifest-driven fast scenario reset supervisor and loopback HTTP API."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time
from typing import Any, Callable
from urllib import error, request


_COLD_START_COMMANDS = {
    "compose-up-core",
    "bootstrap-subscribers",
    "bootstrap-app-data",
    "compose-up-upf",
    "compose-up-gnb",
    "compose-up-smf",
    "wait-for-pfcp-ready",
    "compose-up-ue",
    "bridge-setup",
    "ns3-build",
}
_BASELINE_RESTORE_COMMANDS = (
    "bootstrap-subscribers",
    "bootstrap-app-data",
    "wait-for-pfcp-ready",
    "bridge-setup",
)
_RUNTIME_ORDER = (
    "writer-follow-free5gc",
    "writer-follow-ueransim",
    "writer-follow-ns3",
    "writer-follow-split-ns3",
    "policy-acceptor",
    "user-plane-gate",
    "split-gate",
    "real-ue-flows",
    "split-results",
    "bridge-probe-post-ns3",
    "ns3-run",
)


class ResetBusyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ManagedProcess:
    name: str
    pid: int
    generation: int
    argv: tuple[str, ...]
    log_file: str


@dataclass(frozen=True, slots=True)
class ResetResult:
    run_id: str
    generation: int
    mode: str
    duration_ms: int
    restarted_services: tuple[str, ...]
    processes: tuple[ManagedProcess, ...]


class ProcessSupervisor:
    def __init__(self, registry_file: Path, *, run_id: str, run_dir: Path) -> None:
        self.registry_file = registry_file
        self.run_id = run_id
        self.run_dir = run_dir
        self._processes: dict[int, subprocess.Popen[str]] = {}
        self._entries = self._load_registry()

    def start(
        self,
        command: dict[str, Any],
        *,
        generation: int,
        log_dir: Path,
    ) -> ManagedProcess:
        name = str(command["name"])
        argv = [str(item) for item in command["argv"]]
        cwd = str(command["cwd"])
        env = os.environ.copy()
        env.update({str(key): str(value) for key, value in (command.get("env") or {}).items()})
        env["SCENARIO_RESET_GENERATION"] = str(generation)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{name}.log"
        with log_file.open("w", encoding="utf-8") as handle:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                text=True,
                start_new_session=True,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
        self._processes[process.pid] = process
        entry = ManagedProcess(
            name=name,
            pid=process.pid,
            generation=generation,
            argv=tuple(argv),
            log_file=str(log_file),
        )
        self._entries = [item for item in self._entries if item.name != name]
        self._entries.append(entry)
        self._save_registry()
        return entry

    def stop_all(self, *, grace_seconds: float = 0.5) -> None:
        entries = list(self._entries)
        for entry in reversed(entries):
            if self._pid_matches(entry):
                self._signal_group(entry.pid, signal.SIGTERM)
        deadline = time.monotonic() + max(0.0, grace_seconds)
        while time.monotonic() < deadline and any(self._pid_matches(entry) for entry in entries):
            time.sleep(0.02)
        for entry in reversed(entries):
            if self._pid_matches(entry):
                self._signal_group(entry.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        self._processes.clear()
        self._entries.clear()
        self._save_registry()

    def status(self) -> tuple[ManagedProcess, ...]:
        return tuple(entry for entry in self._entries if self._pid_matches(entry))

    def _pid_matches(self, entry: ManagedProcess) -> bool:
        process = self._processes.get(entry.pid)
        if process is not None:
            return process.poll() is None
        proc_cmdline = Path(f"/proc/{entry.pid}/cmdline")
        try:
            command_line = proc_cmdline.read_bytes().replace(b"\x00", b" ").decode(
                "utf-8", errors="replace"
            )
        except OSError:
            return False
        if not command_line:
            return False
        scope_tokens = (self.run_id, str(self.run_dir))
        return any(token and token in command_line for token in scope_tokens)

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals | int) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            return

    def _load_registry(self) -> list[ManagedProcess]:
        try:
            payload = json.loads(self.registry_file.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []
        rows = payload.get("processes", []) if isinstance(payload, dict) else []
        entries: list[ManagedProcess] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            try:
                entries.append(
                    ManagedProcess(
                        name=str(row["name"]),
                        pid=int(row["pid"]),
                        generation=int(row["generation"]),
                        argv=tuple(str(item) for item in row["argv"]),
                        log_file=str(row["log_file"]),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return entries

    def _save_registry(self) -> None:
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.registry_file.with_suffix(self.registry_file.suffix + ".tmp")
        temporary.write_text(
            json.dumps(
                {"run_id": self.run_id, "processes": [asdict(item) for item in self._entries]},
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.registry_file)


class FastResetController:
    def __init__(
        self,
        manifest_path: Path,
        *,
        supervisor: ProcessSupervisor | Any | None = None,
        command_runner: Callable[..., Any] = subprocess.run,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.manifest_path = manifest_path.expanduser().resolve()
        payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("run manifest root must be an object")
        self.manifest = payload
        self.run_id = str(payload["run_id"])
        self.run_dir = Path(str(payload["run_dir"])).expanduser().resolve()
        self.state_dir = self.run_dir / "state"
        self.state_file = self.state_dir / "fast-reset-state.json"
        self.registry_file = self.state_dir / "fast-reset-processes.json"
        self.command_by_name = {
            str(command["name"]): command
            for command in payload.get("commands", [])
            if isinstance(command, dict) and command.get("name")
        }
        self._runner = command_runner
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self.supervisor = supervisor or ProcessSupervisor(
            self.registry_file,
            run_id=self.run_id,
            run_dir=self.run_dir,
        )

    def reset(self, *, cold: bool = False) -> ResetResult:
        if not self._lock.acquire(blocking=False):
            raise ResetBusyError("a scenario reset is already in progress")
        started = self._monotonic()
        try:
            previous = self._load_state()
            cold_start = cold or not bool(previous.get("initialized"))
            generation = int(previous.get("generation", 0)) + 1
            self.supervisor.stop_all()
            self._reset_artifacts()
            restarted_services: tuple[str, ...] = ()
            if cold_start:
                self._cold_start()
            else:
                restarted_services = self._restart_control_plane()
                self._restore_baseline()
            processes = self._start_runtime(generation)
            result = ResetResult(
                run_id=self.run_id,
                generation=generation,
                mode="cold" if cold_start else "fast",
                duration_ms=max(0, round((self._monotonic() - started) * 1000)),
                restarted_services=restarted_services,
                processes=processes,
            )
            self._save_state(result, initialized=True)
            return result
        except BaseException:
            self.supervisor.stop_all()
            self._save_failure_state()
            raise
        finally:
            self._lock.release()

    def status(self) -> dict[str, Any]:
        state = self._load_state()
        expected_names = {
            str(process.get("name"))
            for process in state.get("processes", [])
            if isinstance(process, dict) and process.get("name")
        }
        running = [asdict(item) for item in self.supervisor.status()]
        running_names = {str(process["name"]) for process in running}
        state["processes"] = running
        state["missing_processes"] = sorted(expected_names - running_names)
        state["healthy"] = (
            bool(state.get("initialized"))
            and "ns3-run" in running_names
            and not state["missing_processes"]
        )
        return state

    def shutdown(self) -> None:
        self.supervisor.stop_all()

    def _cold_start(self) -> None:
        for command in self.manifest.get("commands", []):
            if isinstance(command, dict) and command.get("name") in _COLD_START_COMMANDS:
                self._run_command(command)

    def _restart_control_plane(self) -> tuple[str, ...]:
        services = tuple(
            dict.fromkeys(
                str(service)
                for service in [
                    *self.manifest.get("core_services", []),
                    *self.manifest.get("ran_services", []),
                ]
                if str(service)
            )
        )
        if not services:
            raise ValueError("manifest does not define control-plane services")
        argv = [
            "docker",
            "compose",
            "-p",
            str(self.manifest["compose_project_name"]),
            "-f",
            str(self.manifest["compose_file"]),
            "restart",
            "--timeout",
            "0",
            *services,
        ]
        compose_command = self.command_by_name.get("compose-up-core")
        compose_cwd = str(compose_command["cwd"]) if compose_command is not None else str(self.run_dir)
        self._runner(argv, cwd=compose_cwd, check=True, text=True)
        return services

    def _restore_baseline(self) -> None:
        for name in _BASELINE_RESTORE_COMMANDS:
            command = self.command_by_name.get(name)
            if command is not None:
                self._run_command(command)

    def _start_runtime(self, generation: int) -> tuple[ManagedProcess, ...]:
        log_dir = self.run_dir / "logs" / f"generation-{generation:06d}"
        started: list[ManagedProcess] = []
        for name in _RUNTIME_ORDER:
            command = self.command_by_name.get(name)
            if command is None:
                continue
            started.append(
                self.supervisor.start(command, generation=generation, log_dir=log_dir)
            )
        if not any(item.name == "ns3-run" for item in started):
            raise ValueError("manifest does not define ns3-run")
        return tuple(started)

    def _run_command(self, command: dict[str, Any]) -> None:
        env = os.environ.copy()
        env.update({str(key): str(value) for key, value in (command.get("env") or {}).items()})
        self._runner(
            [str(item) for item in command["argv"]],
            cwd=str(command["cwd"]),
            env=env,
            check=True,
            text=True,
        )

    def _reset_artifacts(self) -> None:
        files = [
            self.manifest.get("snapshot_file"),
            self.manifest.get("clock_file"),
            self.manifest.get("state_db"),
            self.manifest.get("runtime_state_file"),
            self.manifest.get("result_file"),
            str(self.state_dir / "policy-acceptor-state.json"),
        ]
        clock_file = Path(str(self.manifest.get("clock_file") or ""))
        if str(clock_file):
            files.append(str(clock_file.parent / "real-ue-flows.jsonl"))
        state_db = Path(str(self.manifest.get("state_db") or ""))
        if str(state_db):
            files.extend((str(state_db) + "-wal", str(state_db) + "-shm"))

        gate_file = self.manifest.get("user_plane_gate_file")
        if gate_file:
            gate_payload = json.loads(Path(str(gate_file)).read_text(encoding="utf-8"))
            files.extend((gate_payload.get("event_log"), gate_payload.get("kpi_log")))
            for socket_key in ("socket_path", "authorization_socket"):
                socket_path = gate_payload.get(socket_key)
                if socket_path:
                    Path(str(socket_path)).unlink(missing_ok=True)

        for raw_path in files:
            if not raw_path:
                continue
            path = Path(str(raw_path)).expanduser().resolve()
            self._assert_run_owned(path)
            path.unlink(missing_ok=True)

        archive_root = Path(str(self.manifest["archive_dir"])).expanduser().resolve()
        archive_run = (archive_root / self.run_id).resolve()
        if archive_run.parent != archive_root or archive_run.name != self.run_id:
            raise ValueError(f"invalid run archive path: {archive_run}")
        if archive_run.exists():
            shutil.rmtree(archive_run)

    def _assert_run_owned(self, path: Path) -> None:
        if path == self.state_file or path == self.registry_file:
            return
        try:
            path.relative_to(self.run_dir)
        except ValueError as exc:
            raise ValueError(f"refusing to reset path outside run directory: {path}") from exc

    def _load_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {"run_id": self.run_id, "generation": 0, "initialized": False}
        return payload if isinstance(payload, dict) else {}

    def _save_state(self, result: ResetResult, *, initialized: bool) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            **asdict(result),
            "initialized": initialized,
            "manifest": str(self.manifest_path),
        }
        temporary = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.state_file)

    def _save_failure_state(self) -> None:
        previous = self._load_state()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        previous.update(
            {
                "run_id": self.run_id,
                "initialized": False,
                "manifest": str(self.manifest_path),
            }
        )
        temporary = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        temporary.write_text(json.dumps(previous, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.state_file)


class ResetApiServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        controller: FastResetController,
        *,
        token: str | None,
    ) -> None:
        super().__init__(address, ResetRequestHandler)
        self.controller = controller
        self.token = token


class ResetRequestHandler(BaseHTTPRequestHandler):
    server: ResetApiServer

    def do_GET(self) -> None:
        if self.path.rstrip("/") not in {"", "/health", "/v1/status"}:
            self._send_json(404, {"error": "not found"})
            return
        if not self._authorized():
            return
        status = self.server.controller.status()
        self._send_json(200 if status.get("healthy") else 503, status)

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/v1/reset":
            self._send_json(404, {"error": "not found"})
            return
        if not self._authorized():
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "invalid Content-Length"})
            return
        if length > 65536:
            self._send_json(413, {"error": "request body too large"})
            return
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            self._send_json(400, {"error": f"invalid JSON: {exc}"})
            return
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "request body must be an object"})
            return
        if "cold" in payload and not isinstance(payload["cold"], bool):
            self._send_json(400, {"error": "cold must be a boolean"})
            return
        try:
            result = self.server.controller.reset(cold=bool(payload.get("cold", False)))
        except ResetBusyError as exc:
            self._send_json(409, {"error": str(exc)})
            return
        except Exception as exc:
            self._send_json(500, {"error": str(exc)})
            return
        self._send_json(200, asdict(result))

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _authorized(self) -> bool:
        expected = self.server.token
        if expected is None:
            return True
        supplied = self.headers.get("Authorization", "")
        if not supplied.startswith("Bearer ") or not hmac.compare_digest(supplied[7:], expected):
            self._send_json(401, {"error": "unauthorized"})
            return False
        return True

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(args: argparse.Namespace) -> int:
    if args.host not in {"127.0.0.1", "::1", "localhost"} and not args.token:
        raise ValueError("a bearer token is required when reset API is not bound to loopback")
    controller = FastResetController(Path(args.manifest))
    if args.initialize != "none":
        controller.reset(cold=args.initialize == "cold")
    server = ResetApiServer((args.host, args.port), controller, token=args.token)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
        if not args.keep_running:
            controller.shutdown()
    return 0


def _client(args: argparse.Namespace, *, reset: bool) -> int:
    target = args.url.rstrip("/") + ("/v1/reset" if reset else "/v1/status")
    headers = {"Accept": "application/json"}
    data = None
    method = "GET"
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"
    if reset:
        headers["Content-Type"] = "application/json"
        data = json.dumps({"cold": args.cold}).encode("utf-8")
        method = "POST"
    http_request = request.Request(target, data=data, headers=headers, method=method)
    try:
        with request.urlopen(http_request, timeout=args.timeout) as response:
            print(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        print(exc.read().decode("utf-8"))
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast scenario reset supervisor")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the reset API and supervise an episode")
    serve.add_argument("manifest")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18081)
    serve.add_argument("--token")
    serve.add_argument("--initialize", choices=("auto", "cold", "none"), default="auto")
    serve.add_argument("--keep-running", action="store_true")
    serve.set_defaults(handler=_serve)

    for name, is_reset in (("reset", True), ("status", False)):
        client = subparsers.add_parser(name)
        client.add_argument("--url", default="http://127.0.0.1:18081")
        client.add_argument("--token")
        client.add_argument("--timeout", type=float, default=120.0)
        if is_reset:
            client.add_argument("--cold", action="store_true")
        client.set_defaults(handler=lambda args, reset=is_reset: _client(args, reset=reset))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
