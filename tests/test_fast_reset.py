from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib import error, request

from bridge.orchestrator.fast_reset import (
    FastResetController,
    ManagedProcess,
    ResetApiServer,
    ResetResult,
)


class _FakeSupervisor:
    def __init__(self) -> None:
        self.stop_count = 0
        self.started: list[ManagedProcess] = []
        self.next_pid = 1000

    def stop_all(self, **_kwargs: object) -> None:
        self.stop_count += 1
        self.started.clear()

    def start(
        self,
        command: dict[str, object],
        *,
        generation: int,
        log_dir: Path,
    ) -> ManagedProcess:
        self.next_pid += 1
        process = ManagedProcess(
            name=str(command["name"]),
            pid=self.next_pid,
            generation=generation,
            argv=tuple(str(item) for item in command["argv"]),
            log_file=str(log_dir / f"{command['name']}.log"),
        )
        self.started.append(process)
        return process

    def status(self) -> tuple[ManagedProcess, ...]:
        return tuple(self.started)


class _Monotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        self.value += 0.01
        return self.value


def _command(name: str, root: Path, *, background: bool = False) -> dict[str, object]:
    return {
        "name": name,
        "cwd": str(root),
        "argv": ["tool", name, str(root)],
        "background": background,
        "env": {},
    }


class FastResetControllerTest(unittest.TestCase):
    def _write_manifest(
        self,
        root: Path,
        *,
        snapshot_file: Path | None = None,
        split: bool = False,
    ) -> Path:
        run_dir = root / "artifacts" / "runs" / "reset-run"
        generated = run_dir / "generated"
        ns3_dir = generated / "ns3"
        state_dir = run_dir / "state"
        for directory in (generated, ns3_dir, state_dir):
            directory.mkdir(parents=True, exist_ok=True)
        compose_file = generated / "compose.yaml"
        compose_file.write_text("services: {}\n", encoding="utf-8")
        gate_file = generated / "user-plane-gate.json"
        gate_file.write_text(
            json.dumps(
                {
                    "socket_path": str(run_dir / "gate.sock"),
                    "authorization_socket": str(run_dir / "gate.sock.agents"),
                    "event_log": str(ns3_dir / "packet-events.jsonl"),
                    "kpi_log": str(ns3_dir / "packet-kpis.jsonl"),
                }
            ),
            encoding="utf-8",
        )
        commands = [
            _command("compose-up-core", root),
            _command("bootstrap-subscribers", root),
            _command("bootstrap-app-data", root),
            _command("compose-up-upf", root),
            _command("compose-up-gnb", root),
            _command("bridge-setup", root),
            _command("compose-up-smf", root),
            *([_command("wait-for-pfcp-ready", root)] if split else []),
            _command("compose-up-ue", root),
            _command("writer-follow-ns3", root, background=True),
            _command("policy-acceptor", root, background=True),
            _command("user-plane-gate", root, background=True),
            _command("real-ue-flows", root, background=True),
            _command("bridge-probe-post-ns3", root, background=True),
            _command("ns3-build", root),
            _command("ns3-run", root),
            _command("compose-down", root),
        ]
        manifest = {
            "run_id": "reset-run",
            "run_dir": str(run_dir),
            "compose_file": str(compose_file),
            "compose_project_name": "reset-project",
            "core_services": ["mongodb", "free5gc-amf", "free5gc-smf"],
            "ran_services": ["free5gc-upf", "ueransim", "ue-1"],
            "snapshot_file": str(snapshot_file or ns3_dir / "snapshots.jsonl"),
            "clock_file": str(ns3_dir / "clock.json"),
            "state_db": str(state_dir / "writer.db"),
            "archive_dir": str(run_dir / "archive"),
            "user_plane_gate_file": str(gate_file),
            "commands": commands,
        }
        if split:
            manifest["runtime_state_file"] = str(state_dir / "split-runtime.json")
            manifest["result_file"] = str(run_dir / "split-result.json")
            manifest["user_plane_gate_file"] = None
            replacements = {
                "writer-follow-ns3": "writer-follow-split-ns3",
                "user-plane-gate": "split-gate",
                "real-ue-flows": "split-results",
            }
            for command in commands:
                name = str(command["name"])
                if name in replacements:
                    command["name"] = replacements[name]
                    command["argv"][1] = replacements[name]
        manifest_file = run_dir / "run-manifest.json"
        manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
        return manifest_file

    def test_cold_start_then_fast_reset_reuses_compose_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_file = self._write_manifest(root)
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            stale_files = [
                Path(manifest["snapshot_file"]),
                Path(manifest["clock_file"]),
                Path(manifest["state_db"]),
                Path(manifest["run_dir"]) / "generated" / "ns3" / "packet-events.jsonl",
            ]
            for path in stale_files:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("stale\n", encoding="utf-8")
            supervisor = _FakeSupervisor()
            calls: list[tuple[list[str], dict[str, object]]] = []

            def runner(argv: list[str], **kwargs: object) -> None:
                calls.append((argv, kwargs))

            controller = FastResetController(
                manifest_file,
                supervisor=supervisor,
                command_runner=runner,
                monotonic=_Monotonic(),
            )

            cold = controller.reset()

            self.assertEqual(cold.mode, "cold")
            self.assertEqual(cold.generation, 1)
            self.assertNotIn("restart", [item for argv, _ in calls for item in argv])
            self.assertTrue(all(not path.exists() for path in stale_files))
            self.assertEqual(
                [process.name for process in cold.processes],
                [
                    "writer-follow-ns3",
                    "policy-acceptor",
                    "user-plane-gate",
                    "real-ue-flows",
                    "bridge-probe-post-ns3",
                    "ns3-run",
                ],
            )

            calls.clear()
            fast = controller.reset()

            self.assertEqual(fast.mode, "fast")
            self.assertEqual(fast.generation, 2)
            self.assertLessEqual(fast.duration_ms, 20)
            self.assertEqual(supervisor.stop_count, 2)
            self.assertEqual(calls[0][0][0:2], ["docker", "compose"])
            self.assertIn("restart", calls[0][0])
            self.assertNotIn("ns3-build", [item for argv, _ in calls for item in argv])
            self.assertEqual(
                [argv[1] for argv, _ in calls[1:]],
                ["bootstrap-subscribers", "bootstrap-app-data", "bridge-setup"],
            )
            self.assertEqual(
                fast.restarted_services,
                ("mongodb", "free5gc-amf", "free5gc-smf", "free5gc-upf", "ueransim", "ue-1"),
            )
            state = json.loads(controller.state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["generation"], 2)
            self.assertTrue(state["initialized"])
            self.assertEqual(controller.status()["processes"], [asdict(item) for item in fast.processes])

    def test_split_manifest_resets_split_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_file = self._write_manifest(root, split=True)
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            for key in ("runtime_state_file", "result_file"):
                path = Path(manifest[key])
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("stale\n", encoding="utf-8")
            supervisor = _FakeSupervisor()
            calls: list[list[str]] = []
            controller = FastResetController(
                manifest_file,
                supervisor=supervisor,
                command_runner=lambda argv, **_kwargs: calls.append(argv),
            )

            cold = controller.reset()
            fast = controller.reset()

            self.assertEqual(fast.mode, "fast")
            self.assertTrue(
                all(not Path(manifest[key]).exists() for key in ("runtime_state_file", "result_file"))
            )
            self.assertEqual(
                [process.name for process in cold.processes],
                [
                    "writer-follow-split-ns3",
                    "policy-acceptor",
                    "split-gate",
                    "split-results",
                    "bridge-probe-post-ns3",
                    "ns3-run",
                ],
            )
            self.assertIn("wait-for-pfcp-ready", [argv[1] for argv in calls if len(argv) > 1])

    def test_refuses_to_delete_artifact_outside_run_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root / "outside.jsonl"
            outside.write_text("keep\n", encoding="utf-8")
            manifest_file = self._write_manifest(root, snapshot_file=outside)
            supervisor = _FakeSupervisor()
            controller = FastResetController(
                manifest_file,
                supervisor=supervisor,
                command_runner=lambda *_args, **_kwargs: None,
            )

            with self.assertRaisesRegex(ValueError, "outside run directory"):
                controller.reset()

            self.assertEqual(outside.read_text(encoding="utf-8"), "keep\n")
            self.assertFalse(controller.status().get("initialized", False))


class _ApiController:
    def __init__(self) -> None:
        self.cold_values: list[bool] = []

    def reset(self, *, cold: bool = False) -> ResetResult:
        self.cold_values.append(cold)
        return ResetResult(
            run_id="api-run",
            generation=3,
            mode="cold" if cold else "fast",
            duration_ms=7,
            restarted_services=("amf", "smf"),
            processes=(),
        )

    def status(self) -> dict[str, object]:
        return {"run_id": "api-run", "generation": 2, "healthy": True}


class ResetApiTest(unittest.TestCase):
    def test_token_protected_reset_endpoint(self) -> None:
        controller = _ApiController()
        server = ResetApiServer(("127.0.0.1", 0), controller, token="secret")  # type: ignore[arg-type]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            unauthorized = request.Request(
                f"{base_url}/v1/reset",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(error.HTTPError) as raised:
                request.urlopen(unauthorized, timeout=2)
            self.assertEqual(raised.exception.code, 401)
            raised.exception.close()

            authorized = request.Request(
                f"{base_url}/v1/reset",
                data=b'{"cold":true}',
                headers={
                    "Authorization": "Bearer secret",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with request.urlopen(authorized, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))

            self.assertEqual(payload["generation"], 3)
            self.assertEqual(payload["mode"], "cold")
            self.assertEqual(controller.cold_values, [True])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
