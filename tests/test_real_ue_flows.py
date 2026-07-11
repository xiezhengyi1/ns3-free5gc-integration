from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_real_ue_flows.py"
SPEC = importlib.util.spec_from_file_location("run_real_ue_flows", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RealUeFlowsTest(unittest.TestCase):
    def test_authorization_ledger_is_idempotent_after_success(self) -> None:
        ledger = MODULE._AuthorizationLedger()

        self.assertTrue(ledger.should_dispatch(41))
        ledger.complete(41)

        self.assertFalse(ledger.should_dispatch(41))

    def test_parser_requires_authorization_socket(self) -> None:
        parser = MODULE._build_parser()

        args = parser.parse_args(
            [
                "--flow-profile-file",
                "flows.tsv",
                "--state-file",
                "state.jsonl",
                "--target-ip",
                "192.0.2.1",
                "--upf-container",
                "upf",
                "--authorization-socket",
                "/tmp/authorize.sock",
                "ue1=ue-container",
            ]
        )

        self.assertEqual(args.authorization_socket, "/tmp/authorize.sock")

    def test_select_interface_for_session_uses_requested_index_when_available(self) -> None:
        selected = MODULE._select_interface_for_session(
            [["uesimtun0", "10.0.0.1"], ["uesimtun1", "10.0.0.2"]],
            1,
        )

        self.assertEqual(selected, ["uesimtun1", "10.0.0.2"])

    def test_select_interface_for_session_returns_none_when_requested_tunnel_is_missing(self) -> None:
        selected = MODULE._select_interface_for_session(
            [["uesimtun0", "10.0.0.1"]],
            1,
        )

        self.assertIsNone(selected)

    def test_resolve_ue_interface_returns_none_when_no_tunnel_is_available(self) -> None:
        with mock.patch.object(MODULE, "_list_ue_interfaces", return_value=[]):
            resolved = MODULE._resolve_ue_interface("ue-ue1", 0)

        self.assertIsNone(resolved)

    def test_resolve_ue_interface_treats_unready_container_as_not_ready(self) -> None:
        with mock.patch.object(
            MODULE,
            "_list_ue_interfaces",
            side_effect=MODULE.subprocess.CalledProcessError(1, ["docker", "exec"]),
        ):
            resolved = MODULE._resolve_ue_interface("ue-ue1", 0)

        self.assertIsNone(resolved)

    def test_resolve_downlink_route_skips_unready_upf_container(self) -> None:
        with mock.patch.object(
            MODULE.subprocess,
            "check_output",
            side_effect=MODULE.subprocess.CalledProcessError(1, ["docker", "exec"]),
        ):
            resolved = MODULE._resolve_downlink_route(["upf-1"], "10.60.0.1")

        self.assertIsNone(resolved)

    def test_resolve_ue_interface_returns_requested_tunnel_when_available(self) -> None:
        with mock.patch.object(
            MODULE,
            "_list_ue_interfaces",
            return_value=[["uesimtun0", "10.0.0.1"], ["uesimtun1", "10.0.0.2"]],
        ):
            resolved = MODULE._resolve_ue_interface("ue-ue2", 1)

        self.assertEqual(
            resolved,
            {"iface": "uesimtun1", "ip": "10.0.0.2"},
        )

    def test_controlled_sender_argv_carries_experiment_identity(self) -> None:
        argv = MODULE._controlled_sender_argv(
            sender="/tmp/sender",
            target_ip="192.0.2.1",
            destination_port=5000,
            source_port=15000,
            interface="uesimtun0",
            packet_size=256,
            flow_id="flow-1",
            epoch_id=7,
            application_sequence=9,
        )

        self.assertEqual(
            argv,
            [
                "/tmp/sender",
                "192.0.2.1",
                "5000",
                "15000",
                "uesimtun0",
                "256",
                "1",
                "flow-1",
                "7",
                "9",
            ],
        )
