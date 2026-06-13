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
    def test_parser_accepts_controlled_mode(self) -> None:
        parser = MODULE._build_parser()

        args = parser.parse_args(
            [
                "--flow-profile-file",
                "flows.tsv",
                "--clock-file",
                "clock.json",
                "--state-file",
                "state.jsonl",
                "--run-id",
                "run-1",
                "--scenario-id",
                "scenario-1",
                "--target-ip",
                "192.0.2.1",
                "--upf-container",
                "upf",
                "--controlled",
                "--authorization-socket",
                "/tmp/authorize.sock",
                "ue1=ue-container",
            ]
        )

        self.assertTrue(args.controlled)
        self.assertEqual(args.authorization_socket, "/tmp/authorize.sock")

    def test_effective_tick_window_uses_nominal_window_when_starting(self) -> None:
        elapsed_ms, skipped_ticks = MODULE._effective_tick_window_ms(
            last_tick=None,
            last_sim_time_ms=None,
            tick_index=0,
            sim_time_ms=100,
            nominal_tick_ms=100,
        )

        self.assertEqual(elapsed_ms, 100)
        self.assertEqual(skipped_ticks, 0)

    def test_effective_tick_window_drops_backlog_when_ticks_were_skipped(self) -> None:
        elapsed_ms, skipped_ticks = MODULE._effective_tick_window_ms(
            last_tick=0,
            last_sim_time_ms=100,
            tick_index=16,
            sim_time_ms=1700,
            nominal_tick_ms=100,
        )

        self.assertEqual(elapsed_ms, 100)
        self.assertEqual(skipped_ticks, 15)

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
