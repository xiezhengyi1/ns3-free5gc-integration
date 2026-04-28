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
    def test_select_interface_for_session_uses_requested_index_when_available(self) -> None:
        selected, used_fallback = MODULE._select_interface_for_session(
            [["uesimtun0", "10.0.0.1"], ["uesimtun1", "10.0.0.2"]],
            1,
        )

        self.assertEqual(selected, ["uesimtun1", "10.0.0.2"])
        self.assertFalse(used_fallback)

    def test_select_interface_for_session_falls_back_when_only_one_tunnel_exists(self) -> None:
        selected, used_fallback = MODULE._select_interface_for_session(
            [["uesimtun0", "10.0.0.1"]],
            1,
        )

        self.assertEqual(selected, ["uesimtun0", "10.0.0.1"])
        self.assertTrue(used_fallback)

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
            ({"iface": "uesimtun1", "ip": "10.0.0.2"}, False),
        )