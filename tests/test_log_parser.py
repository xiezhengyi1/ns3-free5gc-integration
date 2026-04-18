from __future__ import annotations

import unittest

from bridge.writer.log_parser import parse_free5gc_compose_line, parse_ueransim_compose_line


class LogParserTest(unittest.TestCase):
    def test_parses_free5gc_smf_pdu_session_event(self) -> None:
        events = parse_free5gc_compose_line(
            "free5gc-smf  | 2026-04-17T12:00:00Z [INFO][SMF][PduSess] Receive Create SM Context Request\n",
            run_id="run-1",
            scenario_id="scenario-1",
            tick_index=3,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "free5gc.pdu_session_create_request")
        self.assertEqual(events[0].entity_type, "pdu_session")
        self.assertEqual(events[0].tick_index, 3)

    def test_parses_ueransim_registration_success(self) -> None:
        events = parse_ueransim_compose_line(
            "ue-ue1  | [2026-04-17 12:00:00.000] [nas] [info] Initial Registration is successful\n",
            run_id="run-1",
            scenario_id="scenario-1",
            tick_index=5,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "ueransim.registration_success")
        self.assertEqual(events[0].entity_type, "ue")
        self.assertEqual(events[0].entity_id, "ue1")

    def test_parses_ueransim_pdu_session_success(self) -> None:
        events = parse_ueransim_compose_line(
            "ue-ue1  | [2026-04-17 12:00:00.000] [nas] [info] PDU Session establishment is successful PSI[1]\n",
            run_id="run-1",
            scenario_id="scenario-1",
            tick_index=6,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "ueransim.pdu_session_established")
        self.assertEqual(events[0].entity_type, "pdu_session")
        self.assertEqual(events[0].entity_id, "ue1:psi-1")

    def test_parses_ueransim_registration_failure(self) -> None:
        events = parse_ueransim_compose_line(
            "ue-ue1  | [2026-04-17 12:00:00.000] [nas] [error] Initial Registration failed [CONGESTION]\n",
            run_id="run-1",
            scenario_id="scenario-1",
            tick_index=7,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "ueransim.registration_failure")
        self.assertEqual(events[0].entity_id, "ue1")

    def test_parses_free5gc_auth_backend_error(self) -> None:
        events = parse_free5gc_compose_line(
            "ausf  | 2026-04-17T07:14:36.364337845Z [INFO][AUSF][UeAuth] GenerateAuthDataApi error: json: cannot unmarshal string into Go value of type models.ProblemDetails\n",
            run_id="run-1",
            scenario_id="scenario-1",
            tick_index=8,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "free5gc.authentication_backend_error")
        self.assertEqual(events[0].entity_type, "nf")
        self.assertEqual(events[0].entity_id, "ausf")


if __name__ == "__main__":
    unittest.main()