from typing import Any

from norway_financial_bootstrap.brreg_client import (
    DEFAULT_USER_AGENT,
    BrregFinancialClient,
)
from norway_financial_bootstrap.candidates import FinancialCandidate


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, text: str) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse | Exception]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: int) -> FakeResponse:
        self.calls.append(url)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_brreg_client_default_user_agent_matches_bootstrap_spec() -> None:
    assert DEFAULT_USER_AGENT == "corpscout-norway-financial-bootstrap/0.1"


def test_brreg_client_maps_success_to_existing_fetch_row_contract() -> None:
    session = FakeSession([FakeResponse(200, [{"id": 1}], '[{"id":1}]')])
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "SUCCESS AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert session.calls == [
        "https://data.brreg.no/regnskapsregisteret/regnskap/811685852"
    ]
    assert row["fetch_status"] == "success"
    assert row["attempt_count"] == 1
    assert row["raw_response"] == '[{"id":1}]'


def test_brreg_client_defaults_fetched_at_to_utc_timestamp() -> None:
    session = FakeSession([FakeResponse(200, [{"id": 1}], '[{"id":1}]')])
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "SUCCESS AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
    )

    assert row["fetch_status"] == "success"
    assert row["fetched_at"]
    assert row["fetched_at"].endswith("Z")


def test_brreg_client_does_not_retry_404() -> None:
    session = FakeSession([FakeResponse(404, {"message": "missing"}, "missing")])
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "MISSING AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert row["fetch_status"] == "not_found"
    assert row["attempt_count"] == 1


def test_brreg_client_retries_500_then_records_success_attempt_count() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        [
            FakeResponse(500, {"message": "server"}, "server"),
            FakeResponse(200, [{"id": 1}], '[{"id":1}]'),
        ]
    )
    client = BrregFinancialClient(session=session, sleep=sleeps.append)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "SUCCESS AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert row["fetch_status"] == "success"
    assert row["attempt_count"] == 2
    assert sleeps == [30.0]


def test_brreg_client_maps_json_parse_failure_to_invalid_payload() -> None:
    session = FakeSession([FakeResponse(200, ValueError("bad json"), "not json")])
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "BROKEN AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert row["fetch_status"] == "invalid_payload"
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "bad json"
    assert row["raw_response"] == "not json"
