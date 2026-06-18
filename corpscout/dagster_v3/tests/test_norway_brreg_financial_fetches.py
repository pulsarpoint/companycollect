import json
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.norway_brreg import financial_fetches


class FakeResponse:
    def __init__(self, *, status_code: int, payload: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> Any:
        return self._payload


class FakeDltRequestsClient:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        self.calls.append((url, timeout))
        if url == "https://data.brreg.no/regnskapsregisteret/regnskap/network":
            raise OSError("network down")
        return self.responses[url]


class InterruptingClient(FakeDltRequestsClient):
    def __init__(self, responses: dict[str, FakeResponse], *, interrupt_after: int) -> None:
        super().__init__(responses)
        self.interrupt_after = interrupt_after

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        if len(self.calls) >= self.interrupt_after:
            raise KeyboardInterrupt
        return super().get(url, timeout=timeout)


def test_financial_fetch_status_values_are_explicit() -> None:
    assert financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS == "success"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_NOT_FOUND == "not_found"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR == "server_error"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_NETWORK_ERROR == "network_error"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD == "invalid_payload"


def test_financial_fetches_schema_matches_emitted_success_row() -> None:
    row = financial_fetches.financial_fetch_success_row(
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
        payload=[{"id": 1}],
        source_run_id="run-1",
        source_line_number=1,
        status_code=200,
        fetched_at="2026-06-17T00:00:00.000Z",
        attempt_count=1,
    )

    assert set(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS) == set(row)
    assert row["fetch_status"] == "success"
    assert row["http_status"] == 200
    assert row["source_record_id"] == "923609016"
    assert json.loads(row["raw_response"]) == [{"id": 1}]
    assert len(row["source_payload_hash"]) == 64


def test_financial_fetches_schema_matches_emitted_failure_row() -> None:
    row = financial_fetches.financial_fetch_failure_row(
        org={
            "org_number": "814115232",
            "legal_name": "BROKEN AS",
            "website": "https://example.test",
            "last_submitted_accounts_year": "2024",
        },
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/814115232",
        source_run_id="run-1",
        source_line_number=2,
        status_code=500,
        fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-06-17T00:00:00.000Z",
        attempt_count=3,
        raw_response="",
    )

    assert set(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS) == set(row)
    assert row["fetch_status"] == "server_error"
    assert row["http_status"] == 500
    assert row["raw_response"] == ""
    assert row["source_payload_hash"] == "0" * 64


def test_financial_fetches_table_schema_is_explicit() -> None:
    assert financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS["org_number"] == {
        "data_type": "text",
        "nullable": False,
    }
    assert financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS["raw_response"] == {
        "data_type": "text"
    }
    assert financial_fetches.FINANCIAL_FETCHES_TABLE == "financial_fetches"


def test_resumable_financial_fetches_persist_completed_rows_before_interrupt(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "norway.duckdb"
    _seed_entities(database_path)
    client = InterruptingClient(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": FakeResponse(
                status_code=404,
                payload={"message": "not found"},
                text='{"message":"not found"}',
            ),
        },
        interrupt_after=1,
    )

    try:
        financial_fetches.run_brreg_financial_statement_fetches(
            database_path=database_path,
            base_url="https://data.brreg.no/regnskapsregisteret/regnskap",
            source_run_id="run-1",
            timeout_seconds=120,
            user_agent="test-agent",
            fetched_at="2026-06-17T00:00:00.000Z",
            client=client,
            commit_every_rows=1,
        )
    except KeyboardInterrupt:
        pass

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select org_number, fetch_status, http_status
            from norway_brreg.financial_fetches
            order by org_number
            """
        ).fetchall()

    assert rows == [("811685852", "not_found", 404)]


def test_resumable_financial_fetches_skip_existing_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    _seed_entities(database_path)
    client = FakeDltRequestsClient(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": FakeResponse(
                status_code=404,
                payload={"message": "not found"},
                text='{"message":"not found"}',
            ),
        }
    )
    financial_fetches.run_brreg_financial_statement_fetches(
        database_path=database_path,
        base_url="https://data.brreg.no/regnskapsregisteret/regnskap",
        source_run_id="run-1",
        timeout_seconds=120,
        user_agent="test-agent",
        fetched_at="2026-06-17T00:00:00.000Z",
        client=client,
        commit_every_rows=1,
    )
    assert [url for url, _timeout in client.calls] == [
        "https://data.brreg.no/regnskapsregisteret/regnskap/811685852",
        "https://data.brreg.no/regnskapsregisteret/regnskap/814115232",
        "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
        "https://data.brreg.no/regnskapsregisteret/regnskap/network",
    ]

    resume_client = FakeDltRequestsClient({})
    counts = financial_fetches.run_brreg_financial_statement_fetches(
        database_path=database_path,
        base_url="https://data.brreg.no/regnskapsregisteret/regnskap",
        source_run_id="run-2",
        timeout_seconds=120,
        user_agent="test-agent",
        fetched_at="2026-06-17T00:00:00.000Z",
        client=resume_client,
        commit_every_rows=1,
    )

    assert resume_client.calls == []
    assert counts["total_candidates"] == 4
    assert counts["skipped_existing"] == 4
    assert counts["fetched"] == 0


def _seed_entities(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema if not exists norway_brreg")
        connection.execute(
            """
            create or replace table norway_brreg.entities as
            select *
            from (
                values
                    ('923609016', 'EQUINOR ASA', 'www.equinor.com', '2024', true),
                    ('811685852', 'MISSING AS', 'www.missing.test', '2024', true),
                    ('814115232', 'SERVER AS', 'www.server.test', '2024', true),
                    ('network', 'NETWORK AS', 'www.network.test', '2024', true)
            ) as t(org_number, legal_name, website, last_submitted_accounts_year, is_active)
            """
        )
