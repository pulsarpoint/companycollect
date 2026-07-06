import json
from typing import Any

import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches


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


def test_financial_fetches_parquet_schema_is_canonical() -> None:
    schema = financial_fetches.financial_fetches_parquet_schema()

    assert list(schema) == list(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS)
    assert schema["org_number"] == pl.Utf8
    assert schema["source_line_number"] == pl.Int64
    assert schema["fetched_at"] == financial_fetches.FINANCIAL_FETCHED_AT_DTYPE


def test_financial_fetch_column_type_mapping_rejects_unsupported_types() -> None:
    assert (
        financial_fetches.polars_type_for_financial_fetch_column({"data_type": "text"})
        == pl.Utf8
    )
    assert (
        financial_fetches.polars_type_for_financial_fetch_column(
            {"data_type": "bigint"}
        )
        == pl.Int64
    )
    assert (
        financial_fetches.polars_type_for_financial_fetch_column(
            {"data_type": "timestamp"}
        )
        == financial_fetches.FINANCIAL_FETCHED_AT_DTYPE
    )
    with pytest.raises(
        ValueError,
        match="Unsupported Norway Brreg financial fetch column type",
    ):
        financial_fetches.polars_type_for_financial_fetch_column({"data_type": "json"})


def test_snapshot_financial_candidates_use_active_companies_without_website_filter() -> None:
    frame = pl.DataFrame(
        [
            {
                "org_number": "2",
                "name": "Active Website",
                "is_active": True,
                "primary_website_url": "https://x.no",
                "last_submitted_accounts_year": "2025",
            },
            {
                "org_number": "1",
                "name": "Active No Website",
                "is_active": True,
                "primary_website_url": None,
                "last_submitted_accounts_year": "2025",
            },
            {
                "org_number": "3",
                "name": "Inactive",
                "is_active": False,
                "primary_website_url": "https://y.no",
                "last_submitted_accounts_year": "2025",
            },
            {
                "org_number": "4",
                "name": "No Accounts",
                "is_active": True,
                "primary_website_url": None,
                "last_submitted_accounts_year": None,
            },
        ]
    )

    rows = financial_fetches.financial_fetch_candidates_from_no_companies(frame)

    assert [row["org_number"] for row in rows] == ["1", "2"]


def test_transport_failure_status_is_retryable_failure_for_downstream_guard() -> None:
    row = financial_fetches.financial_fetch_failure_row(
        org={"org_number": "999", "legal_name": "Broken AS"},
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/999",
        source_run_id="run-1",
        source_line_number=1,
        status_code=500,
        fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-06-30T00:00:00.000Z",
        attempt_count=5,
        raw_response="server error",
    )

    assert row["fetch_status"] == "server_error"
    for fetch_status in ("success", "not_found", "unsupported_layout", "gone", "empty"):
        assert financial_fetches.financial_fetch_status_requires_failure(fetch_status) is False
    for fetch_status in ("server_error", "network_error", "invalid_payload"):
        assert financial_fetches.financial_fetch_status_requires_failure(fetch_status) is True


def test_unsupported_statement_layout_500_is_terminal_source_outcome() -> None:
    org_url = "https://data.brreg.no/regnskapsregisteret/regnskap/894924322"
    body = (
        '{"timestamp":"2026-07-06T15:57:03.777+0000","status":"500",'
        '"error":"Internal Server Error","message":"Regnskapet inneholder en '
        'oppstillingsplan som ikke er stottet (IDEELL)",'
        '"path":"/regnskapsregisteret/regnskap/894924322"}'
    )
    client = FakeDltRequestsClient(
        {org_url: FakeResponse(status_code=500, text=body)}
    )

    [row] = financial_fetches.fetch_financial_rows_for_orgs(
        orgs=[{"org_number": "894924322", "legal_name": "IDEELL ORG"}],
        source_run_id="run-1",
        client=client,
    )

    assert row["fetch_status"] == "unsupported_layout"
    assert row["http_status"] == 500
    assert row["error_type"] == "UnsupportedStatementLayout"
    assert row["raw_response"] == body
    assert financial_fetches.financial_fetch_status_requires_failure(row["fetch_status"]) is False


def test_plain_500_without_layout_marker_stays_retryable_server_error() -> None:
    org_url = "https://data.brreg.no/regnskapsregisteret/regnskap/814115232"
    client = FakeDltRequestsClient(
        {org_url: FakeResponse(status_code=500, text="Internal Server Error")}
    )

    [row] = financial_fetches.fetch_financial_rows_for_orgs(
        orgs=[{"org_number": "814115232", "legal_name": "SERVER AS"}],
        source_run_id="run-1",
        client=client,
    )

    assert row["fetch_status"] == "server_error"
    assert financial_fetches.financial_fetch_status_requires_failure(row["fetch_status"]) is True


def test_fetch_financial_rows_for_orgs_fetches_supplied_orgs_without_duckdb() -> None:
    client = FakeDltRequestsClient(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": FakeResponse(
                status_code=200,
                payload=[{"regnskapsperiode": {"fraDato": "2024-01-01"}}],
                text='[{"regnskapsperiode":{"fraDato":"2024-01-01"}}]',
            ),
            "https://data.brreg.no/regnskapsregisteret/regnskap/814115232": FakeResponse(
                status_code=500,
                payload={"message": "server error"},
                text="server error",
            ),
        }
    )

    rows = financial_fetches.fetch_financial_rows_for_orgs(
        orgs=[
            {
                "org_number": "811685852",
                "legal_name": "SUCCESS AS",
                "website": "",
                "last_submitted_accounts_year": "2024",
            },
            {
                "org_number": "814115232",
                "legal_name": "SERVER AS",
                "website": "",
                "last_submitted_accounts_year": "2024",
            },
        ],
        source_run_id="run-1",
        timeout_seconds=120,
        fetched_at="2026-06-30T00:00:00.000Z",
        client=client,
    )

    assert [url for url, _timeout in client.calls] == [
        "https://data.brreg.no/regnskapsregisteret/regnskap/811685852",
        "https://data.brreg.no/regnskapsregisteret/regnskap/814115232",
    ]
    assert [row["fetch_status"] for row in rows] == ["success", "server_error"]
    assert [row["source_line_number"] for row in rows] == [1, 2]
    assert rows[0]["raw_response"] == '[{"regnskapsperiode":{"fraDato":"2024-01-01"}}]'
    assert rows[1]["raw_response"] == "server error"


def test_fetch_financial_rows_for_orgs_accepts_log_and_reports_status_counts() -> None:
    client = FakeDltRequestsClient(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": FakeResponse(
                status_code=200,
                payload=[{"id": 1}],
                text='[{"id":1}]',
            ),
        }
    )
    messages: list[tuple[str, tuple[Any, ...]]] = []

    rows = financial_fetches.fetch_financial_rows_for_orgs(
        orgs=[
            {
                "org_number": "811685852",
                "legal_name": "SUCCESS AS",
                "website": "",
                "last_submitted_accounts_year": "2024",
            },
        ],
        source_run_id="run-1",
        timeout_seconds=120,
        fetched_at="2026-06-30T00:00:00.000Z",
        client=client,
        log=lambda message, *args: messages.append((message, args)),
    )

    assert [row["fetch_status"] for row in rows] == ["success"]
    assert messages == [
        ("Preparing Norway Brreg financial row fetches: candidates=%s", (1,)),
        (
            "Completed Norway Brreg financial row fetches: fetched=%s statuses=%s",
            (1, {"success": 1}),
        ),
    ]
