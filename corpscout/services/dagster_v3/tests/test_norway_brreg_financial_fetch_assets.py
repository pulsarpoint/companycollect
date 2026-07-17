from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.assets import financial_fetches as assets
from dagster_v3.defs.norway_brreg_financial.assets.financial_fetches import (
    norway_brreg_financial_responses_updates_json,
    norway_brreg_financial_responses_updates_parquet,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    financial_update_response_index_object_key,
    financial_update_response_partition_prefix,
)


class FakeClickhouseClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def execute(self, query: str) -> list[tuple[Any, ...]]:
        self.queries.append(query)
        return self.rows


class FakeClickhouseConnection:
    def __init__(self, client: FakeClickhouseClient) -> None:
        self.client = client

    def __enter__(self) -> FakeClickhouseClient:
        return self.client

    def __exit__(self, *args: Any) -> None:
        return None


class FakeClickhouseResource:
    def __init__(self, client: FakeClickhouseClient) -> None:
        self.client = client

    def get_connection(self) -> FakeClickhouseConnection:
        return FakeClickhouseConnection(self.client)


class FakeFinancialStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, pl.DataFrame]] = []

    def write_update_response_index(
        self,
        partition_date: str,
        frame: pl.DataFrame,
    ) -> str:
        self.writes.append((partition_date, frame))
        return financial_update_response_index_object_key(partition_date)


def test_update_asset_dependencies_form_json_then_parquet_graph() -> None:
    json_key = dg.AssetKey("norway_brreg_financial_responses_updates_json")
    parquet_key = dg.AssetKey("norway_brreg_financial_responses_updates_parquet")

    assert norway_brreg_financial_responses_updates_json.asset_deps[json_key] == set()
    assert json_key in (
        norway_brreg_financial_responses_updates_parquet.asset_deps[parquet_key]
    )


def test_update_json_reads_candidates_and_calls_json_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clickhouse_client = FakeClickhouseClient(
        [("923609016", "EQUINOR ASA", "https://www.equinor.com", "2024")]
    )
    captured: dict[str, Any] = {}

    def fake_materialize(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "candidate_count": 1,
            "reused_count": 0,
            "downloaded_count": 1,
            "status_counts": {"success": 1},
            "partition_prefix": kwargs["partition_prefix"],
        }

    monkeypatch.setattr(assets, "materialize_response_json_partition", fake_materialize)

    result = norway_brreg_financial_responses_updates_json(
        context=dg.build_asset_context(partition_key="2026-07-16"),
        clickhouse=FakeClickhouseResource(clickhouse_client),
        norway_brreg_financial_storage=object(),
    )

    [query] = clickhouse_client.queries
    assert "FROM no_companies AS company" in query
    assert "FROM no_financial_statements" in query
    assert "latest_financial_year" in query
    assert captured["candidates"] == [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "https://www.equinor.com",
            "last_submitted_accounts_year": "2024",
        }
    ]
    assert captured["partition_prefix"] == (
        "norway_brreg/financial/responses/updates/date=2026-07-16/"
    )
    assert result.metadata["downloaded_count"] == 1


def test_update_candidate_query_uses_canonical_company_state() -> None:
    assert "norway_brreg_entity_updates_no_companies_parquet" not in (
        assets.UPDATE_CANDIDATES_SQL
    )
    assert "company.is_active" in assets.UPDATE_CANDIDATES_SQL
    assert "company.last_submitted_accounts_year" in assets.UPDATE_CANDIDATES_SQL


def test_update_parquet_writes_verified_metadata_without_raw_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeFinancialStorage()
    frame = financial_fetches.financial_fetches_frame(
        [
            financial_fetches.response_record(
                org={"org_number": "923609016"},
                source_url="https://example.test/923609016",
                source_run_id="run-1",
                source_line_number=1,
                fetch_status="success",
                http_status=200,
                error_type="",
                error_message="",
                attempt_count=1,
                fetched_at="2026-07-17T00:00:00.000Z",
                source_object_key="responses/org=923609016/response.json",
                source_payload_hash="a" * 64,
            )
        ]
    )
    monkeypatch.setattr(
        assets,
        "verified_response_index_frame",
        lambda **kwargs: (
            frame,
            {
                "candidate_count": 1,
                "row_count": 1,
                "status_counts": {"success": 1},
                "success_manifest_key": f"{kwargs['partition_prefix']}_SUCCESS.json",
            },
        ),
    )

    result = norway_brreg_financial_responses_updates_parquet(
        context=dg.build_asset_context(partition_key="2026-07-16"),
        norway_brreg_financial_storage=storage,
    )

    [(partition_date, written_frame)] = storage.writes
    assert partition_date == "2026-07-16"
    assert "raw_response" not in written_frame.columns
    assert result.metadata["s3_key"] == (
        "norway_brreg/financial/response_index/updates/"
        "date=2026-07-16/responses.parquet"
    )
    assert financial_update_response_partition_prefix(partition_date).endswith(
        "date=2026-07-16/"
    )
