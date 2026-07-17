from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import dagster as dg
import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.assets import financial_bootstrap
from dagster_v3.defs.norway_brreg_financial.assets.financial_bootstrap import (
    NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT,
    NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS,
    norway_brreg_financial_bootstrap_responses_json,
    norway_brreg_financial_bootstrap_responses_parquet,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    financial_bootstrap_response_index_object_key,
    financial_bootstrap_response_partition_prefix,
    financial_response_object_key,
)


class FakeClickhouseClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        self.executed.append((sql, params))
        return self.rows


class FakeClickhouseResource:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.client = FakeClickhouseClient(rows)

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, pl.DataFrame]] = []

    def write_bootstrap_response_index(
        self,
        bucket_key: str,
        frame: pl.DataFrame,
    ) -> str:
        self.writes.append((bucket_key, frame))
        return financial_bootstrap_response_index_object_key(bucket_key)


def test_bootstrap_partitions_and_response_keys_are_stable() -> None:
    keys = NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS.get_partition_keys()

    assert len(keys) == NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT == 64
    assert keys[0] == "bucket_00"
    assert keys[-1] == "bucket_63"
    prefix = financial_bootstrap_response_partition_prefix("bucket_07")
    assert prefix == (
        "norway_brreg/financial/responses/bootstrap/bucket=bucket_07/"
    )
    assert financial_response_object_key(prefix, "923609016") == (
        "norway_brreg/financial/responses/bootstrap/bucket=bucket_07/"
        "org=923609016/response.json"
    )
    assert financial_bootstrap_response_index_object_key("bucket_07") == (
        "norway_brreg/financial/response_index/bootstrap/"
        "bucket=bucket_07/responses.parquet"
    )


def test_bootstrap_parquet_depends_directly_on_bootstrap_json() -> None:
    parquet_key = dg.AssetKey(
        "norway_brreg_financial_bootstrap_responses_parquet"
    )

    assert dg.AssetKey("norway_brreg_financial_bootstrap_responses_json") in (
        norway_brreg_financial_bootstrap_responses_parquet.asset_deps[parquet_key]
    )


def test_bootstrap_json_queries_stable_bucket_and_passes_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clickhouse = FakeClickhouseResource(
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

    monkeypatch.setattr(
        financial_bootstrap,
        "materialize_response_json_partition",
        fake_materialize,
    )

    result = norway_brreg_financial_bootstrap_responses_json(
        context=dg.build_asset_context(partition_key="bucket_11"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=object(),
    )

    [(sql, params)] = clickhouse.client.executed
    assert "cityHash64(toString(org_number))" in sql
    assert params == {"bucket_count": 64, "bucket_index": 11}
    assert captured["candidates"] == [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "https://www.equinor.com",
            "last_submitted_accounts_year": "2024",
        }
    ]
    assert captured["partition_prefix"].endswith("bucket=bucket_11/")
    assert result.metadata["candidate_count"] == 1


def test_bootstrap_parquet_writes_only_verified_response_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()
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
        financial_bootstrap,
        "verified_response_index_frame",
        lambda **_kwargs: (
            frame,
            {
                "candidate_count": 1,
                "row_count": 1,
                "status_counts": {"success": 1},
                "success_manifest_key": "responses/_SUCCESS.json",
            },
        ),
    )

    result = norway_brreg_financial_bootstrap_responses_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        norway_brreg_financial_storage=storage,
    )

    [(bucket_key, written_frame)] = storage.writes
    assert bucket_key == "bucket_00"
    assert "raw_response" not in written_frame.columns
    assert result.metadata["row_count"] == 1
