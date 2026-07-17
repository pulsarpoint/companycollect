from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.constants import (
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_bootstrap_response_index_object_key,
    financial_bootstrap_response_partition_prefix,
    financial_response_checkpoint_object_key,
    financial_response_object_key,
    financial_response_success_object_key,
    financial_statements_snapshot_object_key,
    financial_statements_update_object_key,
    financial_statements_usd_snapshot_object_key,
    financial_statements_usd_update_object_key,
    financial_update_response_index_object_key,
    financial_update_response_partition_prefix,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.created_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return (str(bucket), key) in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[(str(bucket), key)]

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == str(bucket) and key.startswith(prefix)
        ]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        self.objects[(str(bucket), key)] = body


def test_norway_financial_json_and_index_keys_are_stable() -> None:
    bootstrap_prefix = financial_bootstrap_response_partition_prefix("bucket_07")
    update_prefix = financial_update_response_partition_prefix("2026-07-17")

    assert bootstrap_prefix == (
        "norway_brreg/financial/responses/bootstrap/bucket=bucket_07/"
    )
    assert update_prefix == (
        "norway_brreg/financial/responses/updates/date=2026-07-17/"
    )
    assert financial_response_object_key(bootstrap_prefix, "923609016") == (
        f"{bootstrap_prefix}org=923609016/response.json"
    )
    assert financial_response_checkpoint_object_key(
        bootstrap_prefix,
        "run-1",
        3,
    ) == f"{bootstrap_prefix}checkpoints/run=run-1/batch=000003.json"
    assert financial_response_success_object_key(bootstrap_prefix) == (
        f"{bootstrap_prefix}_SUCCESS.json"
    )
    assert financial_bootstrap_response_index_object_key("bucket_07") == (
        "norway_brreg/financial/response_index/bootstrap/"
        "bucket=bucket_07/responses.parquet"
    )
    assert financial_update_response_index_object_key("2026-07-17") == (
        "norway_brreg/financial/response_index/updates/"
        "date=2026-07-17/responses.parquet"
    )


def test_successful_response_json_is_immutable() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(
        object_store=FakeObjectStore()
    )
    key = "norway_brreg/financial/responses/test/org=1/response.json"

    storage.write_response(key, b'[{"id":1}]')
    storage.write_response(key, b'[{"id":1}]')

    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        storage.write_response(key, b'[{"id":2}]')


def test_checkpoint_records_round_trip_in_sorted_key_order() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(
        object_store=FakeObjectStore()
    )
    prefix = financial_bootstrap_response_partition_prefix("bucket_00")
    second_key = financial_response_checkpoint_object_key(prefix, "run-2", 0)
    first_key = financial_response_checkpoint_object_key(prefix, "run-1", 0)
    storage.write_json_object(second_key, {"records": [{"org_number": "200"}]})
    storage.write_json_object(first_key, {"records": [{"org_number": "100"}]})

    assert storage.list_response_checkpoint_keys(prefix) == [first_key, second_key]
    assert storage.read_response_records(prefix) == [
        {"org_number": "100"},
        {"org_number": "200"},
    ]


def test_response_indexes_preserve_distinct_historical_objects_for_same_org() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(
        object_store=FakeObjectStore()
    )
    bootstrap_row = _response_index_row(
        source_object_key="responses/bootstrap/org=923609016/response.json",
        fetched_at="2026-07-01T00:00:00.000Z",
    )
    update_row = _response_index_row(
        source_object_key="responses/updates/org=923609016/response.json",
        fetched_at="2026-07-17T00:00:00.000Z",
    )
    storage.write_bootstrap_response_index(
        "bucket_00",
        financial_fetches.financial_fetches_frame([bootstrap_row]),
    )
    storage.write_update_response_index(
        "2026-07-17",
        financial_fetches.financial_fetches_frame([update_row]),
    )
    messages: list[str] = []

    consolidated = storage.read_consolidated_historical_response_index(
        log=lambda message, *args: messages.append(message % args)
    )

    assert consolidated.height == 2
    assert set(consolidated.get_column("source_object_key")) == {
        bootstrap_row["source_object_key"],
        update_row["source_object_key"],
    }
    assert "raw_response" not in consolidated.columns
    assert any("response index parquet files" in message for message in messages)


def test_statement_parquet_object_keys_and_round_trips_are_stable() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(
        object_store=FakeObjectStore()
    )
    frame = pl.DataFrame([{"org_number": "923609016", "filing_id": 1}])

    assert storage.write_snapshot_statements(frame) == (
        financial_statements_snapshot_object_key()
    )
    assert storage.write_update_statements("2026-07-17", frame) == (
        financial_statements_update_object_key("2026-07-17")
    )
    assert storage.write_snapshot_usd_statements(frame) == (
        financial_statements_usd_snapshot_object_key()
    )
    assert storage.write_update_usd_statements("2026-07-17", frame) == (
        financial_statements_usd_update_object_key("2026-07-17")
    )
    assert storage.read_snapshot_statements().to_dicts() == frame.to_dicts()
    assert storage.read_update_statements("2026-07-17").to_dicts() == frame.to_dicts()
    assert storage.read_snapshot_usd_statements().to_dicts() == frame.to_dicts()
    assert storage.read_update_usd_statements("2026-07-17").to_dicts() == (
        frame.to_dicts()
    )
    assert NORWAY_BRREG_FINANCIAL_BUCKET in storage.object_store.created_buckets


def _response_index_row(
    *,
    source_object_key: str,
    fetched_at: str,
) -> dict[str, Any]:
    return financial_fetches.response_record(
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "https://www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        source_url=(
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016"
        ),
        source_run_id="run-1",
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at=fetched_at,
        source_object_key=source_object_key,
        source_payload_hash="a" * 64,
    )
