from __future__ import annotations

from io import BytesIO

import polars as pl

from dagster_v3.defs.norway_brreg import financial_fetches
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_fetches_snapshot_object_key,
    financial_fetches_update_object_key,
    financial_raw_fetch_object_key,
    financial_statements_snapshot_object_key,
    financial_statements_update_object_key,
    financial_statements_usd_snapshot_object_key,
    financial_statements_usd_update_object_key,
    financial_update_candidates_object_key,
)


class FakeObjectStore:
    def __init__(self, keys: list[str] | None = None) -> None:
        self.created_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}
        for key in keys or []:
            self.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)] = b""

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        ]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body


def test_norway_financial_storage_object_keys_are_stable() -> None:
    assert financial_fetches_snapshot_object_key() == (
        "norway_brreg/financial/fetches/snapshot/financial_fetches.parquet"
    )
    assert financial_fetches_update_object_key("2026-06-30") == (
        "norway_brreg/financial/fetches/updates/date=2026-06-30/financial_fetches.parquet"
    )
    assert financial_raw_fetch_object_key("923609016", "2025") == (
        "norway_brreg/financial/raw_fetches/org=923609016/year=2025/financial_fetch.parquet"
    )
    assert financial_update_candidates_object_key("2026-06-30") == (
        "norway_brreg/financial/update_candidates/date=2026-06-30/financial_update_candidates.parquet"
    )
    assert financial_statements_snapshot_object_key() == (
        "norway_brreg/financial/statements/snapshot/financial_statements.parquet"
    )
    assert financial_statements_update_object_key("2026-06-30") == (
        "norway_brreg/financial/statements/updates/date=2026-06-30/financial_statements.parquet"
    )
    assert financial_statements_usd_snapshot_object_key() == (
        "norway_brreg/financial/statements_usd/snapshot/financial_statements.parquet"
    )
    assert financial_statements_usd_update_object_key("2026-06-30") == (
        "norway_brreg/financial/statements_usd/updates/date=2026-06-30/financial_statements.parquet"
    )


def test_storage_writes_and_reads_snapshot_fetches() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())
    snapshot_frame = pl.DataFrame(
        [{"org_number": "923609016", "accounts_year": "2025"}]
    )
    update_frame = pl.DataFrame([{"org_number": "923609016", "accounts_year": "2024"}])

    snapshot_key = storage.write_snapshot_fetches(snapshot_frame)
    update_key = storage.write_update_fetches("2026-06-30", update_frame)

    assert snapshot_key == financial_fetches_snapshot_object_key()
    assert update_key == financial_fetches_update_object_key("2026-06-30")
    assert storage.read_snapshot_fetches().to_dicts() == snapshot_frame.to_dicts()
    assert (
        storage.read_update_fetches("2026-06-30").to_dicts() == update_frame.to_dicts()
    )


def test_raw_fetch_write_skips_existing_object_when_overwrite_is_false() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    existing_frame = pl.DataFrame([{"org_number": "923609016", "status": "existing"}])
    replacement_frame = pl.DataFrame(
        [{"org_number": "923609016", "status": "replacement"}]
    )
    key = financial_raw_fetch_object_key("923609016", "2025")
    object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)] = _parquet_bytes(
        existing_frame
    )

    assert storage.raw_fetch_exists("923609016", "2025")

    returned_key = storage.write_raw_fetch(
        "923609016",
        "2025",
        replacement_frame,
        overwrite=False,
    )

    assert returned_key == key
    assert _stored_frame(object_store, key).to_dicts() == existing_frame.to_dicts()
    assert object_store.created_buckets == []


def test_raw_fetch_write_replaces_existing_object_when_overwrite_is_true() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    existing_frame = pl.DataFrame([{"org_number": "923609016", "status": "existing"}])
    replacement_frame = pl.DataFrame(
        [{"org_number": "923609016", "status": "replacement"}]
    )
    key = financial_raw_fetch_object_key("923609016", "2025")
    object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)] = _parquet_bytes(
        existing_frame
    )

    returned_key = storage.write_raw_fetch(
        "923609016",
        "2025",
        replacement_frame,
        overwrite=True,
    )

    assert returned_key == key
    assert _stored_frame(object_store, key).to_dicts() == replacement_frame.to_dicts()
    assert object_store.created_buckets == [NORWAY_BRREG_ENTITY_BUCKET]


def test_raw_fetch_read_round_trip() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())
    frame = pl.DataFrame(
        [
            {
                "org_number": "923609016",
                "last_submitted_accounts_year": "2025",
                "fetch_status": "success",
            }
        ]
    )

    key = storage.write_raw_fetch("923609016", "2025", frame, overwrite=False)

    assert key == financial_raw_fetch_object_key("923609016", "2025")
    assert storage.read_raw_fetch("923609016", "2025").to_dicts() == frame.to_dicts()


def test_financial_storage_lists_historical_raw_fetch_keys() -> None:
    first_key = financial_raw_fetch_object_key("100", "2024")
    second_key = financial_raw_fetch_object_key("200", "2023")
    object_store = FakeObjectStore(
        keys=[
            second_key,
            "norway_brreg/financial/raw_fetches/org=bad/readme.txt",
            "norway_brreg/financial/raw_fetches/org=bad/year=2024/not_financial_fetch.parquet",
            first_key,
        ]
    )
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)

    assert storage.list_historical_raw_fetch_keys() == [first_key, second_key]


def test_financial_storage_reads_historical_raw_fetches() -> None:
    object_store = FakeObjectStore()
    first_key = financial_raw_fetch_object_key("100", "2024")
    second_key = financial_raw_fetch_object_key("200", "2023")
    first_frame = pl.DataFrame(
        {
            "org_number": ["100"],
            "fetch_status": ["success"],
            "attempt_count": pl.Series([1], dtype=pl.Int32),
        }
    )
    second_frame = pl.DataFrame(
        {
            "org_number": ["200"],
            "fetch_status": ["not_found"],
            "attempt_count": pl.Series([2], dtype=pl.Int64),
        }
    )
    object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, second_key)] = _parquet_bytes(
        second_frame
    )
    object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, first_key)] = _parquet_bytes(
        first_frame
    )
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)

    historical_frame = storage.read_historical_raw_fetches()

    assert historical_frame.to_dicts() == [
        {"org_number": "100", "fetch_status": "success", "attempt_count": 1},
        {"org_number": "200", "fetch_status": "not_found", "attempt_count": 2},
    ]
    assert historical_frame.schema["attempt_count"] == pl.Int64


def test_financial_storage_reads_empty_historical_raw_fetches_with_schema() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())

    historical_frame = storage.read_historical_raw_fetches()

    assert historical_frame.height == 0
    assert historical_frame.schema == financial_fetches.financial_fetches_parquet_schema()


def test_update_candidates_write_and_read_round_trip() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())
    frame = pl.DataFrame([{"org_number": "923609016", "accounts_year": "2025"}])

    key = storage.write_update_financial_candidates("2026-06-30", frame)

    assert key == financial_update_candidates_object_key("2026-06-30")
    assert (
        storage.read_update_financial_candidates("2026-06-30").to_dicts()
        == frame.to_dicts()
    )


def test_original_statement_write_and_read_methods_round_trip() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())
    snapshot_frame = pl.DataFrame([{"org_number": "923609016", "revenue": 1000}])
    update_frame = pl.DataFrame([{"org_number": "923609016", "revenue": 1100}])

    snapshot_key = storage.write_snapshot_statements(snapshot_frame)
    update_key = storage.write_update_statements("2026-06-30", update_frame)

    assert snapshot_key == financial_statements_snapshot_object_key()
    assert update_key == financial_statements_update_object_key("2026-06-30")
    assert storage.read_snapshot_statements().to_dicts() == snapshot_frame.to_dicts()
    assert (
        storage.read_update_statements("2026-06-30").to_dicts()
        == update_frame.to_dicts()
    )


def test_usd_statement_write_and_read_methods_round_trip() -> None:
    storage = NorwayBrregFinancialParquetStorageResource(object_store=FakeObjectStore())
    snapshot_frame = pl.DataFrame([{"org_number": "923609016", "revenue_usd": 95.5}])
    update_frame = pl.DataFrame([{"org_number": "923609016", "revenue_usd": 100.25}])

    snapshot_key = storage.write_snapshot_usd_statements(snapshot_frame)
    update_key = storage.write_update_usd_statements("2026-06-30", update_frame)

    assert snapshot_key == financial_statements_usd_snapshot_object_key()
    assert update_key == financial_statements_usd_update_object_key("2026-06-30")
    assert (
        storage.read_snapshot_usd_statements().to_dicts() == snapshot_frame.to_dicts()
    )
    assert (
        storage.read_update_usd_statements("2026-06-30").to_dicts()
        == update_frame.to_dicts()
    )


def _stored_frame(object_store: FakeObjectStore, key: str) -> pl.DataFrame:
    return pl.read_parquet(
        BytesIO(object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)])
    )


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()
