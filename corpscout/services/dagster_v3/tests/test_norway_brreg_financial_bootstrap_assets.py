from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

import dagster as dg
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.assets import financial_bootstrap
from dagster_v3.defs.norway_brreg_financial.assets.financial_bootstrap import (
    NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT,
    NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS,
    norway_brreg_financial_bootstrap_fetches_parquet,
)
from dagster_v3.defs.norway_brreg_financial.constants import (
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_bootstrap_chunk_object_key,
    latest_fetch_rows_per_org,
    legacy_bootstrap_done_marker_key,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        ]


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


def test_bootstrap_partitions_and_chunk_keys_are_stable() -> None:
    keys = NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS.get_partition_keys()
    assert len(keys) == NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT == 64
    assert keys[0] == "bucket_00"
    assert keys[-1] == "bucket_63"
    assert financial_bootstrap_chunk_object_key("bucket_07", 3) == (
        "norway_brreg/financial/bootstrap/fetches/bucket=bucket_07/chunk=0003.parquet"
    )
    assert legacy_bootstrap_done_marker_key("923609016") == (
        "norway_brreg/finance/raw_reports/org=923609016/status/done.json"
    )


def test_snapshot_inventory_depends_on_bootstrap_asset() -> None:
    from dagster_v3.defs.norway_brreg_financial.assets.financial_fetches import (
        norway_brreg_financial_fetches_snapshot_parquet,
    )

    assert dg.AssetKey("norway_brreg_financial_bootstrap_fetches_parquet") in (
        norway_brreg_financial_fetches_snapshot_parquet.asset_deps[
            dg.AssetKey("norway_brreg_financial_fetches_snapshot_parquet")
        ]
    )


def test_consolidated_historical_fetches_union_bootstrap_and_raw_with_latest_per_org() -> None:
    storage, _object_store = _storage()
    storage.write_bootstrap_chunk(
        "bucket_00",
        0,
        financial_fetches.financial_fetches_frame(
            [
                _failure_row(_candidate("811111111"), fetch_status="server_error"),
                _success_row(_candidate("822222222")),
            ]
        ),
    )
    storage.write_bootstrap_chunk(
        "bucket_01",
        0,
        financial_fetches.financial_fetches_frame(
            [_success_row(_candidate("833333333"))]
        ),
    )
    # Newer per-org raw fetch (update-asset layout) must win over the older
    # bootstrap failure row for the same org.
    update_row = _success_row(_candidate("811111111"))
    update_row["fetched_at"] = "2026-07-06T00:00:00.000Z"
    storage.write_raw_fetch(
        "811111111",
        "2024",
        financial_fetches.financial_fetches_frame([update_row]),
        overwrite=True,
    )

    consolidated = storage.read_consolidated_historical_fetches()

    rows = {
        row["org_number"]: row["fetch_status"]
        for row in consolidated.select(["org_number", "fetch_status"]).to_dicts()
    }
    assert rows == {
        "811111111": "success",
        "822222222": "success",
        "833333333": "success",
    }


def test_consolidated_historical_fetches_empty_layouts_return_empty_schema_frame() -> None:
    storage, _object_store = _storage()
    consolidated = storage.read_consolidated_historical_fetches()
    assert consolidated.is_empty()
    assert consolidated.columns == list(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS)


def test_bootstrap_candidate_query_uses_bucket_predicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clickhouse = FakeClickhouseResource(
        [("923609016", "EQUINOR ASA", "https://www.equinor.com", "2024")]
    )
    storage, _object_store = _storage()
    monkeypatch.setattr(
        financial_fetches,
        "fetch_financial_rows_for_orgs",
        _fake_fetch_success,
    )

    norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_11"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    [(sql, params)] = clickhouse.client.executed
    assert "cityHash64(toString(org_number))" in sql
    assert params == {"bucket_count": 64, "bucket_index": 11}


def test_bootstrap_asset_fetches_new_orgs_into_size_limited_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(financial_bootstrap, "BOOTSTRAP_CHUNK_SIZE", 2)
    monkeypatch.setattr(
        financial_fetches,
        "fetch_financial_rows_for_orgs",
        _fake_fetch_success,
    )
    storage, _object_store = _storage()
    clickhouse = FakeClickhouseResource(
        [
            ("811111111", "A AS", None, "2024"),
            ("822222222", "B AS", None, "2024"),
            ("833333333", "C AS", None, "2024"),
        ]
    )

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["candidate_count"] == 3
    assert result.metadata["already_done_count"] == 0
    assert result.metadata["converted_count"] == 0
    assert result.metadata["fetched_count"] == 3
    assert result.metadata["chunk_count"] == 2
    assert result.metadata["status_counts"] == {"success": 3}
    assert result.metadata["s3_bucket"] == NORWAY_BRREG_FINANCIAL_BUCKET
    assert storage.list_bootstrap_chunk_keys("bucket_00") == [
        financial_bootstrap_chunk_object_key("bucket_00", 0),
        financial_bootstrap_chunk_object_key("bucket_00", 1),
    ]
    bucket_frame = storage.read_bootstrap_bucket_fetches("bucket_00")
    assert sorted(bucket_frame["org_number"].to_list()) == [
        "811111111",
        "822222222",
        "833333333",
    ]


def test_bootstrap_asset_skips_orgs_already_in_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched_orgs: list[str] = []

    def fake_fetch(**kwargs: Any) -> list[dict[str, Any]]:
        orgs = list(kwargs["orgs"])
        fetched_orgs.extend(org["org_number"] for org in orgs)
        return [_success_row(org) for org in orgs]

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fake_fetch)
    storage, _object_store = _storage()
    storage.write_bootstrap_chunk(
        "bucket_00",
        0,
        financial_fetches.financial_fetches_frame(
            [_success_row(_candidate("811111111"))]
        ),
    )
    clickhouse = FakeClickhouseResource(
        [
            ("811111111", "A AS", None, "2024"),
            ("822222222", "B AS", None, "2024"),
        ]
    )

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert fetched_orgs == ["822222222"]
    assert result.metadata["already_done_count"] == 1
    assert result.metadata["fetched_count"] == 1
    assert storage.list_bootstrap_chunk_keys("bucket_00") == [
        financial_bootstrap_chunk_object_key("bucket_00", 0),
        financial_bootstrap_chunk_object_key("bucket_00", 1),
    ]


def test_bootstrap_asset_converts_legacy_done_marker_without_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fetch(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("legacy-converted orgs must not call the BRREG API")

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fail_fetch)
    storage, object_store = _storage()
    report_key_2023 = "norway_brreg/finance/raw_reports/org=811111111/year=2023/type=SELSKAP/id=1.json"
    report_key_2024 = "norway_brreg/finance/raw_reports/org=811111111/year=2024/type=SELSKAP/id=2.json"
    _write_json(object_store, report_key_2023, {"id": 1, "regnskapstype": "SELSKAP"})
    _write_json(object_store, report_key_2024, {"id": 2, "regnskapstype": "SELSKAP"})
    _write_json(
        object_store,
        legacy_bootstrap_done_marker_key("811111111"),
        {
            "org_number": "811111111",
            "fetch_status": "success",
            "report_count": 2,
            "raw_report_keys": [report_key_2023, report_key_2024],
            "completed_at": "2026-07-01T10:00:00.000Z",
        },
    )
    clickhouse = FakeClickhouseResource([("811111111", "A AS", None, "2024")])

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["converted_count"] == 1
    assert result.metadata["fetched_count"] == 0
    assert result.metadata["status_counts"] == {"success": 1}
    [row] = storage.read_bootstrap_bucket_fetches("bucket_00").to_dicts()
    assert row["org_number"] == "811111111"
    assert row["fetch_status"] == "success"
    assert json.loads(row["raw_response"]) == [
        {"id": 1, "regnskapstype": "SELSKAP"},
        {"id": 2, "regnskapstype": "SELSKAP"},
    ]


def test_bootstrap_asset_converts_legacy_not_found_marker_without_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fetch(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("legacy-converted orgs must not call the BRREG API")

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fail_fetch)
    storage, object_store = _storage()
    _write_json(
        object_store,
        legacy_bootstrap_done_marker_key("811111111"),
        {
            "org_number": "811111111",
            "fetch_status": "not_found",
            "report_count": 0,
            "raw_report_keys": [],
            "completed_at": "2026-07-01T10:00:00.000Z",
        },
    )
    clickhouse = FakeClickhouseResource([("811111111", "A AS", None, "2024")])

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["converted_count"] == 1
    assert result.metadata["status_counts"] == {"not_found": 1}
    [row] = storage.read_bootstrap_bucket_fetches("bucket_00").to_dicts()
    assert row["fetch_status"] == "not_found"
    assert row["http_status"] == 404
    assert row["error_type"] == "LegacyBootstrapMarker"


def test_bootstrap_asset_refetches_orgs_with_only_legacy_failed_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        financial_fetches,
        "fetch_financial_rows_for_orgs",
        _fake_fetch_success,
    )
    storage, object_store = _storage()
    _write_json(
        object_store,
        "norway_brreg/finance/raw_reports/org=811111111/status/failed.json",
        {"org_number": "811111111", "fetch_status": "server_error"},
    )
    clickhouse = FakeClickhouseResource([("811111111", "A AS", None, "2024")])

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["converted_count"] == 0
    assert result.metadata["fetched_count"] == 1
    assert result.metadata["status_counts"] == {"success": 1}


def test_bootstrap_asset_raises_on_retryable_statuses_after_writing_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch(**kwargs: Any) -> list[dict[str, Any]]:
        return [
            _failure_row(org, fetch_status="server_error")
            for org in kwargs["orgs"]
        ]

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fake_fetch)
    storage, _object_store = _storage()
    clickhouse = FakeClickhouseResource([("811111111", "A AS", None, "2024")])

    with pytest.raises(RuntimeError, match="retryable statuses"):
        norway_brreg_financial_bootstrap_fetches_parquet(
            context=dg.build_asset_context(partition_key="bucket_00"),
            clickhouse=clickhouse,
            norway_brreg_financial_storage=storage,
        )

    [row] = storage.read_bootstrap_bucket_fetches("bucket_00").to_dicts()
    assert row["fetch_status"] == "server_error"


def test_bootstrap_asset_refetches_orgs_whose_latest_row_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        financial_fetches,
        "fetch_financial_rows_for_orgs",
        _fake_fetch_success,
    )
    storage, _object_store = _storage()
    storage.write_bootstrap_chunk(
        "bucket_00",
        0,
        financial_fetches.financial_fetches_frame(
            [_failure_row(_candidate("811111111"), fetch_status="server_error")]
        ),
    )
    clickhouse = FakeClickhouseResource([("811111111", "A AS", None, "2024")])

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["already_done_count"] == 0
    assert result.metadata["fetched_count"] == 1
    bucket_frame = storage.read_bootstrap_bucket_fetches("bucket_00")
    assert bucket_frame.height == 2
    [latest] = latest_fetch_rows_per_org(bucket_frame).to_dicts()
    assert latest["fetch_status"] == "success"


def test_bootstrap_asset_with_no_pending_orgs_writes_no_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fetch(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("no pending orgs, so the BRREG API must not be called")

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fail_fetch)
    storage, _object_store = _storage()
    clickhouse = FakeClickhouseResource([])

    result = norway_brreg_financial_bootstrap_fetches_parquet(
        context=dg.build_asset_context(partition_key="bucket_00"),
        clickhouse=clickhouse,
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["candidate_count"] == 0
    assert result.metadata["chunk_count"] == 0
    assert storage.list_bootstrap_chunk_keys("bucket_00") == []


class StorageProxy:
    """Duck-typed stand-in so direct asset invocation does not re-initialize the
    ConfigurableResource and drop the injected fake object store."""

    def __init__(self, inner: NorwayBrregFinancialParquetStorageResource) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _storage() -> tuple[StorageProxy, FakeObjectStore]:
    object_store = FakeObjectStore()
    return (
        StorageProxy(NorwayBrregFinancialParquetStorageResource(object_store=object_store)),
        object_store,
    )


def _write_json(object_store: FakeObjectStore, key: str, value: Any) -> None:
    object_store.write_bytes(
        key,
        json.dumps(value).encode("utf-8"),
        bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
    )


def _candidate(org_number: str) -> dict[str, Any]:
    return {
        "org_number": org_number,
        "legal_name": f"{org_number} AS",
        "website": "",
        "last_submitted_accounts_year": "2024",
    }


def _fake_fetch_success(**kwargs: Any) -> list[dict[str, Any]]:
    return [_success_row(org) for org in kwargs["orgs"]]


def _success_row(org: dict[str, Any]) -> dict[str, Any]:
    return financial_fetches.financial_fetch_success_row(
        org=org,
        source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{org['org_number']}",
        payload=[{"id": 1, "regnskapstype": "SELSKAP"}],
        source_run_id="run-1",
        source_line_number=1,
        status_code=200,
        fetched_at="2026-07-05T00:00:00.000Z",
        attempt_count=1,
    )


def _failure_row(org: dict[str, Any], *, fetch_status: str) -> dict[str, Any]:
    return financial_fetches.financial_fetch_failure_row(
        org=org,
        source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{org['org_number']}",
        source_run_id="run-1",
        source_line_number=1,
        status_code=500,
        fetch_status=fetch_status,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-07-04T00:00:00.000Z",
        attempt_count=1,
        raw_response="server error",
    )
