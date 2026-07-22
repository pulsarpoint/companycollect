import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster import AssetKey
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs


def test_eodhd_assets_are_registered_one_to_one() -> None:
    from dagster_v3.defs.eodhd import tables

    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "eodhd_reference_raw_objects" in asset_keys
    for table_name in tables.EODHD_REFERENCE_TABLES:
        assert f"{table_name}_duckdb" in asset_keys
        assert table_name in asset_keys
    assert "eodhd_reference_complete" in asset_keys
    assert "eodhd_eod_price_raw_objects" in asset_keys
    assert "eodhd_eod_prices_duckdb" in asset_keys
    assert tables.EODHD_EOD_PRICES_TABLE in asset_keys


def test_eodhd_asset_dependencies_match_storage_boundaries() -> None:
    from dagster_v3.defs.eodhd import tables

    graph = load_project_defs().get_repository_def().asset_graph

    for table_name in (
        tables.EODHD_EXCHANGES_TABLE,
        tables.EODHD_EXCHANGE_MICS_TABLE,
        tables.EODHD_SYMBOLS_TABLE,
    ):
        assert graph.get(AssetKey([f"{table_name}_duckdb"])).parent_keys == {
            AssetKey(["eodhd_reference_raw_objects"])
        }
        assert graph.get(AssetKey([table_name])).parent_keys == {
            AssetKey([f"{table_name}_duckdb"])
        }

    assert graph.get(AssetKey(["eodhd_symbol_mics_duckdb"])).parent_keys == {
        AssetKey(["eodhd_exchange_mics_duckdb"]),
        AssetKey(["eodhd_symbols_duckdb"]),
    }
    assert graph.get(AssetKey(["eodhd_reference_complete"])).parent_keys == {
        AssetKey([table_name]) for table_name in tables.EODHD_REFERENCE_TABLES
    }
    assert graph.get(AssetKey(["eodhd_eod_price_raw_objects"])).parent_keys == {
        AssetKey(["eodhd_reference_complete"])
    }
    assert graph.get(AssetKey(["eodhd_eod_prices_duckdb"])).parent_keys == {
        AssetKey(["eodhd_eod_price_raw_objects"])
    }
    assert graph.get(AssetKey([tables.EODHD_EOD_PRICES_TABLE])).parent_keys == {
        AssetKey(["eodhd_eod_prices_duckdb"])
    }


def test_eodhd_price_assets_use_stable_static_hash_partitions() -> None:
    from dagster_v3.defs.eodhd import assets, tables

    repository = load_project_defs().get_repository_def()
    graph = repository.asset_graph
    partition_keys = assets.EODHD_PRICE_PARTITIONS.get_partition_keys()

    assert isinstance(assets.EODHD_PRICE_PARTITIONS, dg.StaticPartitionsDefinition)
    assert len(partition_keys) == assets.EODHD_PRICE_BUCKET_COUNT == 256
    assert partition_keys[0] == "bucket_000"
    assert partition_keys[-1] == "bucket_255"
    assert assets.eodhd_price_partition_key("CDR.WAR") == "bucket_118"
    assert assets.eodhd_price_bucket_index("bucket_118") == 118

    for asset_key in (
        "eodhd_eod_price_raw_objects",
        "eodhd_eod_prices_duckdb",
        tables.EODHD_EOD_PRICES_TABLE,
    ):
        node = graph.get(AssetKey(asset_key))
        assert node.partitions_def is assets.EODHD_PRICE_PARTITIONS
        assert node.backfill_policy == dg.BackfillPolicy.multi_run(
            max_partitions_per_run=1
        )

    assert (
        repository.get_job("eodhd_price_backfill_job").partitions_def
        is assets.EODHD_PRICE_PARTITIONS
    )


def test_eodhd_price_partition_key_validation_rejects_invalid_values() -> None:
    from dagster_v3.defs.eodhd import assets

    for partition_key in ("bucket_256", "bucket_-1", "118", "bucket_x"):
        with pytest.raises(ValueError):
            assets.eodhd_price_bucket_index(partition_key)


def test_eodhd_tables_use_distinct_duckdb_files(monkeypatch, tmp_path: Path) -> None:
    from dagster_v3.defs.eodhd import assets, tables

    monkeypatch.setattr(assets, "EODHD_DUCKDB_DIRECTORY", tmp_path)

    paths = {
        assets.eodhd_duckdb_path(table_name)
        for table_name in tables.EODHD_REFERENCE_TABLES
    }

    assert paths == {
        tmp_path / f"{table_name}.duckdb"
        for table_name in tables.EODHD_REFERENCE_TABLES
    }
    assert assets.eodhd_duckdb_path(
        tables.EODHD_EOD_PRICES_TABLE,
        partition_key="bucket_118",
    ) == (tmp_path / tables.EODHD_EOD_PRICES_TABLE / "bucket_118.duckdb")
    with pytest.raises(ValueError, match="requires a partition key"):
        assets.eodhd_duckdb_path(tables.EODHD_EOD_PRICES_TABLE)


def test_reference_config_rejects_invalid_limits_and_blank_exchange_codes() -> None:
    from dagster_v3.defs.eodhd.source import EodhdReferenceConfig

    with pytest.raises(ValueError, match="greater than zero"):
        EodhdReferenceConfig(max_exchanges=0)
    with pytest.raises(ValueError, match="must not be blank"):
        EodhdReferenceConfig(exchange_codes_csv="  ")


def test_price_backfill_config_resolves_six_year_window() -> None:
    from dagster_v3.defs.eodhd.source import EodhdPriceBackfillConfig

    config = EodhdPriceBackfillConfig(years=6)

    assert config.resolve_date_window(datetime(2026, 7, 21, tzinfo=UTC)) == (
        "2020-07-21",
        "2026-07-21",
    )
    assert "max_workers" not in EodhdPriceBackfillConfig.model_fields


def test_price_symbol_selection_only_returns_requested_hash_partition(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.eodhd import assets, tables
    from dagster_v3.defs.eodhd.source import EodhdPriceBackfillConfig

    monkeypatch.setattr(assets, "EODHD_DUCKDB_DIRECTORY", tmp_path)
    database_path = assets.eodhd_duckdb_path(tables.EODHD_SYMBOLS_TABLE)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema eodhd")
        connection.execute(
            "create table eodhd.eodhd_symbols "
            "(eodhd_symbol_key varchar, exchange_code varchar, ticker varchar, "
            "currency varchar, instrument_type varchar, is_delisted integer)"
        )
        connection.executemany(
            "insert into eodhd.eodhd_symbols values (?, ?, ?, ?, ?, ?)",
            [
                ("CDR.WAR", "WAR", "CDR", "PLN", "Common Stock", 0),
                ("AAPL.US", "US", "AAPL", "USD", "Common Stock", 0),
            ],
        )

    symbols = assets.select_eodhd_price_symbols(
        config=EodhdPriceBackfillConfig(),
        partition_key="bucket_118",
    )

    assert [symbol["eodhd_symbol_key"] for symbol in symbols] == ["CDR.WAR"]


def test_reference_snapshot_writes_active_and_delisted_symbol_objects() -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd.source import (
        EODHD_RAW_BUCKET,
        EodhdReferenceConfig,
        read_json_gzip,
        reference_snapshot_object_key,
        write_eodhd_reference_snapshot,
    )

    client = FakeEodhdClient()
    s3 = FakeS3Client()
    object_store = ObjectStoreResource(s3_client=s3, bucket=EODHD_RAW_BUCKET)

    metadata = write_eodhd_reference_snapshot(
        client=client,
        object_store=object_store,
        config=EodhdReferenceConfig(request_delay_seconds=0),
        run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
        log=lambda *_args: None,
    )

    assert metadata["exchange_count"] == 1
    assert metadata["active_symbol_count"] == 1
    assert metadata["delisted_symbol_count"] == 1
    assert client.symbol_calls == [("WAR", False), ("WAR", True)]

    snapshot = json.loads(
        s3.objects[(EODHD_RAW_BUCKET, reference_snapshot_object_key("run-1"))]
    )
    assert snapshot["completed"] is True
    assert len(snapshot["objects"]) == 3
    symbol_object = next(
        item for item in snapshot["objects"] if item["kind"] == "symbols_active"
    )
    rows = read_json_gzip(s3.objects[(EODHD_RAW_BUCKET, symbol_object["object_key"])])
    assert rows[0]["Code"] == "CDR"


def test_reference_rows_preserve_exchange_and_symbol_provenance() -> None:
    from dagster_v3.defs.eodhd.source import (
        exchange_rows_from_payload,
        symbol_rows_from_payload,
    )

    exchange_rows = exchange_rows_from_payload(
        [
            {
                "Code": "US",
                "Name": "USA Stocks",
                "OperatingMIC": "XNAS,XNYS",
                "Country": "USA",
                "Currency": "USD",
                "CountryISO2": "US",
                "CountryISO3": "USA",
            }
        ],
        source_run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
    )
    exchange, mics = exchange_rows
    assert exchange[0]["exchange_code"] == "US"
    assert exchange[0]["operating_mic_raw"] == "XNAS,XNYS"
    assert [row["mic"] for row in mics] == ["XNAS", "XNYS"]
    assert [row["mic_position"] for row in mics] == [1, 2]

    symbols = symbol_rows_from_payload(
        [
            {
                "Code": "AAPL",
                "Name": "Apple Inc",
                "Country": "USA",
                "Exchange": "NASDAQ",
                "Currency": "USD",
                "Type": "Common Stock",
                "Isin": "US0378331005",
            }
        ],
        exchange_code="US",
        is_delisted=False,
        source_run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
    )
    assert symbols[0]["eodhd_symbol_key"] == "AAPL.US"
    assert symbols[0]["reported_exchange_code"] == "NASDAQ"
    assert symbols[0]["is_delisted"] == 0
    assert len(symbols[0]["source_payload_hash"]) == 64


def test_symbol_mic_resolution_uses_reported_exchange() -> None:
    from dagster_v3.defs.eodhd.source import resolve_symbol_mic_rows

    rows = resolve_symbol_mic_rows(
        symbols=[
            {
                "eodhd_symbol_key": "AAPL.US",
                "exchange_code": "US",
                "reported_exchange_code": "NASDAQ",
            }
        ],
        exchange_mics={"US": ("XNAS", "XNYS")},
        source_run_id="run-1",
        resolved_at="2026-07-21T10:00:00+00:00",
    )

    assert [(row["mic"], row["is_primary"]) for row in rows] == [("XNAS", 1)]
    assert rows[0]["resolution_method"] == "reported_exchange_alias"
    assert rows[0]["resolution_confidence"] == "high"


def test_price_rows_keep_daily_ohlcv_and_object_lineage() -> None:
    from dagster_v3.defs.eodhd.source import price_rows_from_payload

    rows = price_rows_from_payload(
        [
            {
                "date": "2026-07-20",
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "adjusted_close": 103.5,
                "volume": 800000,
            }
        ],
        symbol={
            "eodhd_symbol_key": "CDR.WAR",
            "exchange_code": "WAR",
            "ticker": "CDR",
            "currency": "PLN",
        },
        source_run_id="prices-1",
        source_object_key="prices/run_id=prices-1/symbols/CDR.WAR.json.gz",
        retrieved_at="2026-07-21T10:00:00+00:00",
    )

    assert rows[0]["eodhd_symbol_key"] == "CDR.WAR"
    assert rows[0]["price_date"] == "2026-07-20"
    assert rows[0]["adjusted_close"] == "103.5"
    assert rows[0]["source_record_id"] == "CDR.WAR:2026-07-20"
    assert rows[0]["source_object_key"].endswith("CDR.WAR.json.gz")


def test_price_snapshot_download_materializes_completed_duckdb_table(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    client = FakeEodhdClient()
    s3 = FakeS3Client()
    object_store = ObjectStoreResource(s3_client=s3, bucket=EODHD_RAW_BUCKET)
    symbols = [
        {
            "eodhd_symbol_key": "CDR.WAR",
            "exchange_code": "WAR",
            "ticker": "CDR",
            "currency": "PLN",
        }
    ]

    download_metadata = assets.download_eodhd_price_snapshot(
        client=client,
        object_store=object_store,
        symbols=symbols,
        partition_key="bucket_118",
        run_id="prices-1",
        start_date="2020-07-21",
        end_date="2026-07-21",
        request_delay_seconds=0,
        progress_interval=1,
        log=lambda *_args: None,
    )
    database_path = tmp_path / "eodhd_eod_prices.duckdb"
    materialize_metadata = assets.materialize_eodhd_prices(
        database_path=database_path,
        object_store=object_store,
        raw_run_id="prices-1",
        partition_key="bucket_118",
        progress_interval=1,
        log=lambda *_args: None,
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            "select eodhd_symbol_key, price_date, close, currency, source_run_id "
            "from eodhd.eodhd_eod_prices order by price_date"
        ).fetchall()

    assert client.price_calls == [("CDR.WAR", "2020-07-21", "2026-07-21")]
    assert download_metadata["symbol_count"] == 1
    assert download_metadata["partition_key"] == "bucket_118"
    assert download_metadata["price_row_count"] == 2
    assert materialize_metadata["row_count"] == 2
    assert materialize_metadata["partition_key"] == "bucket_118"
    assert rows == [
        ("CDR.WAR", datetime(2026, 7, 20).date(), 105.0, "PLN", "prices-1"),
        ("CDR.WAR", datetime(2026, 7, 21).date(), 108.0, "PLN", "prices-1"),
    ]


def test_empty_price_partition_materializes_as_successful_empty_table(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    object_store = ObjectStoreResource(
        s3_client=FakeS3Client(),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()
    download_metadata = assets.download_eodhd_price_snapshot(
        client=client,
        object_store=object_store,
        symbols=[],
        partition_key="bucket_000",
        run_id="empty-prices",
        start_date="2020-07-21",
        end_date="2026-07-21",
        request_delay_seconds=0,
        progress_interval=1,
        log=lambda *_args: None,
    )
    database_path = tmp_path / "bucket_000.duckdb"

    materialize_metadata = assets.materialize_eodhd_prices(
        database_path=database_path,
        object_store=object_store,
        raw_run_id="empty-prices",
        partition_key="bucket_000",
        progress_interval=1,
        log=lambda *_args: None,
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        row_count = connection.execute(
            "select count(*) from eodhd.eodhd_eod_prices"
        ).fetchone()[0]
    assert client.price_calls == []
    assert download_metadata["symbol_count"] == 0
    assert materialize_metadata["row_count"] == 0
    assert materialize_metadata["min_price_date"] is None
    assert row_count == 0


def test_duckdb_reference_table_materialization_writes_expected_contract(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets, tables
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET, EodhdReferenceConfig

    s3 = FakeS3Client()
    object_store = ObjectStoreResource(s3_client=s3, bucket=EODHD_RAW_BUCKET)
    write_client = FakeEodhdClient()
    from dagster_v3.defs.eodhd.source import write_eodhd_reference_snapshot

    write_eodhd_reference_snapshot(
        client=write_client,
        object_store=object_store,
        config=EodhdReferenceConfig(request_delay_seconds=0),
        run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
        log=lambda *_args: None,
    )
    database_path = tmp_path / "eodhd_symbols.duckdb"

    metadata = assets.materialize_eodhd_reference_table(
        table_name=tables.EODHD_SYMBOLS_TABLE,
        database_path=database_path,
        object_store=object_store,
        raw_run_id="run-1",
        log=lambda *_args: None,
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        row = connection.execute(
            "select eodhd_symbol_key, instrument_type, is_delisted "
            "from eodhd.eodhd_symbols order by is_delisted"
        ).fetchall()
    assert row == [("CDR.WAR", "Common Stock", 0), ("OLD.WAR", "Common Stock", 1)]
    assert metadata["row_count"] == 2


def test_reference_completion_rejects_orphan_symbol_mics(monkeypatch) -> None:
    from dagster_v3.defs.eodhd import assets

    class QueryResult:
        def __init__(self, rows: list[tuple[Any, ...]]) -> None:
            self.result_rows = rows

    class Client:
        def query(self, query: str) -> QueryResult:
            if "missing_symbol_mics" in query:
                return QueryResult([("UNKNOWN.US", "XNAS")])
            return QueryResult([("run-1", 2)])

    class ConnectionContext:
        def __enter__(self) -> Client:
            return Client()

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        ClickhouseResource,
        "get_connection",
        lambda _self: ConnectionContext(),
    )

    with pytest.raises(ValueError, match="missing symbols"):
        assets.validate_eodhd_reference_snapshot(ClickhouseResource(host="localhost"))


class FakeEodhdClient:
    def __init__(self) -> None:
        self.symbol_calls: list[tuple[str, bool]] = []
        self.price_calls: list[tuple[str, str, str]] = []

    def exchanges(self) -> list[dict[str, Any]]:
        return [
            {
                "Code": "WAR",
                "Name": "Warsaw Stock Exchange",
                "OperatingMIC": "XWAR",
                "Country": "Poland",
                "Currency": "PLN",
                "CountryISO2": "PL",
                "CountryISO3": "POL",
            }
        ]

    def symbols(self, exchange_code: str, *, delisted: bool) -> list[dict[str, Any]]:
        self.symbol_calls.append((exchange_code, delisted))
        return [
            {
                "Code": "OLD" if delisted else "CDR",
                "Name": "Old SA" if delisted else "CD PROJEKT SA",
                "Country": "Poland",
                "Exchange": "WAR",
                "Currency": "PLN",
                "Type": "Common Stock",
                "Isin": None if delisted else "PLOPTTC00011",
            }
        ]

    def prices(
        self,
        symbol_key: str,
        *,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        self.price_calls.append((symbol_key, start_date, end_date))
        return [
            {
                "date": "2026-07-20",
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "adjusted_close": 103.5,
                "volume": 800_000,
            },
            {
                "date": "2026-07-21",
                "open": 105,
                "high": 112,
                "low": 102,
                "close": 108,
                "adjusted_close": 108,
                "volume": 750_000,
            },
        ]


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, *, Bucket: str) -> None:
        return None

    def put_object(self, *, Bucket: str, Key: str, Body: Any) -> None:
        self.objects[(Bucket, Key)] = Body.encode() if isinstance(Body, str) else Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "NoSuchKey"}}  # type: ignore[attr-defined]
            raise error
        return {}
