import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
import requests
from dagster import AssetKey
from dagster_clickhouse import ClickhouseResource


def eodhd_repository() -> Any:
    from dagster_v3.defs.eodhd import assets

    return dg.Definitions.merge(
        assets.defs,
        dg.Definitions(resources={"clickhouse": ClickhouseResource(host="localhost")}),
    ).get_repository_def()


def test_eodhd_assets_are_registered_one_to_one() -> None:
    from dagster_v3.defs.eodhd import tables

    repository = eodhd_repository()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "eodhd_reference_raw_objects" in asset_keys
    for table_name in tables.EODHD_REFERENCE_TABLES:
        assert f"{table_name}_duckdb" in asset_keys
        assert table_name in asset_keys
    assert "eodhd_reference_complete" in asset_keys
    assert "eodhd_eod_price_legacy_year_objects" not in asset_keys
    assert "eodhd_eod_price_legacy_cleanup" not in asset_keys
    assert "eodhd_eod_price_history_raw_objects" in asset_keys
    assert "eodhd_eod_price_history_duckdb" in asset_keys
    assert "eodhd_eod_price_history_clickhouse" in asset_keys
    assert "eodhd_eod_price_daily_raw_objects" in asset_keys
    assert "eodhd_eod_price_daily_duckdb" in asset_keys
    assert "eodhd_eod_price_daily_clickhouse" in asset_keys


def test_eodhd_asset_dependencies_match_storage_boundaries() -> None:
    from dagster_v3.defs.eodhd import tables

    graph = eodhd_repository().asset_graph

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
    assert graph.get(AssetKey(["eodhd_eod_price_history_raw_objects"])).parent_keys == {
        AssetKey(["eodhd_reference_complete"])
    }
    assert graph.get(AssetKey(["eodhd_eod_price_history_duckdb"])).parent_keys == {
        AssetKey(["eodhd_eod_price_history_raw_objects"])
    }
    assert graph.get(AssetKey(["eodhd_eod_price_history_clickhouse"])).parent_keys == {
        AssetKey(["eodhd_eod_price_history_duckdb"])
    }
    assert graph.get(AssetKey(["eodhd_eod_price_daily_raw_objects"])).parent_keys == {
        AssetKey(["eodhd_reference_complete"])
    }
    assert graph.get(AssetKey(["eodhd_eod_price_daily_duckdb"])).parent_keys == {
        AssetKey(["eodhd_eod_price_daily_raw_objects"])
    }
    assert graph.get(AssetKey(["eodhd_eod_price_daily_clickhouse"])).parent_keys == {
        AssetKey(["eodhd_eod_price_daily_duckdb"])
    }


def test_eodhd_price_assets_are_partitioned_only_by_year_or_date() -> None:
    from dagster_v3.defs.eodhd import assets

    repository = eodhd_repository()
    graph = repository.asset_graph
    assert assets.EODHD_HISTORY_PARTITIONS.get_partition_keys() == [
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
    ]
    assert isinstance(assets.EODHD_DAILY_PARTITIONS, dg.DailyPartitionsDefinition)

    for asset_key in (
        "eodhd_eod_price_history_raw_objects",
        "eodhd_eod_price_history_duckdb",
        "eodhd_eod_price_history_clickhouse",
    ):
        node = graph.get(AssetKey(asset_key))
        assert node.partitions_def is assets.EODHD_HISTORY_PARTITIONS
        assert node.backfill_policy == dg.BackfillPolicy.multi_run(
            max_partitions_per_run=1
        )

    for asset_key in (
        "eodhd_eod_price_daily_raw_objects",
        "eodhd_eod_price_daily_duckdb",
        "eodhd_eod_price_daily_clickhouse",
    ):
        node = graph.get(AssetKey(asset_key))
        assert node.partitions_def is assets.EODHD_DAILY_PARTITIONS

    assert (
        repository.get_job("eodhd_price_history_backfill_job").partitions_def
        is assets.EODHD_HISTORY_PARTITIONS
    )
    assert (
        repository.get_job("eodhd_price_daily_job").partitions_def
        is assets.EODHD_DAILY_PARTITIONS
    )
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "eodhd_price_legacy_migration_job" not in job_names
    assert "eodhd_price_legacy_cleanup_job" not in job_names


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
        partition_key="2021",
        price_asset_kind="history",
    ) == (tmp_path / tables.EODHD_EOD_PRICES_TABLE / "history" / "2021.duckdb")
    assert assets.eodhd_duckdb_path(
        tables.EODHD_EOD_PRICES_TABLE,
        partition_key="2026-07-22",
        price_asset_kind="daily",
    ) == (tmp_path / tables.EODHD_EOD_PRICES_TABLE / "daily" / "2026-07-22.duckdb")
    with pytest.raises(ValueError, match="requires a partition key"):
        assets.eodhd_duckdb_path(tables.EODHD_EOD_PRICES_TABLE)


def test_reference_config_rejects_invalid_limits_and_blank_exchange_codes() -> None:
    from dagster_v3.defs.eodhd.source import (
        EodhdPriceBackfillConfig,
        EodhdReferenceConfig,
    )

    with pytest.raises(ValueError, match="greater than zero"):
        EodhdReferenceConfig(max_exchanges=0)
    with pytest.raises(ValueError, match="must not be blank"):
        EodhdReferenceConfig(exchange_codes_csv="  ")
    with pytest.raises(ValueError, match="greater than zero"):
        EodhdPriceBackfillConfig(max_history_requests_per_run=0)


def test_history_backfill_defaults_are_gentle_and_manual() -> None:
    from dagster_v3.defs.eodhd.source import EodhdPriceBackfillConfig

    config = EodhdPriceBackfillConfig()
    repository = eodhd_repository()
    history_job = repository.get_job("eodhd_price_history_backfill_job")
    schedule_names = {
        schedule.name for schedule in repository.schedule_defs
    }

    assert config.request_delay_seconds == 0.25
    assert config.max_history_requests_per_run == 90_000
    assert history_job.run_tags["dagster/max_runtime"] == "0"
    assert history_job.run_tags["dagster/max_retries"] == "0"
    assert history_job.run_tags["eodhd/backfill_mode"] == "manual_single_partition"
    assert "eodhd_price_history_backfill_schedule" not in schedule_names


def test_history_year_window_clamps_first_and_last_year() -> None:
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EodhdPriceBackfillConfig

    assert assets.eodhd_history_year_window("2020") == (
        "2020-07-01",
        "2020-12-31",
    )
    assert assets.eodhd_history_year_window("2021") == (
        "2021-01-01",
        "2021-12-31",
    )
    assert assets.eodhd_history_year_window("2026") == (
        "2026-01-01",
        "2026-06-30",
    )
    assert "max_workers" not in EodhdPriceBackfillConfig.model_fields
    assert "years" not in EodhdPriceBackfillConfig.model_fields
    assert "start_date" not in EodhdPriceBackfillConfig.model_fields
    assert "end_date" not in EodhdPriceBackfillConfig.model_fields


def test_price_symbol_selection_returns_all_eligible_symbols(
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

    symbols = assets.select_eodhd_price_symbols(config=EodhdPriceBackfillConfig())

    assert [symbol["eodhd_symbol_key"] for symbol in symbols] == [
        "AAPL.US",
        "CDR.WAR",
    ]


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


def test_symbol_rows_keep_provider_rows_with_missing_instrument_type() -> None:
    from dagster_v3.defs.eodhd.source import symbol_rows_from_payload

    symbols = symbol_rows_from_payload(
        [{"Code": "XD4", "Name": "Strabag SE", "Type": None}],
        exchange_code="XETRA",
        is_delisted=False,
        source_run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
    )

    assert symbols[0]["instrument_type"] == "Unknown"


def test_symbol_rows_use_ticker_when_provider_name_is_missing() -> None:
    from dagster_v3.defs.eodhd.source import symbol_rows_from_payload

    symbols = symbol_rows_from_payload(
        [{"Code": "JATT", "Name": None, "Type": "Common Stock"}],
        exchange_code="US",
        is_delisted=False,
        source_run_id="run-1",
        retrieved_at="2026-07-21T10:00:00+00:00",
    )

    assert symbols[0]["symbol_name"] == "JATT"


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


def test_history_year_download_reuses_s3_and_materializes_duckdb_table(
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

    object_key = assets.history_symbol_object_key("2026", "CDR.WAR")
    object_store.write_bytes(
        object_key,
        assets.write_price_envelope(
            symbol_key="CDR.WAR",
            covered_ranges=[("2026-01-01", "2026-06-30")],
            prices=[
                {
                    "date": "2026-06-29",
                    "open": 100,
                    "high": 110,
                    "low": 95,
                    "close": 105,
                    "adjusted_close": 103.5,
                    "volume": 800_000,
                }
            ],
            retrieved_at="2026-07-21T10:00:00+00:00",
            source_object_keys=["legacy/CDR.WAR.json.gz"],
        ),
        bucket=EODHD_RAW_BUCKET,
    )

    download_metadata = assets.download_eodhd_history_year(
        client=client,
        object_store=object_store,
        symbols=symbols,
        year="2026",
        request_delay_seconds=0,
        max_requests=90_000,
        progress_interval=1,
        log=lambda *_args: None,
    )
    database_path = tmp_path / "eodhd_eod_prices.duckdb"
    materialize_metadata = assets.materialize_eodhd_history_year(
        database_path=database_path,
        object_store=object_store,
        symbols=symbols,
        year="2026",
        progress_interval=1,
        log=lambda *_args: None,
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            "select eodhd_symbol_key, price_date, close, currency, source_run_id "
            "from eodhd.eodhd_eod_prices order by price_date"
        ).fetchall()

    assert client.price_calls == []
    assert download_metadata["symbol_count"] == 1
    assert download_metadata["year"] == "2026"
    assert download_metadata["reused_symbol_count"] == 1
    assert download_metadata["downloaded_request_count"] == 0
    assert materialize_metadata["row_count"] == 1
    assert materialize_metadata["year"] == "2026"
    assert rows == [
        (
            "CDR.WAR",
            datetime(2026, 6, 29).date(),
            105.0,
            "PLN",
            "history:2026",
        ),
    ]


def test_empty_history_year_materializes_as_successful_empty_table(
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
    download_metadata = assets.download_eodhd_history_year(
        client=client,
        object_store=object_store,
        symbols=[],
        year="2020",
        request_delay_seconds=0,
        max_requests=90_000,
        progress_interval=1,
        log=lambda *_args: None,
    )
    database_path = tmp_path / "bucket_000.duckdb"

    materialize_metadata = assets.materialize_eodhd_history_year(
        database_path=database_path,
        object_store=object_store,
        symbols=[],
        year="2020",
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


def test_history_request_budget_saves_progress_and_resumes_next_run(
    monkeypatch,
) -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    object_store = ObjectStoreResource(
        s3_client=FakeS3Client(),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()
    symbols = [
        {
            "eodhd_symbol_key": symbol_key,
            "exchange_code": "WAR",
            "ticker": symbol_key.split(".", maxsplit=1)[0],
            "currency": "PLN",
        }
        for symbol_key in ("AAA.WAR", "BBB.WAR", "CCC.WAR")
    ]
    sleep_calls: list[float] = []
    monkeypatch.setattr(assets.time, "sleep", sleep_calls.append)

    with pytest.raises(dg.Failure, match="Relaunch this same partition"):
        assets.download_eodhd_history_year(
            client=client,
            object_store=object_store,
            symbols=symbols,
            year="2026",
            request_delay_seconds=0.25,
            max_requests=2,
            progress_interval=1,
            log=lambda *_args: None,
        )

    assert [call[0] for call in client.price_calls] == ["AAA.WAR", "BBB.WAR"]
    assert sleep_calls == [0.25]
    assert object_store.exists(
        assets.history_symbol_object_key("2026", "AAA.WAR"),
        bucket=EODHD_RAW_BUCKET,
    )
    assert object_store.exists(
        assets.history_symbol_object_key("2026", "BBB.WAR"),
        bucket=EODHD_RAW_BUCKET,
    )
    assert not object_store.exists(
        assets.history_symbol_object_key("2026", "CCC.WAR"),
        bucket=EODHD_RAW_BUCKET,
    )

    metadata = assets.download_eodhd_history_year(
        client=client,
        object_store=object_store,
        symbols=symbols,
        year="2026",
        request_delay_seconds=0.25,
        max_requests=2,
        progress_interval=1,
        log=lambda *_args: None,
    )

    assert [call[0] for call in client.price_calls] == [
        "AAA.WAR",
        "BBB.WAR",
        "CCC.WAR",
    ]
    assert sleep_calls == [0.25]
    assert metadata["reused_symbol_count"] == 2
    assert metadata["downloaded_request_count"] == 1


def test_history_request_budget_checkpoints_each_missing_range() -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    object_store = ObjectStoreResource(
        s3_client=FakeS3Client(),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()
    symbols = [
        {
            "eodhd_symbol_key": "AAA.WAR",
            "exchange_code": "WAR",
            "ticker": "AAA",
            "currency": "PLN",
        }
    ]
    object_key = assets.history_symbol_object_key("2026", "AAA.WAR")
    object_store.write_bytes(
        object_key,
        assets.write_price_envelope(
            symbol_key="AAA.WAR",
            covered_ranges=[("2026-03-01", "2026-03-31")],
            prices=[],
            retrieved_at="2026-07-21T10:00:00+00:00",
            source_object_keys=[],
        ),
        bucket=EODHD_RAW_BUCKET,
    )

    with pytest.raises(dg.Failure, match="request budget reached"):
        assets.download_eodhd_history_year(
            client=client,
            object_store=object_store,
            symbols=symbols,
            year="2026",
            request_delay_seconds=0,
            max_requests=1,
            progress_interval=1,
            log=lambda *_args: None,
        )

    checkpoint = assets.read_price_envelope(
        object_store.read_bytes(object_key, bucket=EODHD_RAW_BUCKET)
    )
    assert assets._envelope_ranges(checkpoint) == [
        ("2026-01-01", "2026-03-31")
    ]
    assert client.price_calls == [
        ("AAA.WAR", "2026-01-01", "2026-02-28"),
    ]

    metadata = assets.download_eodhd_history_year(
        client=client,
        object_store=object_store,
        symbols=symbols,
        year="2026",
        request_delay_seconds=0,
        max_requests=90_000,
        progress_interval=1,
        log=lambda *_args: None,
    )

    assert client.price_calls == [
        ("AAA.WAR", "2026-01-01", "2026-02-28"),
        ("AAA.WAR", "2026-04-01", "2026-06-30"),
    ]
    assert metadata["downloaded_request_count"] == 1


def test_history_interruption_resumes_from_durable_symbol_objects(
    monkeypatch,
) -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    object_store = ObjectStoreResource(
        s3_client=FakeS3Client(),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()
    symbols = [
        {
            "eodhd_symbol_key": symbol_key,
            "exchange_code": "WAR",
            "ticker": symbol_key.split(".", maxsplit=1)[0],
            "currency": "PLN",
        }
        for symbol_key in ("AAA.WAR", "BBB.WAR", "CCC.WAR")
    ]
    uninterrupted_prices = client.prices

    def interrupt_on_third_symbol(
        symbol_key: str,
        *,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        if symbol_key == "CCC.WAR":
            raise RuntimeError("worker interrupted")
        return uninterrupted_prices(
            symbol_key,
            start_date=start_date,
            end_date=end_date,
        )

    monkeypatch.setattr(client, "prices", interrupt_on_third_symbol)
    with pytest.raises(RuntimeError, match="worker interrupted"):
        assets.download_eodhd_history_year(
            client=client,
            object_store=object_store,
            symbols=symbols,
            year="2026",
            request_delay_seconds=0,
            max_requests=90_000,
            progress_interval=1,
            log=lambda *_args: None,
        )

    assert [call[0] for call in client.price_calls] == ["AAA.WAR", "BBB.WAR"]
    for symbol_key in ("AAA.WAR", "BBB.WAR"):
        assert object_store.exists(
            assets.history_symbol_object_key("2026", symbol_key),
            bucket=EODHD_RAW_BUCKET,
        )
    assert not object_store.exists(
        assets.history_catalog_object_key("2026"),
        bucket=EODHD_RAW_BUCKET,
    )

    monkeypatch.setattr(client, "prices", uninterrupted_prices)
    metadata = assets.download_eodhd_history_year(
        client=client,
        object_store=object_store,
        symbols=symbols,
        year="2026",
        request_delay_seconds=0,
        max_requests=90_000,
        progress_interval=1,
        log=lambda *_args: None,
    )

    assert [call[0] for call in client.price_calls] == [
        "AAA.WAR",
        "BBB.WAR",
        "CCC.WAR",
    ]
    assert metadata["reused_symbol_count"] == 2
    assert metadata["downloaded_request_count"] == 1
    assert object_store.exists(
        assets.history_catalog_object_key("2026"),
        bucket=EODHD_RAW_BUCKET,
    )


def test_daily_download_reuses_complete_s3_date_without_http() -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

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
    object_store.write_bytes(
        assets.daily_prices_object_key("2026-07-22"),
        assets.write_daily_envelope(
            price_date="2026-07-22",
            covered_symbols=["CDR.WAR"],
            prices=[
                {
                    "eodhd_symbol_key": "CDR.WAR",
                    "date": "2026-07-22",
                    "close": 105,
                }
            ],
            retrieved_at="2026-07-23T04:00:00+00:00",
            source_object_keys=[],
        ),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()

    metadata = assets.download_eodhd_daily_date(
        client=client,
        object_store=object_store,
        symbols=symbols,
        price_date="2026-07-22",
        request_delay_seconds=0,
        progress_interval=1,
        log=lambda *_args: None,
    )

    assert client.price_calls == []
    assert client.bulk_price_calls == []
    assert metadata["reused_symbol_count"] == 1
    assert metadata["downloaded_request_count"] == 0


def test_daily_download_uses_one_full_exchange_request_then_reuses_s3() -> None:
    from dagster_v3.defs.common.resources import ObjectStoreResource
    from dagster_v3.defs.eodhd import assets
    from dagster_v3.defs.eodhd.source import EODHD_RAW_BUCKET

    object_store = ObjectStoreResource(
        s3_client=FakeS3Client(),
        bucket=EODHD_RAW_BUCKET,
    )
    client = FakeEodhdClient()
    symbols = [
        {
            "eodhd_symbol_key": "CDR.WAR",
            "exchange_code": "WAR",
            "ticker": "CDR",
            "currency": "PLN",
        }
    ]

    first = assets.download_eodhd_daily_date(
        client=client,
        object_store=object_store,
        symbols=symbols,
        price_date="2026-07-22",
        request_delay_seconds=0,
        progress_interval=1,
        log=lambda *_args: None,
    )
    second = assets.download_eodhd_daily_date(
        client=client,
        object_store=object_store,
        symbols=symbols,
        price_date="2026-07-22",
        request_delay_seconds=0,
        progress_interval=1,
        log=lambda *_args: None,
    )

    assert client.bulk_price_calls == [("WAR", "2026-07-22", None)]
    assert first["downloaded_request_count"] == 1
    assert second["downloaded_request_count"] == 0


def test_eodhd_bulk_price_resource_passes_date_and_optional_symbols() -> None:
    from dagster_v3.defs.eodhd.resources import EodhdResource

    session = FakeHttpSession()
    resource = EodhdResource(api_token="token", session=session)

    resource.bulk_prices("WAR", price_date="2026-07-22")
    resource.bulk_prices(
        "WAR",
        price_date="2026-07-22",
        symbol_keys=["CDR.WAR"],
    )

    assert session.calls[0][0].endswith("/eod-bulk-last-day/WAR")
    assert session.calls[0][1] == {
        "date": "2026-07-22",
        "api_token": "token",
        "fmt": "json",
    }
    assert session.calls[1][1]["symbols"] == "CDR.WAR"


def test_eodhd_http_errors_do_not_expose_api_token() -> None:
    from dagster_v3.defs.eodhd.resources import EodhdApiError, EodhdResource

    api_token = "secret-token"
    response = FakeHttpResponse(
        status_code=402,
        text=(
            "You exceeded your daily API requests limit. "
            f"Do not echo {api_token}."
        ),
    )
    resource = EodhdResource(
        api_token=api_token,
        session=FakeHttpSession(response=response),
    )

    with pytest.raises(EodhdApiError) as error:
        resource.prices(
            "AAA.WAR",
            start_date="2026-01-01",
            end_date="2026-06-30",
        )

    assert "HTTP 402" in str(error.value)
    assert "daily API requests limit" in str(error.value)
    assert api_token not in str(error.value)


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

    class Client:
        def execute(self, query: str) -> list[tuple[Any, ...]]:
            if "missing_symbol_mics" in query:
                return [("UNKNOWN.US", "XNAS")]
            return [("run-1", 2)]

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
        self.bulk_price_calls: list[tuple[str, str, tuple[str, ...] | None]] = []

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

    def bulk_prices(
        self,
        exchange_code: str,
        *,
        price_date: str,
        symbol_keys: list[str] | None,
    ) -> list[dict[str, Any]]:
        self.bulk_price_calls.append(
            (
                exchange_code,
                price_date,
                tuple(symbol_keys) if symbol_keys is not None else None,
            )
        )
        return []


class FakeHttpResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(response=response)
        return None

    def json(self) -> list[dict[str, Any]]:
        return []


class FakeHttpSession:
    def __init__(self, *, response: FakeHttpResponse | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.response = response or FakeHttpResponse()

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> FakeHttpResponse:
        assert headers["Accept"] == "application/json"
        assert timeout > 0
        self.calls.append((url, params))
        return self.response


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

    def get_paginator(self, operation_name: str) -> "FakeS3Paginator":
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self)

    def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> None:
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]), None)


class FakeS3Paginator:
    def __init__(self, client: FakeS3Client) -> None:
        self.client = client

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
        keys = sorted(
            key
            for bucket, key in self.client.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        return [{"Contents": [{"Key": key} for key in keys]}]
