import time
from collections.abc import Callable, Iterable
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import dagster as dg
import duckdb
import pyarrow as pa
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.eodhd import tables
from dagster_v3.defs.eodhd.resources import EodhdResource
from dagster_v3.defs.eodhd.source import (
    EODHD_RAW_BUCKET,
    EodhdPriceBackfillConfig,
    EodhdRawRunConfig,
    EodhdReferenceConfig,
    exchange_rows_from_payload,
    price_rows_from_payload,
    read_json_gzip,
    read_reference_snapshot,
    reference_snapshot_object_key,
    resolve_symbol_mic_rows,
    symbol_rows_from_payload,
    utc_now_iso,
    write_eodhd_reference_snapshot,
    write_json_gzip,
)

GROUP_NAME = "eodhd"
EODHD_DUCKDB_SCHEMA = "eodhd"
EODHD_DUCKDB_DIRECTORY = Path("data/eodhd")
EODHD_CLICKHOUSE_POOL = "eodhd_clickhouse"
EODHD_PRICE_DOWNLOAD_POOL = "eodhd_price_download"
DUCKDB_INSERT_BATCH_SIZE = 5_000
EODHD_DAILY_SYMBOL_CHUNK_SIZE = 500
EODHD_HISTORY_START_DATE = date(2020, 7, 1)
EODHD_HISTORY_END_DATE = date(2026, 6, 30)
EODHD_DAILY_START_DATE = date(2026, 7, 1)
EODHD_HISTORY_YEARS = tuple(
    str(year)
    for year in range(EODHD_HISTORY_START_DATE.year, EODHD_HISTORY_END_DATE.year + 1)
)
EODHD_HISTORY_PARTITIONS = dg.StaticPartitionsDefinition(list(EODHD_HISTORY_YEARS))
EODHD_DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date=EODHD_DAILY_START_DATE.isoformat(),
    timezone="UTC",
)
EODHD_PRICE_BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "rest", "s3", "eodhd"},
    pool=EODHD_PRICE_DOWNLOAD_POOL,
    description=(
        "Downloads the global EODHD exchange list plus active and delisted symbol "
        "lists into a completed, run-scoped S3 snapshot."
    ),
)
def eodhd_reference_raw_objects(
    context: dg.AssetExecutionContext,
    config: EodhdReferenceConfig,
    eodhd: EodhdResource,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    started_at = time.monotonic()
    retrieved_at = utc_now_iso()
    context.log.info("Starting EODHD global reference download")
    metadata = write_eodhd_reference_snapshot(
        client=eodhd,
        object_store=eodhd_object_store,
        config=config,
        run_id=context.run.run_id,
        retrieved_at=retrieved_at,
        log=context.log.info,
    )
    metadata["elapsed_seconds"] = round(time.monotonic() - started_at, 3)
    context.log.info(
        "Finished EODHD reference download: exchanges=%s active_symbols=%s "
        "delisted_symbols=%s elapsed_seconds=%s",
        metadata["exchange_count"],
        metadata["active_symbol_count"],
        metadata["delisted_symbol_count"],
        metadata["elapsed_seconds"],
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    deps=["eodhd_reference_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    description="Normalizes the EODHD exchange snapshot into its own DuckDB database.",
)
def eodhd_exchanges_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdRawRunConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _reference_table_result(
        context,
        table_name=tables.EODHD_EXCHANGES_TABLE,
        config=config,
        object_store=eodhd_object_store,
    )


@dg.asset(
    deps=["eodhd_reference_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    description="Splits provider exchange MIC strings into one normalized row per MIC.",
)
def eodhd_exchange_mics_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdRawRunConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _reference_table_result(
        context,
        table_name=tables.EODHD_EXCHANGE_MICS_TABLE,
        config=config,
        object_store=eodhd_object_store,
    )


@dg.asset(
    deps=["eodhd_reference_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    description=(
        "Normalizes active and delisted EODHD listings into one symbol table, "
        "preferring an active row when snapshots overlap."
    ),
)
def eodhd_symbols_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdRawRunConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _reference_table_result(
        context,
        table_name=tables.EODHD_SYMBOLS_TABLE,
        config=config,
        object_store=eodhd_object_store,
    )


@dg.asset(
    deps=["eodhd_exchange_mics_duckdb", "eodhd_symbols_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "eodhd"},
    description=(
        "Resolves every EODHD listing to one or more ISO MIC candidates with "
        "explicit method and confidence."
    ),
)
def eodhd_symbol_mics_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    metadata = materialize_eodhd_symbol_mics(log=context.log.info)
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name=tables.EODHD_EXCHANGES_TABLE,
    deps=["eodhd_exchanges_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_exchanges_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_EXCHANGES_TABLE,
        truncate=True,
    )


@dg.asset(
    name=tables.EODHD_EXCHANGE_MICS_TABLE,
    deps=["eodhd_exchange_mics_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_exchange_mics_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_EXCHANGE_MICS_TABLE,
        truncate=True,
    )


@dg.asset(
    name=tables.EODHD_SYMBOLS_TABLE,
    deps=["eodhd_symbols_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_symbols_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_SYMBOLS_TABLE,
        truncate=True,
    )


@dg.asset(
    name=tables.EODHD_SYMBOL_MICS_TABLE,
    deps=["eodhd_symbol_mics_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_symbol_mics_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_SYMBOL_MICS_TABLE,
        truncate=True,
    )


@dg.asset(
    deps=[dg.AssetKey(table_name) for table_name in tables.EODHD_REFERENCE_TABLES],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "eodhd"},
    description=(
        "Verifies that all EODHD reference tables belong to one source snapshot "
        "and contain valid listing-to-MIC relationships."
    ),
)
def eodhd_reference_complete(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return validate_eodhd_reference_snapshot(clickhouse)


@dg.asset(
    deps=["eodhd_reference_complete"],
    group_name=GROUP_NAME,
    kinds={"python", "rest", "s3", "eodhd"},
    partitions_def=EODHD_HISTORY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_PRICE_DOWNLOAD_POOL,
)
def eodhd_eod_price_history_raw_objects(
    context: dg.AssetExecutionContext,
    config: EodhdPriceBackfillConfig,
    eodhd: EodhdResource,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    symbols = select_eodhd_price_symbols(config=config)
    return dg.MaterializeResult(
        metadata=download_eodhd_history_year(
            client=eodhd,
            object_store=eodhd_object_store,
            symbols=symbols,
            year=context.partition_key,
            request_delay_seconds=config.request_delay_seconds,
            progress_interval=config.progress_interval,
            log=context.log.info,
        )
    )


@dg.asset(
    deps=["eodhd_eod_price_history_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    partitions_def=EODHD_HISTORY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_eod_price_history_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdPriceBackfillConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=materialize_eodhd_history_year(
            database_path=eodhd_duckdb_path(
                tables.EODHD_EOD_PRICES_TABLE,
                partition_key=context.partition_key,
                price_asset_kind="history",
            ),
            object_store=eodhd_object_store,
            symbols=select_eodhd_price_symbols(config=config),
            year=context.partition_key,
            progress_interval=config.progress_interval,
            log=context.log.info,
        )
    )


@dg.asset(
    deps=["eodhd_eod_price_history_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    partitions_def=EODHD_HISTORY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_eod_price_history_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_EOD_PRICES_TABLE,
        truncate=False,
        partition_key=context.partition_key,
        price_asset_kind="history",
    )


@dg.asset(
    deps=["eodhd_reference_complete"],
    group_name=GROUP_NAME,
    kinds={"python", "rest", "s3", "eodhd"},
    partitions_def=EODHD_DAILY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_PRICE_DOWNLOAD_POOL,
)
def eodhd_eod_price_daily_raw_objects(
    context: dg.AssetExecutionContext,
    config: EodhdPriceBackfillConfig,
    eodhd: EodhdResource,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    symbols = select_eodhd_price_symbols(config=config, include_delisted=False)
    return dg.MaterializeResult(
        metadata=download_eodhd_daily_date(
            client=eodhd,
            object_store=eodhd_object_store,
            symbols=symbols,
            price_date=context.partition_key,
            request_delay_seconds=config.request_delay_seconds,
            progress_interval=config.progress_interval,
            log=context.log.info,
        )
    )


@dg.asset(
    deps=["eodhd_eod_price_daily_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    partitions_def=EODHD_DAILY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_eod_price_daily_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdPriceBackfillConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=materialize_eodhd_daily_date(
            database_path=eodhd_duckdb_path(
                tables.EODHD_EOD_PRICES_TABLE,
                partition_key=context.partition_key,
                price_asset_kind="daily",
            ),
            object_store=eodhd_object_store,
            symbols=select_eodhd_price_symbols(
                config=config,
                include_delisted=False,
            ),
            price_date=context.partition_key,
            log=context.log.info,
        )
    )


@dg.asset(
    deps=["eodhd_eod_price_daily_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    partitions_def=EODHD_DAILY_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_CLICKHOUSE_POOL,
)
def eodhd_eod_price_daily_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_EOD_PRICES_TABLE,
        truncate=False,
        partition_key=context.partition_key,
        price_asset_kind="daily",
    )


def eodhd_duckdb_path(
    table_name: str,
    *,
    partition_key: str | None = None,
    price_asset_kind: str | None = None,
) -> Path:
    supported_tables = {*tables.EODHD_REFERENCE_TABLES, tables.EODHD_EOD_PRICES_TABLE}
    if table_name not in supported_tables:
        raise ValueError(f"Unsupported EODHD table: {table_name}")
    if table_name == tables.EODHD_EOD_PRICES_TABLE:
        if partition_key is None:
            raise ValueError("EODHD price DuckDB path requires a partition key")
        if price_asset_kind not in {"history", "daily"}:
            raise ValueError("EODHD price DuckDB path requires history or daily kind")
        return (
            EODHD_DUCKDB_DIRECTORY
            / table_name
            / price_asset_kind
            / f"{partition_key}.duckdb"
        )
    if partition_key is not None:
        raise ValueError(f"EODHD reference table {table_name} is not partitioned")
    if price_asset_kind is not None:
        raise ValueError(f"EODHD reference table {table_name} has no price kind")
    return EODHD_DUCKDB_DIRECTORY / f"{table_name}.duckdb"


def materialize_eodhd_reference_table(
    *,
    table_name: str,
    database_path: Path,
    object_store: ObjectStoreResource,
    raw_run_id: str,
    log: Callable[..., object],
) -> dict[str, Any]:
    if table_name not in {
        tables.EODHD_EXCHANGES_TABLE,
        tables.EODHD_EXCHANGE_MICS_TABLE,
        tables.EODHD_SYMBOLS_TABLE,
    }:
        raise ValueError(f"Unsupported raw EODHD reference table: {table_name}")
    snapshot = read_reference_snapshot(object_store, run_id=raw_run_id)
    retrieved_at = str(snapshot["retrieved_at"])
    objects = snapshot["objects"]
    exchange_object = next(
        (item for item in objects if item.get("kind") == "exchanges"),
        None,
    )
    if exchange_object is None:
        raise ValueError("EODHD reference snapshot is missing the exchange object")

    if table_name in {
        tables.EODHD_EXCHANGES_TABLE,
        tables.EODHD_EXCHANGE_MICS_TABLE,
    }:
        exchange_payload = read_json_gzip(
            object_store.read_bytes(
                str(exchange_object["object_key"]),
                bucket=EODHD_RAW_BUCKET,
            )
        )
        exchange_rows, mic_rows = exchange_rows_from_payload(
            exchange_payload,
            source_run_id=raw_run_id,
            retrieved_at=retrieved_at,
        )
        rows = exchange_rows if table_name == tables.EODHD_EXCHANGES_TABLE else mic_rows
    else:
        rows_by_key: dict[str, dict[str, Any]] = {}
        symbol_objects = [
            item for item in objects if str(item.get("kind", "")).startswith("symbols_")
        ]
        for object_number, item in enumerate(symbol_objects, start=1):
            payload = read_json_gzip(
                object_store.read_bytes(
                    str(item["object_key"]),
                    bucket=EODHD_RAW_BUCKET,
                )
            )
            symbol_rows = symbol_rows_from_payload(
                payload,
                exchange_code=str(item["exchange_code"]),
                is_delisted=bool(item["is_delisted"]),
                source_run_id=raw_run_id,
                retrieved_at=retrieved_at,
            )
            for row in symbol_rows:
                existing = rows_by_key.get(row["eodhd_symbol_key"])
                if existing is None or row["is_delisted"] < existing["is_delisted"]:
                    rows_by_key[row["eodhd_symbol_key"]] = row
            if object_number == 1 or object_number % 25 == 0:
                log(
                    "Parsed EODHD symbol objects: progress=%s/%s unique_symbols=%s",
                    object_number,
                    len(symbol_objects),
                    len(rows_by_key),
                )
        rows = list(rows_by_key.values())

    row_count = replace_eodhd_duckdb_table(
        database_path=database_path,
        table_name=table_name,
        rows=rows,
        log=log,
    )
    return {
        "source_run_id": raw_run_id,
        "duckdb_path": str(database_path),
        "duckdb_schema": EODHD_DUCKDB_SCHEMA,
        "duckdb_table": table_name,
        "row_count": row_count,
    }


def materialize_eodhd_symbol_mics(
    *,
    log: Callable[..., object],
) -> dict[str, Any]:
    symbol_database = eodhd_duckdb_path(tables.EODHD_SYMBOLS_TABLE)
    mic_database = eodhd_duckdb_path(tables.EODHD_EXCHANGE_MICS_TABLE)
    if not symbol_database.exists() or not mic_database.exists():
        raise ValueError(
            "Missing EODHD symbol or exchange-MIC DuckDB database; materialize "
            "eodhd_symbols_duckdb and eodhd_exchange_mics_duckdb first"
        )
    with duckdb.connect(str(symbol_database), read_only=True) as connection:
        symbol_columns = (
            "eodhd_symbol_key",
            "exchange_code",
            "reported_exchange_code",
        )
        symbol_rows = [
            dict(zip(symbol_columns, row, strict=True))
            for row in connection.execute(
                "select eodhd_symbol_key, exchange_code, reported_exchange_code "
                "from eodhd.eodhd_symbols order by eodhd_symbol_key"
            ).fetchall()
        ]
        symbol_run_ids = _duckdb_source_run_ids(
            connection,
            table_name=tables.EODHD_SYMBOLS_TABLE,
        )
    with duckdb.connect(str(mic_database), read_only=True) as connection:
        mic_rows = connection.execute(
            "select exchange_code, mic from eodhd.eodhd_exchange_mics "
            "order by exchange_code, mic"
        ).fetchall()
        mic_run_ids = _duckdb_source_run_ids(
            connection,
            table_name=tables.EODHD_EXCHANGE_MICS_TABLE,
        )
    if len(symbol_run_ids) != 1 or symbol_run_ids != mic_run_ids:
        raise ValueError(
            "EODHD symbol and exchange-MIC DuckDB tables do not share one source_run_id"
        )
    source_run_id = next(iter(symbol_run_ids))
    exchange_mics: dict[str, list[str]] = {}
    for exchange_code, mic in mic_rows:
        exchange_mics.setdefault(str(exchange_code), []).append(str(mic))
    resolved_at = utc_now_iso()
    rows = resolve_symbol_mic_rows(
        symbols=symbol_rows,
        exchange_mics={key: tuple(value) for key, value in exchange_mics.items()},
        source_run_id=source_run_id,
        resolved_at=resolved_at,
    )
    row_count = replace_eodhd_duckdb_table(
        database_path=eodhd_duckdb_path(tables.EODHD_SYMBOL_MICS_TABLE),
        table_name=tables.EODHD_SYMBOL_MICS_TABLE,
        rows=rows,
        log=log,
    )
    resolved_symbol_count = len({row["eodhd_symbol_key"] for row in rows})
    log(
        "Resolved EODHD symbol MICs: symbols=%s resolved_symbols=%s rows=%s",
        len(symbol_rows),
        resolved_symbol_count,
        row_count,
    )
    return {
        "source_run_id": source_run_id,
        "duckdb_path": str(eodhd_duckdb_path(tables.EODHD_SYMBOL_MICS_TABLE)),
        "duckdb_schema": EODHD_DUCKDB_SCHEMA,
        "duckdb_table": tables.EODHD_SYMBOL_MICS_TABLE,
        "row_count": row_count,
        "resolved_symbol_count": resolved_symbol_count,
        "unresolved_symbol_count": len(symbol_rows) - resolved_symbol_count,
    }


def replace_eodhd_duckdb_table(
    *,
    database_path: Path,
    table_name: str,
    rows: Iterable[dict[str, Any]],
    log: Callable[..., object],
) -> int:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    columns = tables.EODHD_TABLE_COLUMNS[table_name]
    column_types = tables.EODHD_DUCKDB_COLUMN_TYPES[table_name]
    column_definitions = ", ".join(
        f'"{column}" {column_types[column]}' for column in columns
    )
    qualified_table = f'{EODHD_DUCKDB_SCHEMA}."{table_name}"'
    row_count = 0
    batch: list[dict[str, Any]] = []
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {EODHD_DUCKDB_SCHEMA}")
        connection.execute(
            f"create or replace table {qualified_table} ({column_definitions})"
        )
        for row in rows:
            batch.append({column: row[column] for column in columns})
            if len(batch) >= DUCKDB_INSERT_BATCH_SIZE:
                _insert_eodhd_duckdb_batch(
                    connection=connection,
                    qualified_table=qualified_table,
                    columns=columns,
                    rows=batch,
                )
                row_count += len(batch)
                log("Loaded EODHD DuckDB rows: table=%s rows=%s", table_name, row_count)
                batch.clear()
        if batch:
            _insert_eodhd_duckdb_batch(
                connection=connection,
                qualified_table=qualified_table,
                columns=columns,
                rows=batch,
            )
            row_count += len(batch)
    if row_count == 0:
        raise ValueError(f"EODHD table {table_name} produced zero rows")
    log("Finished EODHD DuckDB table: table=%s rows=%s", table_name, row_count)
    return row_count


def _insert_eodhd_duckdb_batch(
    *,
    connection: duckdb.DuckDBPyConnection,
    qualified_table: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    registered_name = "_eodhd_rows"
    column_list = ", ".join(f'"{column}"' for column in columns)
    connection.register(registered_name, pa.Table.from_pylist(rows))
    try:
        connection.execute(
            f"insert into {qualified_table} ({column_list}) "
            f"select {column_list} from {registered_name}"
        )
    finally:
        connection.unregister(registered_name)


def export_eodhd_table_to_clickhouse(
    context: dg.AssetExecutionContext,
    *,
    clickhouse: ClickhouseResource,
    table_name: str,
    truncate: bool,
    partition_key: str | None = None,
    price_asset_kind: str | None = None,
) -> dg.MaterializeResult:
    database_path = eodhd_duckdb_path(
        table_name,
        partition_key=partition_key,
        price_asset_kind=price_asset_kind,
    )
    if not database_path.exists():
        raise ValueError(
            f"Missing EODHD DuckDB database {database_path}; materialize "
            f"{table_name}_duckdb first"
        )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(table_name,),
    )
    with (
        duckdb.connect(str(database_path), read_only=True) as duckdb_connection,
        clickhouse.get_connection() as clickhouse_client,
    ):
        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=EODHD_DUCKDB_SCHEMA,
            duckdb_table=table_name,
            clickhouse_database=RESOLVED_DATABASE,
            clickhouse_table=table_name,
            columns=tables.EODHD_TABLE_COLUMNS[table_name],
            truncate=truncate,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "clickhouse_database": RESOLVED_DATABASE,
            "clickhouse_table": table_name,
            "duckdb_path": str(database_path),
            "row_count": row_count,
            "publish_mode": "replace" if truncate else "append",
            **({"partition_key": partition_key} if partition_key is not None else {}),
        }
    )


def validate_eodhd_reference_snapshot(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    source_run_ids: set[str] = set()
    row_counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for table_name in tables.EODHD_REFERENCE_TABLES:
            rows = client.execute(
                f"select source_run_id, count() from {RESOLVED_DATABASE}.{table_name} "
                "group by source_run_id order by source_run_id limit 2"
            )
            if len(rows) != 1:
                raise ValueError(
                    f"ClickHouse table {table_name} does not contain exactly one source_run_id"
                )
            source_run_ids.add(str(rows[0][0]))
            row_counts[table_name] = int(rows[0][1])
        orphan_symbol_mics = client.execute(
            "/* missing_symbol_mics */ "
            f"select mappings.eodhd_symbol_key, mappings.mic "
            f"from {RESOLVED_DATABASE}.{tables.EODHD_SYMBOL_MICS_TABLE} as mappings "
            f"left join {RESOLVED_DATABASE}.{tables.EODHD_SYMBOLS_TABLE} as symbols "
            "on symbols.eodhd_symbol_key = mappings.eodhd_symbol_key "
            "where symbols.eodhd_symbol_key is null limit 10"
        )
        invalid_mics = client.execute(
            f"select mic from {RESOLVED_DATABASE}.{tables.EODHD_SYMBOL_MICS_TABLE} "
            "where not match(mic, '^[A-Z0-9]{4}$') order by mic limit 10"
        )
    if orphan_symbol_mics:
        values = ", ".join(str(row[0]) for row in orphan_symbol_mics)
        raise ValueError(f"EODHD MIC mappings reference missing symbols: {values}")
    if invalid_mics:
        values = ", ".join(str(row[0]) for row in invalid_mics)
        raise ValueError(f"EODHD symbol mappings contain invalid MICs: {values}")
    if len(source_run_ids) != 1:
        raise ValueError(
            "EODHD reference tables contain different source_run_id values: "
            f"{sorted(source_run_ids)}"
        )
    source_run_id = next(iter(source_run_ids))
    return dg.MaterializeResult(
        metadata={
            "source_run_id": source_run_id,
            **{
                f"{table_name}_row_count": row_count
                for table_name, row_count in row_counts.items()
            },
        }
    )


def select_eodhd_price_symbols(
    *,
    config: EodhdPriceBackfillConfig,
    include_delisted: bool | None = None,
) -> list[dict[str, Any]]:
    database_path = eodhd_duckdb_path(tables.EODHD_SYMBOLS_TABLE)
    if not database_path.exists():
        raise ValueError(
            "Missing EODHD symbols DuckDB database; materialize eodhd_symbols_duckdb first"
        )
    with duckdb.connect(str(database_path), read_only=True) as connection:
        placeholders = ", ".join("?" for _ in config.instrument_types())
        should_include_delisted = (
            config.include_delisted if include_delisted is None else include_delisted
        )
        where_delisted = "" if should_include_delisted else "and is_delisted = 0"
        query = (
            "select eodhd_symbol_key, exchange_code, ticker, currency "
            "from eodhd.eodhd_symbols "
            f"where instrument_type in ({placeholders}) {where_delisted} "
            "order by eodhd_symbol_key"
        )
        rows = connection.execute(query, config.instrument_types()).fetchall()
    if config.max_symbols is not None:
        rows = rows[: config.max_symbols]
    columns = ("eodhd_symbol_key", "exchange_code", "ticker", "currency")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def eodhd_history_year_window(year: str) -> tuple[str, str]:
    if year not in EODHD_HISTORY_YEARS:
        raise ValueError(f"Unsupported EODHD history year: {year}")
    year_number = int(year)
    start = max(EODHD_HISTORY_START_DATE, date(year_number, 1, 1))
    end = min(EODHD_HISTORY_END_DATE, date(year_number, 12, 31))
    return start.isoformat(), end.isoformat()


def history_symbol_object_key(year: str, symbol_key: str) -> str:
    eodhd_history_year_window(year)
    return f"prices/history/year={year}/symbols/{quote(symbol_key, safe='')}.json.gz"


def history_catalog_object_key(year: str) -> str:
    eodhd_history_year_window(year)
    return f"prices/history/year={year}/catalog.json.gz"


def daily_prices_object_key(price_date: str) -> str:
    parsed_date = date.fromisoformat(price_date)
    if parsed_date < EODHD_DAILY_START_DATE:
        raise ValueError(f"EODHD daily date is before cutover: {price_date}")
    return f"prices/daily/date={price_date}/prices.json.gz"


def write_price_envelope(
    *,
    symbol_key: str,
    covered_ranges: list[tuple[str, str]],
    prices: list[dict[str, Any]],
    retrieved_at: str,
    source_object_keys: list[str],
) -> bytes:
    return write_json_gzip(
        {
            "schema_version": 2,
            "symbol_key": symbol_key,
            "covered_ranges": [
                {"start": start, "end": end} for start, end in covered_ranges
            ],
            "prices": sorted(prices, key=lambda row: str(row["date"])),
            "retrieved_at": retrieved_at,
            "source_object_keys": sorted(set(source_object_keys)),
        }
    )


def read_price_envelope(content: bytes) -> dict[str, Any]:
    envelope = read_json_gzip(content)
    if not isinstance(envelope, dict) or envelope.get("schema_version") != 2:
        raise ValueError("Invalid EODHD history price envelope")
    if not isinstance(envelope.get("prices"), list):
        raise ValueError("EODHD history price envelope has no price list")
    return envelope


def write_daily_envelope(
    *,
    price_date: str,
    covered_symbols: list[str],
    prices: list[dict[str, Any]],
    retrieved_at: str,
    source_object_keys: list[str],
) -> bytes:
    daily_prices_object_key(price_date)
    return write_json_gzip(
        {
            "schema_version": 2,
            "price_date": price_date,
            "covered_symbols": sorted(set(covered_symbols)),
            "prices": sorted(
                prices,
                key=lambda row: (str(row["eodhd_symbol_key"]), str(row["date"])),
            ),
            "retrieved_at": retrieved_at,
            "source_object_keys": sorted(set(source_object_keys)),
        }
    )


def read_daily_envelope(content: bytes) -> dict[str, Any]:
    envelope = read_json_gzip(content)
    if not isinstance(envelope, dict) or envelope.get("schema_version") != 2:
        raise ValueError("Invalid EODHD daily price envelope")
    if not isinstance(envelope.get("prices"), list):
        raise ValueError("EODHD daily price envelope has no price list")
    return envelope


def download_eodhd_history_year(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    symbols: list[dict[str, Any]],
    year: str,
    request_delay_seconds: float,
    progress_interval: int,
    log: Callable[..., object],
) -> dict[str, Any]:
    start_date, end_date = eodhd_history_year_window(year)
    object_store.ensure_bucket(EODHD_RAW_BUCKET)
    started_at = time.monotonic()
    catalog: list[dict[str, Any]] = []
    total_price_rows = 0
    reused_symbol_count = 0
    downloaded_request_count = 0

    log(
        "Starting EODHD history year: year=%s symbols=%s from=%s to=%s",
        year,
        len(symbols),
        start_date,
        end_date,
    )
    for symbol_number, symbol in enumerate(symbols, start=1):
        symbol_key = str(symbol["eodhd_symbol_key"])
        object_key = history_symbol_object_key(year, symbol_key)
        prices_by_date: dict[str, dict[str, Any]] = {}
        covered_ranges: list[tuple[str, str]] = []
        source_object_keys: list[str] = []
        if object_store.exists(object_key, bucket=EODHD_RAW_BUCKET):
            envelope = read_price_envelope(
                object_store.read_bytes(object_key, bucket=EODHD_RAW_BUCKET)
            )
            prices_by_date = {str(row["date"]): row for row in envelope["prices"]}
            covered_ranges = _envelope_ranges(envelope)
            source_object_keys = [
                str(key) for key in envelope.get("source_object_keys", [])
            ]
        missing_ranges = _missing_ranges(
            requested=(start_date, end_date),
            covered=covered_ranges,
        )
        if not missing_ranges:
            reused_symbol_count += 1
        retrieved_at = utc_now_iso()
        for missing_start, missing_end in missing_ranges:
            payload = client.prices(
                symbol_key,
                start_date=missing_start,
                end_date=missing_end,
            )
            for row in payload:
                row_date = str(row.get("date", ""))
                if missing_start <= row_date <= missing_end:
                    prices_by_date[row_date] = row
            covered_ranges.append((missing_start, missing_end))
            downloaded_request_count += 1
        merged_ranges = _merge_ranges(covered_ranges)
        object_store.write_bytes(
            object_key,
            write_price_envelope(
                symbol_key=symbol_key,
                covered_ranges=merged_ranges,
                prices=list(prices_by_date.values()),
                retrieved_at=retrieved_at,
                source_object_keys=source_object_keys,
            ),
            bucket=EODHD_RAW_BUCKET,
        )
        catalog.append(
            {
                **symbol,
                "object_key": object_key,
                "row_count": len(prices_by_date),
                "covered_ranges": [
                    {"start": start, "end": end} for start, end in merged_ranges
                ],
                "retrieved_at": retrieved_at,
            }
        )
        total_price_rows += len(prices_by_date)
        if (
            symbol_number == 1
            or symbol_number % progress_interval == 0
            or symbol_number == len(symbols)
        ):
            elapsed = time.monotonic() - started_at
            rate = symbol_number / elapsed if elapsed > 0 else 0
            log(
                "Prepared EODHD price history: year=%s progress=%s/%s "
                "symbol=%s price_rows=%s rate_symbols_per_second=%.2f "
                "elapsed_seconds=%.1f",
                year,
                symbol_number,
                len(symbols),
                symbol_key,
                total_price_rows,
                rate,
                elapsed,
            )
        if request_delay_seconds > 0 and symbol_number < len(symbols):
            time.sleep(request_delay_seconds)

    catalog_key = history_catalog_object_key(year)
    object_store.write_bytes(
        catalog_key,
        write_json_gzip(
            {
                "schema_version": 2,
                "year": year,
                "completed": True,
                "objects": catalog,
            }
        ),
        bucket=EODHD_RAW_BUCKET,
    )
    return {
        "source_snapshot_id": f"history:{year}",
        "year": year,
        "start_date": start_date,
        "end_date": end_date,
        "symbol_count": len(symbols),
        "reused_symbol_count": reused_symbol_count,
        "downloaded_request_count": downloaded_request_count,
        "price_row_count": total_price_rows,
        "catalog_object_key": catalog_key,
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    }


def download_eodhd_daily_date(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    symbols: list[dict[str, Any]],
    price_date: str,
    request_delay_seconds: float,
    progress_interval: int,
    log: Callable[..., object],
) -> dict[str, Any]:
    object_key = daily_prices_object_key(price_date)
    object_store.ensure_bucket(EODHD_RAW_BUCKET)
    prices_by_symbol: dict[str, dict[str, Any]] = {}
    covered_symbols: set[str] = set()
    source_object_keys: list[str] = []
    if object_store.exists(object_key, bucket=EODHD_RAW_BUCKET):
        envelope = read_daily_envelope(
            object_store.read_bytes(object_key, bucket=EODHD_RAW_BUCKET)
        )
        if envelope.get("price_date") != price_date:
            raise ValueError(f"EODHD daily object date mismatch: {object_key}")
        covered_symbols.update(str(value) for value in envelope["covered_symbols"])
        prices_by_symbol.update(
            {str(row["eodhd_symbol_key"]): row for row in envelope["prices"]}
        )
        source_object_keys.extend(
            str(key) for key in envelope.get("source_object_keys", [])
        )
    symbols_by_exchange: dict[str, list[str]] = {}
    for symbol in symbols:
        symbol_key = str(symbol["eodhd_symbol_key"])
        symbols_by_exchange.setdefault(str(symbol["exchange_code"]), []).append(
            symbol_key
        )
    missing_by_exchange = {
        exchange_code: [
            symbol_key
            for symbol_key in exchange_symbols
            if symbol_key not in covered_symbols
        ]
        for exchange_code, exchange_symbols in symbols_by_exchange.items()
    }
    missing_by_exchange = {
        exchange_code: symbol_keys
        for exchange_code, symbol_keys in missing_by_exchange.items()
        if symbol_keys
    }
    requests: list[tuple[str, list[str], list[str] | None]] = []
    for exchange_code, missing_symbols in sorted(missing_by_exchange.items()):
        if len(missing_symbols) == len(symbols_by_exchange[exchange_code]):
            requests.append((exchange_code, missing_symbols, None))
            continue
        for offset in range(0, len(missing_symbols), EODHD_DAILY_SYMBOL_CHUNK_SIZE):
            chunk = missing_symbols[offset : offset + EODHD_DAILY_SYMBOL_CHUNK_SIZE]
            requests.append((exchange_code, chunk, chunk))

    downloaded_request_count = 0
    retrieved_at = utc_now_iso()
    expected_symbols = {str(symbol["eodhd_symbol_key"]) for symbol in symbols}
    for request_number, (exchange_code, covered_chunk, requested_chunk) in enumerate(
        requests,
        start=1,
    ):
        payload = client.bulk_prices(
            exchange_code,
            price_date=price_date,
            symbol_keys=requested_chunk,
        )
        for row in payload:
            symbol_key = _bulk_price_symbol_key(row, exchange_code=exchange_code)
            if symbol_key not in expected_symbols:
                continue
            prices_by_symbol[symbol_key] = {
                **row,
                "eodhd_symbol_key": symbol_key,
                "date": str(row.get("date") or price_date),
            }
        covered_symbols.update(covered_chunk)
        downloaded_request_count += 1
        object_store.write_bytes(
            object_key,
            write_daily_envelope(
                price_date=price_date,
                covered_symbols=list(covered_symbols),
                prices=list(prices_by_symbol.values()),
                retrieved_at=retrieved_at,
                source_object_keys=source_object_keys,
            ),
            bucket=EODHD_RAW_BUCKET,
        )
        if (
            request_number == 1
            or request_number % progress_interval == 0
            or request_number == len(requests)
        ):
            log(
                "Downloaded EODHD daily prices: date=%s progress=%s/%s "
                "covered_symbols=%s rows=%s",
                price_date,
                request_number,
                len(requests),
                len(covered_symbols),
                len(prices_by_symbol),
            )
        if request_delay_seconds > 0 and request_number < len(requests):
            time.sleep(request_delay_seconds)
    if not requests:
        object_store.write_bytes(
            object_key,
            write_daily_envelope(
                price_date=price_date,
                covered_symbols=list(covered_symbols),
                prices=list(prices_by_symbol.values()),
                retrieved_at=retrieved_at,
                source_object_keys=source_object_keys,
            ),
            bucket=EODHD_RAW_BUCKET,
        )
    return {
        "source_snapshot_id": f"daily:{price_date}",
        "price_date": price_date,
        "symbol_count": len(symbols),
        "reused_symbol_count": len(symbols)
        - sum(len(values) for values in missing_by_exchange.values()),
        "downloaded_request_count": downloaded_request_count,
        "price_row_count": len(prices_by_symbol),
        "object_key": object_key,
    }


def materialize_eodhd_history_year(
    *,
    database_path: Path,
    object_store: ObjectStoreResource,
    symbols: list[dict[str, Any]],
    year: str,
    progress_interval: int,
    log: Callable[..., object],
) -> dict[str, Any]:
    eodhd_history_year_window(year)
    rows: list[dict[str, Any]] = []
    for symbol_number, symbol in enumerate(symbols, start=1):
        object_key = history_symbol_object_key(year, str(symbol["eodhd_symbol_key"]))
        if not object_store.exists(object_key, bucket=EODHD_RAW_BUCKET):
            raise ValueError(f"Missing EODHD history object: {object_key}")
        envelope = read_price_envelope(
            object_store.read_bytes(object_key, bucket=EODHD_RAW_BUCKET)
        )
        rows.extend(
            price_rows_from_payload(
                envelope["prices"],
                symbol=symbol,
                source_run_id=f"history:{year}",
                source_object_key=object_key,
                retrieved_at=str(envelope["retrieved_at"]),
            )
        )
        if symbol_number == 1 or symbol_number % progress_interval == 0:
            log(
                "Parsed EODHD history objects: year=%s progress=%s/%s rows=%s",
                year,
                symbol_number,
                len(symbols),
                len(rows),
            )
    date_bounds = _replace_price_duckdb(database_path=database_path, rows=rows)
    return {
        "source_snapshot_id": f"history:{year}",
        "year": year,
        "duckdb_path": str(database_path),
        "duckdb_schema": EODHD_DUCKDB_SCHEMA,
        "duckdb_table": tables.EODHD_EOD_PRICES_TABLE,
        "row_count": len(rows),
        "symbol_count": len(symbols),
        "min_price_date": str(date_bounds[0]) if date_bounds[0] is not None else None,
        "max_price_date": str(date_bounds[1]) if date_bounds[1] is not None else None,
    }


def materialize_eodhd_daily_date(
    *,
    database_path: Path,
    object_store: ObjectStoreResource,
    symbols: list[dict[str, Any]],
    price_date: str,
    log: Callable[..., object],
) -> dict[str, Any]:
    object_key = daily_prices_object_key(price_date)
    if not object_store.exists(object_key, bucket=EODHD_RAW_BUCKET):
        raise ValueError(f"Missing EODHD daily object: {object_key}")
    envelope = read_daily_envelope(
        object_store.read_bytes(object_key, bucket=EODHD_RAW_BUCKET)
    )
    symbols_by_key = {str(row["eodhd_symbol_key"]): row for row in symbols}
    rows: list[dict[str, Any]] = []
    for raw_row in envelope["prices"]:
        symbol_key = str(raw_row["eodhd_symbol_key"])
        symbol = symbols_by_key.get(symbol_key)
        if symbol is None:
            log("Skipping unknown EODHD daily symbol: symbol=%s", symbol_key)
            continue
        rows.extend(
            price_rows_from_payload(
                [raw_row],
                symbol=symbol,
                source_run_id=f"daily:{price_date}",
                source_object_key=object_key,
                retrieved_at=str(envelope["retrieved_at"]),
            )
        )
    date_bounds = _replace_price_duckdb(database_path=database_path, rows=rows)
    return {
        "source_snapshot_id": f"daily:{price_date}",
        "price_date": price_date,
        "duckdb_path": str(database_path),
        "duckdb_schema": EODHD_DUCKDB_SCHEMA,
        "duckdb_table": tables.EODHD_EOD_PRICES_TABLE,
        "row_count": len(rows),
        "min_price_date": str(date_bounds[0]) if date_bounds[0] is not None else None,
        "max_price_date": str(date_bounds[1]) if date_bounds[1] is not None else None,
    }


def _replace_price_duckdb(
    *,
    database_path: Path,
    rows: list[dict[str, Any]],
) -> tuple[Any, Any]:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = tables.EODHD_EOD_PRICES_TABLE
    columns = tables.EODHD_TABLE_COLUMNS[table_name]
    column_types = tables.EODHD_DUCKDB_COLUMN_TYPES[table_name]
    definitions = ", ".join(f'"{column}" {column_types[column]}' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    qualified_table = f'{EODHD_DUCKDB_SCHEMA}."{table_name}"'
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {EODHD_DUCKDB_SCHEMA}")
        connection.execute(f"create or replace table {qualified_table} ({definitions})")
        if rows:
            connection.executemany(
                f"insert into {qualified_table} values ({placeholders})",
                [tuple(row[column] for column in columns) for row in rows],
            )
        return connection.execute(
            f"select min(price_date), max(price_date) from {qualified_table}"
        ).fetchone()


def _envelope_ranges(envelope: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        (str(value["start"]), str(value["end"]))
        for value in envelope.get("covered_ranges", [])
    ]


def _merge_ranges(values: list[tuple[str, str]]) -> list[tuple[str, str]]:
    if not values:
        return []
    merged: list[tuple[date, date]] = []
    for start_text, end_text in sorted(values):
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
        if not merged or start > merged[-1][1] + timedelta(days=1):
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return [(start.isoformat(), end.isoformat()) for start, end in merged]


def _missing_ranges(
    *,
    requested: tuple[str, str],
    covered: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    requested_start = date.fromisoformat(requested[0])
    requested_end = date.fromisoformat(requested[1])
    cursor = requested_start
    missing: list[tuple[str, str]] = []
    for start_text, end_text in _merge_ranges(covered):
        start = max(date.fromisoformat(start_text), requested_start)
        end = min(date.fromisoformat(end_text), requested_end)
        if end < cursor:
            continue
        if start > cursor:
            missing.append(
                (cursor.isoformat(), (start - timedelta(days=1)).isoformat())
            )
        cursor = max(cursor, end + timedelta(days=1))
    if cursor <= requested_end:
        missing.append((cursor.isoformat(), requested_end.isoformat()))
    return missing


def _bulk_price_symbol_key(row: dict[str, Any], *, exchange_code: str) -> str:
    code = str(row.get("code") or row.get("Code") or "").strip().upper()
    if not code:
        raise ValueError("EODHD bulk price row has no symbol code")
    return code if "." in code else f"{code}.{exchange_code.upper()}"


def _reference_table_result(
    context: dg.AssetExecutionContext,
    *,
    table_name: str,
    config: EodhdRawRunConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    configured_run_id = config.raw_run_id
    if configured_run_id is None and object_store.exists(
        reference_snapshot_object_key(context.run_id),
        bucket=EODHD_RAW_BUCKET,
    ):
        configured_run_id = context.run_id
    raw_run_id = resolve_upstream_materialization_run_id(
        instance=context.instance,
        configured_run_id=configured_run_id,
        upstream_asset_key=dg.AssetKey("eodhd_reference_raw_objects"),
        partition_key=None,
    )
    context.log.info(
        "Using completed EODHD raw materialization: table=%s source_run_id=%s",
        table_name,
        raw_run_id,
    )
    metadata = materialize_eodhd_reference_table(
        table_name=table_name,
        database_path=eodhd_duckdb_path(table_name),
        object_store=object_store,
        raw_run_id=raw_run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


def resolve_upstream_materialization_run_id(
    *,
    instance: dg.DagsterInstance,
    configured_run_id: str | None,
    upstream_asset_key: dg.AssetKey,
    partition_key: str | None,
) -> str:
    if configured_run_id is not None:
        return configured_run_id

    if partition_key is None:
        event = instance.get_latest_materialization_event(upstream_asset_key)
        run_id = event.run_id if event is not None else None
    else:
        records = instance.fetch_materializations(
            dg.AssetRecordsFilter(
                asset_key=upstream_asset_key,
                asset_partitions=[partition_key],
            ),
            limit=1,
        ).records
        run_id = records[0].run_id if records else None

    if run_id is None:
        partition_description = (
            f" partition={partition_key}" if partition_key is not None else ""
        )
        raise ValueError(
            f"Upstream asset {upstream_asset_key.to_user_string()}"
            f"{partition_description} has no successful materialization; "
            "materialize it first or provide raw_run_id"
        )
    return run_id


def _duckdb_source_run_ids(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            f'select distinct source_run_id from {EODHD_DUCKDB_SCHEMA}."{table_name}"'
        ).fetchall()
    }


eodhd_reference_selection = dg.AssetSelection.assets(
    "eodhd_reference_complete"
).upstream()

eodhd_reference_weekly_job = dg.define_asset_job(
    "eodhd_reference_weekly_job",
    selection=eodhd_reference_selection,
)

eodhd_price_history_backfill_job = dg.define_asset_job(
    "eodhd_price_history_backfill_job",
    selection=dg.AssetSelection.assets(
        "eodhd_eod_price_history_raw_objects",
        "eodhd_eod_price_history_duckdb",
        "eodhd_eod_price_history_clickhouse",
    ),
)

eodhd_price_daily_job = dg.define_asset_job(
    "eodhd_price_daily_job",
    selection=dg.AssetSelection.assets(
        "eodhd_eod_price_daily_raw_objects",
        "eodhd_eod_price_daily_duckdb",
        "eodhd_eod_price_daily_clickhouse",
    ),
)

eodhd_price_daily_schedule = dg.build_schedule_from_partitioned_job(
    eodhd_price_daily_job,
    name="eodhd_price_daily_schedule",
    hour_of_day=6,
    minute_of_hour=15,
)


@dg.schedule(
    name="eodhd_reference_weekly_schedule",
    cron_schedule="0 5 * * 0",
    execution_timezone="Europe/Belgrade",
    job=eodhd_reference_weekly_job,
)
def eodhd_reference_weekly_schedule() -> dg.RunRequest:
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[
        eodhd_reference_raw_objects,
        eodhd_exchanges_duckdb,
        eodhd_exchange_mics_duckdb,
        eodhd_symbols_duckdb,
        eodhd_symbol_mics_duckdb,
        eodhd_exchanges_clickhouse,
        eodhd_exchange_mics_clickhouse,
        eodhd_symbols_clickhouse,
        eodhd_symbol_mics_clickhouse,
        eodhd_reference_complete,
        eodhd_eod_price_history_raw_objects,
        eodhd_eod_price_history_duckdb,
        eodhd_eod_price_history_clickhouse,
        eodhd_eod_price_daily_raw_objects,
        eodhd_eod_price_daily_duckdb,
        eodhd_eod_price_daily_clickhouse,
    ],
    jobs=[
        eodhd_reference_weekly_job,
        eodhd_price_history_backfill_job,
        eodhd_price_daily_job,
    ],
    schedules=[eodhd_reference_weekly_schedule, eodhd_price_daily_schedule],
    resources={
        "eodhd": EodhdResource(),
        "eodhd_object_store": ObjectStoreResource(bucket=EODHD_RAW_BUCKET),
    },
)
