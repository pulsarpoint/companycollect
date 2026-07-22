import json
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
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
    price_catalog_object_key,
    price_rows_from_payload,
    price_snapshot_object_key,
    price_symbol_object_key,
    read_json_gzip,
    read_reference_snapshot,
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
EODHD_PRICE_BUCKET_COUNT = 256
EODHD_PRICE_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket_index:03d}" for bucket_index in range(EODHD_PRICE_BUCKET_COUNT)]
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
    partitions_def=EODHD_PRICE_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_PRICE_DOWNLOAD_POOL,
    description=(
        "Downloads one stable symbol bucket of six-year EOD history sequentially "
        "into partition-scoped, compressed S3 objects with a completed catalog."
    ),
)
def eodhd_eod_price_raw_objects(
    context: dg.AssetExecutionContext,
    config: EodhdPriceBackfillConfig,
    eodhd: EodhdResource,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    symbols = select_eodhd_price_symbols(
        config=config,
        partition_key=context.partition_key,
    )
    start_date, end_date = config.resolve_date_window(datetime.now(UTC))
    metadata = download_eodhd_price_snapshot(
        client=eodhd,
        object_store=eodhd_object_store,
        symbols=symbols,
        partition_key=context.partition_key,
        run_id=context.run.run_id,
        start_date=start_date,
        end_date=end_date,
        request_delay_seconds=config.request_delay_seconds,
        progress_interval=config.progress_interval,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    deps=["eodhd_eod_price_raw_objects"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "eodhd"},
    partitions_def=EODHD_PRICE_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    description=(
        "Streams one completed EODHD price partition into its own DuckDB database."
    ),
)
def eodhd_eod_prices_duckdb(
    context: dg.AssetExecutionContext,
    config: EodhdRawRunConfig,
    eodhd_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    raw_run_id = config.raw_run_id or context.run.run_id
    metadata = materialize_eodhd_prices(
        database_path=eodhd_duckdb_path(
            tables.EODHD_EOD_PRICES_TABLE,
            partition_key=context.partition_key,
        ),
        object_store=eodhd_object_store,
        raw_run_id=raw_run_id,
        partition_key=context.partition_key,
        progress_interval=100,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name=tables.EODHD_EOD_PRICES_TABLE,
    deps=["eodhd_eod_prices_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eodhd"},
    partitions_def=EODHD_PRICE_PARTITIONS,
    backfill_policy=EODHD_PRICE_BACKFILL_POLICY,
    pool=EODHD_CLICKHOUSE_POOL,
    description=(
        "Appends EODHD daily OHLCV history into the ReplacingMergeTree price table."
    ),
)
def eodhd_eod_prices_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return export_eodhd_table_to_clickhouse(
        context,
        clickhouse=clickhouse,
        table_name=tables.EODHD_EOD_PRICES_TABLE,
        truncate=False,
        partition_key=context.partition_key,
    )


def eodhd_duckdb_path(
    table_name: str,
    *,
    partition_key: str | None = None,
) -> Path:
    supported_tables = {*tables.EODHD_REFERENCE_TABLES, tables.EODHD_EOD_PRICES_TABLE}
    if table_name not in supported_tables:
        raise ValueError(f"Unsupported EODHD table: {table_name}")
    if table_name == tables.EODHD_EOD_PRICES_TABLE:
        if partition_key is None:
            raise ValueError("EODHD price DuckDB path requires a partition key")
        eodhd_price_bucket_index(partition_key)
        return EODHD_DUCKDB_DIRECTORY / table_name / f"{partition_key}.duckdb"
    if partition_key is not None:
        raise ValueError(f"EODHD reference table {table_name} is not partitioned")
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
    placeholders = ", ".join("?" for _ in columns)
    qualified_table = f'{EODHD_DUCKDB_SCHEMA}."{table_name}"'
    row_count = 0
    batch: list[tuple[Any, ...]] = []
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {EODHD_DUCKDB_SCHEMA}")
        connection.execute(
            f"create or replace table {qualified_table} ({column_definitions})"
        )
        for row in rows:
            batch.append(tuple(row[column] for column in columns))
            if len(batch) >= DUCKDB_INSERT_BATCH_SIZE:
                connection.executemany(
                    f"insert into {qualified_table} values ({placeholders})",
                    batch,
                )
                row_count += len(batch)
                log("Loaded EODHD DuckDB rows: table=%s rows=%s", table_name, row_count)
                batch.clear()
        if batch:
            connection.executemany(
                f"insert into {qualified_table} values ({placeholders})",
                batch,
            )
            row_count += len(batch)
    if row_count == 0:
        raise ValueError(f"EODHD table {table_name} produced zero rows")
    log("Finished EODHD DuckDB table: table=%s rows=%s", table_name, row_count)
    return row_count


def export_eodhd_table_to_clickhouse(
    context: dg.AssetExecutionContext,
    *,
    clickhouse: ClickhouseResource,
    table_name: str,
    truncate: bool,
    partition_key: str | None = None,
) -> dg.MaterializeResult:
    database_path = eodhd_duckdb_path(table_name, partition_key=partition_key)
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
            rows = client.query(
                f"select source_run_id, count() from {RESOLVED_DATABASE}.{table_name} "
                "group by source_run_id order by source_run_id limit 2"
            ).result_rows
            if len(rows) != 1:
                raise ValueError(
                    f"ClickHouse table {table_name} does not contain exactly one source_run_id"
                )
            source_run_ids.add(str(rows[0][0]))
            row_counts[table_name] = int(rows[0][1])
        orphan_symbol_mics = client.query(
            "/* missing_symbol_mics */ "
            f"select mappings.eodhd_symbol_key, mappings.mic "
            f"from {RESOLVED_DATABASE}.{tables.EODHD_SYMBOL_MICS_TABLE} as mappings "
            f"left join {RESOLVED_DATABASE}.{tables.EODHD_SYMBOLS_TABLE} as symbols "
            "on symbols.eodhd_symbol_key = mappings.eodhd_symbol_key "
            "where symbols.eodhd_symbol_key is null limit 10"
        ).result_rows
        invalid_mics = client.query(
            f"select mic from {RESOLVED_DATABASE}.{tables.EODHD_SYMBOL_MICS_TABLE} "
            "where not match(mic, '^[A-Z0-9]{4}$') order by mic limit 10"
        ).result_rows
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
    partition_key: str,
) -> list[dict[str, Any]]:
    bucket_index = eodhd_price_bucket_index(partition_key)
    database_path = eodhd_duckdb_path(tables.EODHD_SYMBOLS_TABLE)
    if not database_path.exists():
        raise ValueError(
            "Missing EODHD symbols DuckDB database; materialize eodhd_symbols_duckdb first"
        )
    with duckdb.connect(str(database_path), read_only=True) as connection:
        placeholders = ", ".join("?" for _ in config.instrument_types())
        where_delisted = "" if config.include_delisted else "and is_delisted = 0"
        query = (
            "select eodhd_symbol_key, exchange_code, ticker, currency "
            "from eodhd.eodhd_symbols "
            f"where instrument_type in ({placeholders}) {where_delisted} "
            "and substr(sha256(eodhd_symbol_key), 1, 2) = ? "
            "order by eodhd_symbol_key"
        )
        parameters = (*config.instrument_types(), f"{bucket_index:02x}")
        rows = connection.execute(query, parameters).fetchall()
    if config.max_symbols is not None:
        rows = rows[: config.max_symbols]
    columns = ("eodhd_symbol_key", "exchange_code", "ticker", "currency")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def eodhd_price_partition_key(symbol_key: str) -> str:
    if not isinstance(symbol_key, str) or not symbol_key:
        raise ValueError("EODHD symbol key must be a non-empty string")
    bucket_index = int(sha256(symbol_key.encode("utf-8")).hexdigest()[:2], 16)
    return f"bucket_{bucket_index:03d}"


def eodhd_price_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid EODHD price partition key: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < EODHD_PRICE_BUCKET_COUNT:
        raise ValueError(f"EODHD price bucket index out of range: {bucket_index}")
    return bucket_index


def download_eodhd_price_snapshot(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    symbols: list[dict[str, Any]],
    partition_key: str,
    run_id: str,
    start_date: str,
    end_date: str,
    request_delay_seconds: float,
    progress_interval: int,
    log: Callable[..., object],
) -> dict[str, Any]:
    eodhd_price_bucket_index(partition_key)
    object_store.ensure_bucket(EODHD_RAW_BUCKET)
    started_at = time.monotonic()
    retrieved_at = utc_now_iso()
    catalog: list[dict[str, Any]] = []
    total_price_rows = 0

    log(
        "Starting EODHD price partition download: partition=%s symbols=%s "
        "from=%s to=%s",
        partition_key,
        len(symbols),
        start_date,
        end_date,
    )
    for symbol_number, symbol in enumerate(symbols, start=1):
        symbol_key = str(symbol["eodhd_symbol_key"])
        symbol_partition_key = eodhd_price_partition_key(symbol_key)
        if symbol_partition_key != partition_key:
            raise ValueError(
                "EODHD symbol assigned to the wrong price partition: "
                f"symbol={symbol_key} expected={symbol_partition_key} "
                f"actual={partition_key}"
            )
        payload = client.prices(
            symbol_key,
            start_date=start_date,
            end_date=end_date,
        )
        object_key = price_symbol_object_key(run_id, partition_key, symbol_key)
        content = write_json_gzip(payload)
        object_store.write_bytes(object_key, content, bucket=EODHD_RAW_BUCKET)
        catalog.append(
            {
                **symbol,
                "object_key": object_key,
                "row_count": len(payload),
                "content_length_bytes": len(content),
                "retrieved_at": retrieved_at,
            }
        )
        total_price_rows += len(payload)
        if (
            symbol_number == 1
            or symbol_number % progress_interval == 0
            or symbol_number == len(symbols)
        ):
            elapsed = time.monotonic() - started_at
            rate = symbol_number / elapsed if elapsed > 0 else 0
            log(
                "Downloaded EODHD price history: partition=%s progress=%s/%s "
                "symbol=%s price_rows=%s rate_symbols_per_second=%.2f "
                "elapsed_seconds=%.1f",
                partition_key,
                symbol_number,
                len(symbols),
                symbol_key,
                total_price_rows,
                rate,
                elapsed,
            )
        if request_delay_seconds > 0 and symbol_number < len(symbols):
            time.sleep(request_delay_seconds)

    catalog_key = price_catalog_object_key(run_id, partition_key)
    object_store.write_bytes(
        catalog_key,
        write_json_gzip(catalog),
        bucket=EODHD_RAW_BUCKET,
    )
    snapshot = {
        "schema_version": 1,
        "source_system": "eodhd",
        "source_run_id": run_id,
        "partition_key": partition_key,
        "retrieved_at": retrieved_at,
        "completed": True,
        "start_date": start_date,
        "end_date": end_date,
        "symbol_count": len(symbols),
        "price_row_count": total_price_rows,
        "catalog_object_key": catalog_key,
    }
    object_store.write_json(
        price_snapshot_object_key(run_id, partition_key),
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")),
        bucket=EODHD_RAW_BUCKET,
    )
    return {
        "source_run_id": run_id,
        "partition_key": partition_key,
        "start_date": start_date,
        "end_date": end_date,
        "symbol_count": len(symbols),
        "price_row_count": total_price_rows,
        "raw_object_count": len(symbols) + 2,
        "catalog_object_key": catalog_key,
        "snapshot_object_key": price_snapshot_object_key(run_id, partition_key),
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    }


def materialize_eodhd_prices(
    *,
    database_path: Path,
    object_store: ObjectStoreResource,
    raw_run_id: str,
    partition_key: str,
    progress_interval: int,
    log: Callable[..., object],
) -> dict[str, Any]:
    eodhd_price_bucket_index(partition_key)
    snapshot_key = price_snapshot_object_key(raw_run_id, partition_key)
    if not object_store.exists(snapshot_key, bucket=EODHD_RAW_BUCKET):
        raise ValueError(
            f"No completed EODHD price snapshot found for run_id={raw_run_id}; "
            "materialize eodhd_eod_price_raw_objects first"
        )
    snapshot = json.loads(
        object_store.read_bytes(snapshot_key, bucket=EODHD_RAW_BUCKET)
    )
    if snapshot.get("completed") is not True:
        raise ValueError(f"EODHD price snapshot is incomplete for run_id={raw_run_id}")
    if snapshot.get("partition_key") != partition_key:
        raise ValueError(
            "EODHD price snapshot partition mismatch: "
            f"expected={partition_key} actual={snapshot.get('partition_key')}"
        )
    catalog = read_json_gzip(
        object_store.read_bytes(
            str(snapshot["catalog_object_key"]),
            bucket=EODHD_RAW_BUCKET,
        )
    )
    if not isinstance(catalog, list):
        raise ValueError(f"EODHD price snapshot catalog is not a list: {raw_run_id}")

    database_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = tables.EODHD_EOD_PRICES_TABLE
    columns = tables.EODHD_TABLE_COLUMNS[table_name]
    column_types = tables.EODHD_DUCKDB_COLUMN_TYPES[table_name]
    definitions = ", ".join(f'"{column}" {column_types[column]}' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    qualified_table = f'{EODHD_DUCKDB_SCHEMA}."{table_name}"'
    row_count = 0
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {EODHD_DUCKDB_SCHEMA}")
        connection.execute(f"create or replace table {qualified_table} ({definitions})")
        for object_number, catalog_row in enumerate(catalog, start=1):
            payload = read_json_gzip(
                object_store.read_bytes(
                    str(catalog_row["object_key"]),
                    bucket=EODHD_RAW_BUCKET,
                )
            )
            rows = price_rows_from_payload(
                payload,
                symbol=catalog_row,
                source_run_id=raw_run_id,
                source_object_key=str(catalog_row["object_key"]),
                retrieved_at=str(catalog_row["retrieved_at"]),
            )
            if rows:
                connection.executemany(
                    f"insert into {qualified_table} values ({placeholders})",
                    [tuple(row[column] for column in columns) for row in rows],
                )
                row_count += len(rows)
            if (
                object_number == 1
                or object_number % progress_interval == 0
                or object_number == len(catalog)
            ):
                log(
                    "Parsed EODHD price objects: progress=%s/%s symbol=%s rows=%s",
                    object_number,
                    len(catalog),
                    catalog_row["eodhd_symbol_key"],
                    row_count,
                )
        date_bounds = connection.execute(
            f"select min(price_date), max(price_date) from {qualified_table}"
        ).fetchone()
    return {
        "source_run_id": raw_run_id,
        "partition_key": partition_key,
        "duckdb_path": str(database_path),
        "duckdb_schema": EODHD_DUCKDB_SCHEMA,
        "duckdb_table": table_name,
        "row_count": row_count,
        "symbol_count": len(catalog),
        "min_price_date": str(date_bounds[0]) if date_bounds[0] is not None else None,
        "max_price_date": str(date_bounds[1]) if date_bounds[1] is not None else None,
    }


def _reference_table_result(
    context: dg.AssetExecutionContext,
    *,
    table_name: str,
    config: EodhdRawRunConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    raw_run_id = config.raw_run_id or context.run.run_id
    metadata = materialize_eodhd_reference_table(
        table_name=table_name,
        database_path=eodhd_duckdb_path(table_name),
        object_store=object_store,
        raw_run_id=raw_run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


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

eodhd_price_backfill_job = dg.define_asset_job(
    "eodhd_price_backfill_job",
    selection=dg.AssetSelection.assets(
        "eodhd_eod_price_raw_objects",
        "eodhd_eod_prices_duckdb",
        tables.EODHD_EOD_PRICES_TABLE,
    ),
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
        eodhd_eod_price_raw_objects,
        eodhd_eod_prices_duckdb,
        eodhd_eod_prices_clickhouse,
    ],
    jobs=[eodhd_reference_weekly_job, eodhd_price_backfill_job],
    schedules=[eodhd_reference_weekly_schedule],
    resources={
        "eodhd": EodhdResource(),
        "eodhd_object_store": ObjectStoreResource(bucket=EODHD_RAW_BUCKET),
    },
)
