from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import dagster as dg
import duckdb
import polars as pl
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import GROUP_NAME
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.assets.entity_updates import (
    NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
from dagster_v3.defs.norway_resolved import tables as no_tables

ENTITY_CLICKHOUSE_TABLES = (
    no_tables.NO_COMPANIES_TABLE,
    no_tables.NO_WEBSITES_TABLE,
    no_tables.NO_INDUSTRIES_TABLE,
)
ENTITY_CLICKHOUSE_DUCKDB_SCHEMA = "norway_brreg_entity_publish"


@dg.asset(
    name="norway_brreg_entities_snapshot_clickhouse",
    deps=[
        dg.AssetKey("norway_brreg_entities_snapshot_no_companies_parquet"),
        dg.AssetKey("norway_brreg_entities_snapshot_no_websites_parquet"),
        dg.AssetKey("norway_brreg_entities_snapshot_no_industries_parquet"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb", "clickhouse", "brreg"},
    description="Replaces Norway entity ClickHouse tables from full snapshot parquet outputs.",
)
def norway_brreg_entities_snapshot_clickhouse(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
) -> dg.MaterializeResult:
    run_id = context.run_id
    context.log.info(
        "Publishing Norway Brreg entity snapshot to ClickHouse: run_id=%s tables=%s",
        run_id,
        ENTITY_CLICKHOUSE_TABLES,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=ENTITY_CLICKHOUSE_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_entity_snapshot_parquets_in_clickhouse(
            storage=norway_brreg_entity_storage,
            clickhouse_client=client,
            run_id=run_id,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg entity snapshot ClickHouse publish: run_id=%s row_counts=%s",
        run_id,
        row_counts,
    )
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


@dg.asset(
    name="norway_brreg_entity_updates_clickhouse",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[
        dg.AssetKey("norway_brreg_entity_updates_no_companies_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_no_websites_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_no_industries_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_affected_orgs_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_removed_orgs_parquet"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb", "clickhouse", "brreg"},
    description="Applies one Norway Brreg entity update parquet partition to ClickHouse.",
)
def norway_brreg_entity_updates_clickhouse(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    context.log.info(
        "Publishing Norway Brreg entity updates to ClickHouse: partition=%s tables=%s",
        partition_date,
        ENTITY_CLICKHOUSE_TABLES,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=ENTITY_CLICKHOUSE_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = apply_entity_update_parquets_to_clickhouse(
            storage=norway_brreg_entity_storage,
            clickhouse_client=client,
            partition_date=partition_date,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg entity update ClickHouse publish: partition=%s row_counts=%s",
        partition_date,
        row_counts,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            **{f"{table}_row_count": count for table, count in row_counts.items()},
        }
    )


def replace_entity_snapshot_parquets_in_clickhouse(
    *,
    storage: NorwayBrregEntityParquetStorageResource,
    clickhouse_client: Any,
    run_id: str,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    frames = {
        table_name: storage.read_normalized_snapshot_table(table_name)
        for table_name in ENTITY_CLICKHOUSE_TABLES
    }
    _log(log, "Loaded Norway Brreg snapshot parquet frames: run_id=%s rows=%s", run_id, _row_counts(frames))
    with duckdb.connect(":memory:") as connection:
        _load_frames_into_duckdb(connection, frames)
        return replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=ENTITY_CLICKHOUSE_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table_name, no_tables.RESOLVED_EXPORT_COLUMNS[table_name])
                for table_name in ENTITY_CLICKHOUSE_TABLES
            ),
        )


def apply_entity_update_parquets_to_clickhouse(
    *,
    storage: NorwayBrregEntityParquetStorageResource,
    clickhouse_client: Any,
    partition_date: str,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    affected_orgs = storage.read_normalized_update_table(
        partition_date,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    )
    removed_orgs = storage.read_normalized_update_table(
        partition_date,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
    )
    frames = {
        table_name: storage.read_normalized_update_table(partition_date, table_name)
        for table_name in ENTITY_CLICKHOUSE_TABLES
    }
    row_counts = {
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: affected_orgs.height,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: removed_orgs.height,
        **_row_counts(frames),
    }
    if affected_orgs.height < 1:
        _validate_no_replacements_without_affected_orgs(frames)
        _log(
            log,
            "Norway Brreg update partition %s has no affected orgs; skipping ClickHouse changes",
            partition_date,
        )
        return row_counts

    _log(
        log,
        "Applying Norway Brreg update partition %s to ClickHouse: rows=%s",
        partition_date,
        row_counts,
    )
    affected_stage_table = f"_tmp_no_affected_orgs_{uuid.uuid4().hex}"
    affected_stage_qualified = _quote_clickhouse_qualified_table(
        RESOLVED_DATABASE,
        affected_stage_table,
    )
    clickhouse_client.execute(
        f"CREATE TABLE {affected_stage_qualified} "
        f"({_quote_clickhouse_identifier('org_number')} String) ENGINE = Memory"
    )
    primary_error: Exception | None = None
    try:
        affected_org_rows = _affected_org_rows(affected_orgs)
        clickhouse_client.execute(
            f"INSERT INTO {affected_stage_qualified} "
            f"({_quote_clickhouse_identifier('org_number')}) VALUES",
            affected_org_rows,
        )
        for table_name in ENTITY_CLICKHOUSE_TABLES:
            clickhouse_client.execute(
                f"ALTER TABLE {_quote_clickhouse_qualified_table(RESOLVED_DATABASE, table_name)} "
                f"DELETE WHERE {_quote_clickhouse_identifier('org_number')} IN "
                f"(SELECT {_quote_clickhouse_identifier('org_number')} FROM {affected_stage_qualified}) "
                "SETTINGS mutations_sync = 1"
            )

        with duckdb.connect(":memory:") as connection:
            _load_frames_into_duckdb(connection, frames)
            for table_name in ENTITY_CLICKHOUSE_TABLES:
                row_counts[table_name] = export_duckdb_connection_table_to_clickhouse(
                    duckdb_connection=connection,
                    clickhouse_client=clickhouse_client,
                    duckdb_schema=ENTITY_CLICKHOUSE_DUCKDB_SCHEMA,
                    duckdb_table=table_name,
                    clickhouse_database=RESOLVED_DATABASE,
                    clickhouse_table=table_name,
                    columns=no_tables.RESOLVED_EXPORT_COLUMNS[table_name],
                    truncate=False,
                )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        _drop_affected_stage_table(
            clickhouse_client,
            affected_stage_qualified,
            suppress_errors=primary_error is not None,
        )
    return row_counts


def _load_frames_into_duckdb(
    connection: duckdb.DuckDBPyConnection,
    frames: dict[str, pl.DataFrame],
) -> None:
    connection.execute(f"create schema {_quote_duckdb_identifier(ENTITY_CLICKHOUSE_DUCKDB_SCHEMA)}")
    for table_name, frame in frames.items():
        _validate_frame_columns(table_name, frame)
        registered_name = f"_frame_{table_name}"
        connection.register(registered_name, frame.to_arrow())
        try:
            connection.execute(
                f"create table {_quote_duckdb_identifier(ENTITY_CLICKHOUSE_DUCKDB_SCHEMA)}."
                f"{_quote_duckdb_identifier(table_name)} as "
                f"select * from {_quote_duckdb_identifier(registered_name)}"
            )
        finally:
            connection.unregister(registered_name)


def _validate_frame_columns(table_name: str, frame: pl.DataFrame) -> None:
    missing_columns = [
        column
        for column in no_tables.RESOLVED_EXPORT_COLUMNS[table_name]
        if column not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Norway Brreg normalized parquet table {table_name} is missing columns: "
            + ", ".join(missing_columns)
        )


def _affected_org_rows(affected_orgs: pl.DataFrame) -> list[tuple[str]]:
    return [
        (str(org_number),)
        for org_number in affected_orgs.get_column("org_number").drop_nulls().unique(
            maintain_order=True
        )
    ]


def _validate_no_replacements_without_affected_orgs(frames: dict[str, pl.DataFrame]) -> None:
    replacement_counts = _row_counts(frames)
    if any(count > 0 for count in replacement_counts.values()):
        raise ValueError(
            "Norway Brreg update partition has replacement rows but no affected orgs: "
            + str(replacement_counts)
        )


def _drop_affected_stage_table(
    clickhouse_client: Any,
    qualified_table: str,
    *,
    suppress_errors: bool,
) -> None:
    try:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_table}")
    except Exception:
        if not suppress_errors:
            raise


def _row_counts(frames: dict[str, pl.DataFrame]) -> dict[str, int]:
    return {table_name: frame.height for table_name, frame in frames.items()}


def _quote_duckdb_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _quote_clickhouse_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _quote_clickhouse_qualified_table(database: str, table: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(database)}."
        f"{_quote_clickhouse_identifier(table)}"
    )


def _log(log: Callable[..., None] | None, message: str, *args: Any) -> None:
    if log is not None:
        log(message, *args)
