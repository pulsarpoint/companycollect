from datetime import UTC, datetime
import os
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
import duckdb

from dagster_v3.defs.clickhouse.resolved import (
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.duckdb.schema_contract import validate_duckdb_table_contract
from dagster_v3.defs.eurostat import source, tables, transform

GROUP_NAME = "eurostat"
EUROSTAT_DUCKDB_POOL = "eurostat_duckdb"
EUROSTAT_TIMEZONE = "Europe/Belgrade"

EUROSTAT_DUCKDB_PATH = Path(
    os.environ.get("EUROSTAT_DUCKDB_PATH", "data/eurostat_source.duckdb")
).expanduser()
if not EUROSTAT_DUCKDB_PATH.is_absolute():
    EUROSTAT_DUCKDB_PATH = EUROSTAT_DUCKDB_PATH.resolve()


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "tsv", "xml", "eurostat"},
    description=(
        "Downloads the selected all-geography Eurostat datasets and their SDMX "
        "structures to content-addressed object storage."
    ),
)
def eurostat_snapshot_s3(
    context: AssetExecutionContext,
    eurostat_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return source.sync_eurostat_snapshot(
        object_store=eurostat_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        datasets=source.EUROSTAT_DATASETS,
        session=None,
        timeout_seconds=source.DEFAULT_TIMEOUT_SECONDS,
    )


@dg.asset(
    deps=["eurostat_snapshot_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "tsv", "xml", "duckdb", "sql", "eurostat"},
    pool=EUROSTAT_DUCKDB_POOL,
    description=(
        "Downloads the completed Eurostat snapshot from object storage, then "
        "normalizes SDMX datasets, dimensions, series, and observations in DuckDB."
    ),
)
def eurostat_observations_duckdb(
    context: AssetExecutionContext,
    eurostat_object_store: ObjectStoreResource,
    eurostat_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = source.read_snapshot_manifest(
        object_store=eurostat_object_store,
        run_id=context.run_id,
    )
    with transform.local_snapshot_files(
        object_store=eurostat_object_store,
        manifest=manifest,
        datasets=source.EUROSTAT_DATASETS,
    ) as local_snapshot:
        # All source files and SDMX metadata are local and hash-verified before
        # opening the persistent single-writer DuckDB database.
        EUROSTAT_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with eurostat_duckdb.get_connection() as connection:
            counts = transform.replace_eurostat_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
            )

    context.log.info("Normalized Eurostat snapshot in DuckDB", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=["eurostat_observations_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "eurostat"},
    pool=EUROSTAT_DUCKDB_POOL,
    description=(
        "Atomically replaces the migrated ClickHouse Eurostat dataset, dimension, "
        "series, and observation tables from DuckDB."
    ),
)
def eurostat_observations_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    eurostat_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(eurostat_duckdb) as connection:
        counts = export_eurostat_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )

    context.log.info("Published Eurostat tables to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_eurostat_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
) -> dict[str, int]:
    for table_name, (_, contract) in tables.EUROSTAT_TABLE_CONTRACTS.items():
        validate_duckdb_table_contract(
            duckdb_connection,
            schema=tables.EUROSTAT_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )
    with clickhouse.get_connection() as client:
        clickhouse_tables = tuple(
            (table_name, columns)
            for table_name, (
                columns,
                _contract,
            ) in tables.EUROSTAT_TABLE_CONTRACTS.items()
        )
        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.EUROSTAT_DUCKDB_SCHEMA,
            clickhouse_database=tables.EUROSTAT_DATABASE,
            tables=clickhouse_tables,
        )
    return {
        f"{table_name}_rows": row_counts[table_name]
        for table_name in tables.EUROSTAT_TABLE_CONTRACTS
    }


eurostat_refresh_job = dg.define_asset_job(
    "eurostat_refresh_job",
    selection=dg.AssetSelection.assets("eurostat_observations_clickhouse").upstream(),
)

eurostat_weekly_schedule = dg.ScheduleDefinition(
    name="eurostat_weekly_schedule",
    job=eurostat_refresh_job,
    cron_schedule="55 5 * * 0",
    execution_timezone=EUROSTAT_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            eurostat_snapshot_s3,
            eurostat_observations_duckdb,
            eurostat_observations_clickhouse,
        ],
        jobs=[eurostat_refresh_job],
        schedules=[eurostat_weekly_schedule],
        resources={
            "eurostat_object_store": ObjectStoreResource(
                bucket=source.EUROSTAT_RAW_BUCKET
            ),
            "eurostat_duckdb": duckdb_resource(EUROSTAT_DUCKDB_PATH),
        },
    )
