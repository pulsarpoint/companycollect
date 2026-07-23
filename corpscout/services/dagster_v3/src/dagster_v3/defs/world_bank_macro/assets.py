from datetime import UTC, date, datetime
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
from dagster_v3.defs.world_bank_macro import source, tables, transform

GROUP_NAME = "world_bank_macro"
WORLD_BANK_DUCKDB_POOL = "world_bank_macro_duckdb"
WORLD_BANK_TIMEZONE = "Europe/Belgrade"
MINIMUM_WORLD_BANK_COUNTRIES = 200

WORLD_BANK_DUCKDB_PATH = Path(
    os.environ.get(
        "WORLD_BANK_DUCKDB_PATH",
        "data/world_bank_macro_source.duckdb",
    )
).expanduser()
if not WORLD_BANK_DUCKDB_PATH.is_absolute():
    WORLD_BANK_DUCKDB_PATH = WORLD_BANK_DUCKDB_PATH.resolve()


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "json", "world-bank"},
    description=(
        "Downloads all-country World Development Indicators archives and the "
        "World Bank country catalog to content-addressed object storage."
    ),
)
def world_bank_snapshot_s3(
    context: AssetExecutionContext,
    world_bank_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return source.sync_world_bank_snapshot(
        object_store=world_bank_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        end_year=date.today().year,
        session=None,
        timeout_seconds=source.DEFAULT_TIMEOUT_SECONDS,
    )


@dg.asset(
    deps=["world_bank_snapshot_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "world-bank"},
    pool=WORLD_BANK_DUCKDB_POOL,
    description=(
        "Downloads the completed World Bank snapshot from object storage, then "
        "normalizes country macro observations in DuckDB."
    ),
)
def world_bank_macro_observations_duckdb(
    context: AssetExecutionContext,
    world_bank_object_store: ObjectStoreResource,
    world_bank_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = source.read_snapshot_manifest(
        object_store=world_bank_object_store,
        run_id=context.run_id,
    )

    # This context downloads and verifies every input before get_connection()
    # opens the single-writer DuckDB file.
    with transform.local_snapshot_files(
        object_store=world_bank_object_store,
        manifest=manifest,
    ) as local_snapshot:
        WORLD_BANK_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with world_bank_duckdb.get_connection() as connection:
            counts = transform.replace_world_bank_macro_observations(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=MINIMUM_WORLD_BANK_COUNTRIES,
            )

    context.log.info("Normalized World Bank observations in DuckDB", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=["world_bank_macro_observations_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "world-bank"},
    pool=WORLD_BANK_DUCKDB_POOL,
    description=(
        "Atomically replaces the migrated ClickHouse World Bank macro table "
        "with the normalized DuckDB snapshot."
    ),
)
def world_bank_macro_observations_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    world_bank_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(world_bank_duckdb) as connection:
        counts = export_world_bank_macro_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )

    context.log.info("Published World Bank observations to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_world_bank_macro_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
) -> dict[str, int | str]:
    validate_duckdb_table_contract(
        duckdb_connection,
        schema=tables.WORLD_BANK_DUCKDB_SCHEMA,
        table=tables.WORLD_BANK_MACRO_TABLE,
        contract=tables.WORLD_BANK_DUCKDB_CONTRACT,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.WORLD_BANK_DUCKDB_SCHEMA,
            clickhouse_database=tables.WORLD_BANK_DATABASE,
            tables=((tables.WORLD_BANK_MACRO_TABLE, tables.WORLD_BANK_MACRO_COLUMNS),),
        )
    return {
        "rows": row_counts[tables.WORLD_BANK_MACRO_TABLE],
        "table": tables.QUALIFIED_WORLD_BANK_MACRO_TABLE,
    }


world_bank_macro_refresh_job = dg.define_asset_job(
    "world_bank_macro_refresh_job",
    selection=dg.AssetSelection.assets(
        "world_bank_macro_observations_clickhouse"
    ).upstream(),
)

world_bank_macro_weekly_schedule = dg.ScheduleDefinition(
    name="world_bank_macro_weekly_schedule",
    job=world_bank_macro_refresh_job,
    cron_schedule="20 4 * * 0",
    execution_timezone=WORLD_BANK_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            world_bank_snapshot_s3,
            world_bank_macro_observations_duckdb,
            world_bank_macro_observations_clickhouse,
        ],
        jobs=[world_bank_macro_refresh_job],
        schedules=[world_bank_macro_weekly_schedule],
        resources={
            "world_bank_object_store": ObjectStoreResource(
                bucket=source.WORLD_BANK_RAW_BUCKET
            ),
            "world_bank_duckdb": duckdb_resource(WORLD_BANK_DUCKDB_PATH),
        },
    )
