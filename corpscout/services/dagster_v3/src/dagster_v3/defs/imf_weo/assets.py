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
from dagster_v3.defs.imf_weo import source, tables, transform

GROUP_NAME = "imf_weo"
IMF_WEO_DUCKDB_POOL = "imf_weo_duckdb"
IMF_WEO_TIMEZONE = "Europe/Belgrade"
MINIMUM_IMF_WEO_COUNTRIES = 190
MINIMUM_IMF_WEO_INDICATORS = 40

IMF_WEO_DUCKDB_PATH = Path(
    os.environ.get("IMF_WEO_DUCKDB_PATH", "data/imf_weo_source.duckdb")
).expanduser()
if not IMF_WEO_DUCKDB_PATH.is_absolute():
    IMF_WEO_DUCKDB_PATH = IMF_WEO_DUCKDB_PATH.resolve()


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "xlsx", "imf"},
    description=(
        "Downloads the current all-country IMF World Economic Outlook workbook "
        "to content-addressed object storage."
    ),
)
def imf_weo_snapshot_s3(
    context: AssetExecutionContext,
    imf_weo_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return source.sync_imf_weo_snapshot(
        object_store=imf_weo_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        session=None,
        timeout_seconds=source.DEFAULT_TIMEOUT_SECONDS,
    )


@dg.asset(
    deps=["imf_weo_snapshot_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "xlsx", "duckdb", "imf"},
    pool=IMF_WEO_DUCKDB_POOL,
    description=(
        "Downloads the completed WEO workbook from object storage and normalizes "
        "country series, observations, and release vintages in DuckDB."
    ),
)
def imf_weo_observations_duckdb(
    context: AssetExecutionContext,
    imf_weo_object_store: ObjectStoreResource,
    imf_weo_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = source.read_snapshot_manifest(
        object_store=imf_weo_object_store,
        run_id=context.run_id,
    )
    with transform.local_snapshot_file(
        object_store=imf_weo_object_store,
        manifest=manifest,
    ) as local_snapshot:
        # The S3 file and DuckDB Excel extension are local before the persistent
        # single-writer database is opened.
        transform.ensure_excel_extension_installed()
        IMF_WEO_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with imf_weo_duckdb.get_connection() as connection:
            counts = transform.replace_imf_weo_vintage(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=MINIMUM_IMF_WEO_COUNTRIES,
                minimum_indicator_count=MINIMUM_IMF_WEO_INDICATORS,
            )

    context.log.info("Normalized IMF WEO vintage in DuckDB", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=["imf_weo_observations_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "imf"},
    pool=IMF_WEO_DUCKDB_POOL,
    description=(
        "Atomically replaces the migrated ClickHouse IMF WEO vintage, series, "
        "and observation tables from DuckDB."
    ),
)
def imf_weo_observations_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    imf_weo_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(imf_weo_duckdb) as connection:
        counts = export_imf_weo_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )

    context.log.info("Published IMF WEO tables to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_imf_weo_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
) -> dict[str, int]:
    table_contracts = (
        (
            tables.IMF_WEO_VINTAGES_TABLE,
            tables.IMF_WEO_VINTAGES_COLUMNS,
            tables.IMF_WEO_VINTAGES_CONTRACT,
        ),
        (
            tables.IMF_WEO_SERIES_TABLE,
            tables.IMF_WEO_SERIES_COLUMNS,
            tables.IMF_WEO_SERIES_CONTRACT,
        ),
        (
            tables.IMF_WEO_OBSERVATIONS_TABLE,
            tables.IMF_WEO_OBSERVATIONS_COLUMNS,
            tables.IMF_WEO_OBSERVATIONS_CONTRACT,
        ),
    )
    for table_name, _, contract in table_contracts:
        validate_duckdb_table_contract(
            duckdb_connection,
            schema=tables.IMF_WEO_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.IMF_WEO_DUCKDB_SCHEMA,
            clickhouse_database=tables.IMF_WEO_DATABASE,
            tables=tuple(
                (table_name, columns) for table_name, columns, _ in table_contracts
            ),
        )
    return {
        f"{table_name}_rows": row_counts[table_name]
        for table_name, _, _ in table_contracts
    }


imf_weo_refresh_job = dg.define_asset_job(
    "imf_weo_refresh_job",
    selection=dg.AssetSelection.assets("imf_weo_observations_clickhouse").upstream(),
)

imf_weo_weekly_schedule = dg.ScheduleDefinition(
    name="imf_weo_weekly_schedule",
    job=imf_weo_refresh_job,
    cron_schedule="0 5 * * 0",
    execution_timezone=IMF_WEO_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            imf_weo_snapshot_s3,
            imf_weo_observations_duckdb,
            imf_weo_observations_clickhouse,
        ],
        jobs=[imf_weo_refresh_job],
        schedules=[imf_weo_weekly_schedule],
        resources={
            "imf_weo_object_store": ObjectStoreResource(
                bucket=source.IMF_WEO_RAW_BUCKET
            ),
            "imf_weo_duckdb": duckdb_resource(IMF_WEO_DUCKDB_PATH),
        },
    )
