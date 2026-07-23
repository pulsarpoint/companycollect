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
from dagster_v3.defs.un_comtrade import source, tables, transform

GROUP_NAME = "un_comtrade"
UN_COMTRADE_DUCKDB_POOL = "un_comtrade_duckdb"
UN_COMTRADE_TIMEZONE = "Europe/Belgrade"
MINIMUM_HISTORICAL_REPORTERS = 150

UN_COMTRADE_DUCKDB_PATH = Path(
    os.environ.get(
        "UN_COMTRADE_DUCKDB_PATH",
        "data/un_comtrade_source.duckdb",
    )
).expanduser()
if not UN_COMTRADE_DUCKDB_PATH.is_absolute():
    UN_COMTRADE_DUCKDB_PATH = UN_COMTRADE_DUCKDB_PATH.resolve()


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "csv", "un-comtrade"},
    description=(
        "Downloads annual total merchandise imports, exports, and availability "
        "metadata for every UN Comtrade reporter into content-addressed storage."
    ),
)
def un_comtrade_snapshot_s3(
    context: AssetExecutionContext,
    un_comtrade_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return source.sync_un_comtrade_snapshot(
        object_store=un_comtrade_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        start_year=source.UN_COMTRADE_START_YEAR,
        end_year=date.today().year - 1,
        session=None,
        timeout_seconds=source.DEFAULT_TIMEOUT_SECONDS,
        request_interval_seconds=source.DEFAULT_REQUEST_INTERVAL_SECONDS,
    )


@dg.asset(
    deps=["un_comtrade_snapshot_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "csv", "duckdb", "sql", "un-comtrade"},
    pool=UN_COMTRADE_DUCKDB_POOL,
    description=(
        "Downloads and verifies every completed UN Comtrade S3 object before "
        "normalizing annual country trade totals and release metadata in DuckDB."
    ),
)
def un_comtrade_annual_totals_duckdb(
    context: AssetExecutionContext,
    un_comtrade_object_store: ObjectStoreResource,
    un_comtrade_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = source.read_snapshot_manifest(
        object_store=un_comtrade_object_store,
        run_id=context.run_id,
    )
    with transform.local_snapshot_files(
        object_store=un_comtrade_object_store,
        manifest=manifest,
    ) as local_snapshot:
        # No internet request or S3 transfer runs while the persistent
        # single-writer DuckDB file is open.
        UN_COMTRADE_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with un_comtrade_duckdb.get_connection() as connection:
            counts = transform.replace_un_comtrade_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_historical_reporters=MINIMUM_HISTORICAL_REPORTERS,
            )

    context.log.info("Normalized UN Comtrade annual totals", extra=counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=["un_comtrade_annual_totals_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "un-comtrade"},
    pool=UN_COMTRADE_DUCKDB_POOL,
    description=(
        "Atomically replaces the migrated UN Comtrade availability and annual "
        "country trade-total tables in ClickHouse."
    ),
)
def un_comtrade_annual_totals_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    un_comtrade_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(un_comtrade_duckdb) as connection:
        counts = export_un_comtrade_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )

    context.log.info("Published UN Comtrade tables to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_un_comtrade_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
) -> dict[str, int]:
    for table_name, (_, contract) in tables.UN_COMTRADE_TABLE_CONTRACTS.items():
        validate_duckdb_table_contract(
            duckdb_connection,
            schema=tables.UN_COMTRADE_DUCKDB_SCHEMA,
            table=table_name,
            contract=contract,
        )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.UN_COMTRADE_DUCKDB_SCHEMA,
            clickhouse_database=tables.UN_COMTRADE_DATABASE,
            tables=tuple(
                (table_name, columns)
                for table_name, (
                    columns,
                    _contract,
                ) in tables.UN_COMTRADE_TABLE_CONTRACTS.items()
            ),
        )
    return {
        f"{table_name}_rows": row_counts[table_name]
        for table_name in tables.UN_COMTRADE_TABLE_CONTRACTS
    }


un_comtrade_refresh_job = dg.define_asset_job(
    "un_comtrade_refresh_job",
    selection=dg.AssetSelection.assets(
        "un_comtrade_annual_totals_clickhouse"
    ).upstream(),
)

un_comtrade_monthly_schedule = dg.ScheduleDefinition(
    name="un_comtrade_monthly_schedule",
    job=un_comtrade_refresh_job,
    cron_schedule="10 6 10 * *",
    execution_timezone=UN_COMTRADE_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            un_comtrade_snapshot_s3,
            un_comtrade_annual_totals_duckdb,
            un_comtrade_annual_totals_clickhouse,
        ],
        jobs=[un_comtrade_refresh_job],
        schedules=[un_comtrade_monthly_schedule],
        resources={
            "un_comtrade_object_store": ObjectStoreResource(
                bucket=source.UN_COMTRADE_RAW_BUCKET
            ),
            "un_comtrade_duckdb": duckdb_resource(UN_COMTRADE_DUCKDB_PATH),
        },
    )
