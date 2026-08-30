from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.ats_clickhouse import (
    AtsClickhouseTables,
    publish_ats_snapshot,
)
from dagster_v3.defs.common.ats_source import (
    local_snapshot_files,
    read_snapshot_manifest,
    sync_board_snapshots,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_lever import source, tables, transform

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
DUCKDB_POOL = "sweden_lever_duckdb"


@dg.asset(
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3", "lever"},
    description="Stores a complete successful snapshot of every reviewed Lever board.",
)
def sweden_lever_snapshot_s3(
    context: dg.AssetExecutionContext, sweden_lever_object_store: ObjectStoreResource
) -> dg.MaterializeResult:
    manifest = sync_board_snapshots(
        object_store=sweden_lever_object_store,
        bucket=tables.S3_BUCKET,
        provider=tables.PROVIDER,
        boards=source.BOARDS,
        fetch_board=source.fetch_board,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "board_count": len(manifest["boards"]),
            "job_count": sum(board["job_count"] for board in manifest["boards"]),
            "manifest_key": manifest["manifest_key"],
        }
    )


@dg.asset(
    deps=["sweden_lever_snapshot_s3"],
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "duckdb", "lever"},
    pool=DUCKDB_POOL,
    description="Normalizes the Lever snapshot into source-owned DuckDB tables.",
)
def sweden_lever_snapshot_duckdb(
    context: dg.AssetExecutionContext,
    sweden_lever_object_store: ObjectStoreResource,
    sweden_lever_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = read_snapshot_manifest(
        object_store=sweden_lever_object_store,
        bucket=tables.S3_BUCKET,
        provider=tables.PROVIDER,
        run_id=context.run.run_id,
    )
    with local_snapshot_files(
        object_store=sweden_lever_object_store,
        bucket=tables.S3_BUCKET,
        manifest=manifest,
    ) as snapshot_files:
        with sweden_lever_duckdb.get_connection() as connection:
            counts = transform.replace_lever_snapshot_tables(
                connection=connection, snapshot_files=snapshot_files
            )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=["sweden_lever_snapshot_duckdb"],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "lever"},
    pool=DUCKDB_POOL,
    description="Publishes Lever history and atomically replaces Lever current jobs.",
)
def sweden_lever_snapshot_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_lever_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_lever_duckdb) as connection:
        counts = publish_ats_snapshot(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            tables=_clickhouse_tables(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


def _clickhouse_tables() -> AtsClickhouseTables:
    return AtsClickhouseTables(
        database=tables.CLICKHOUSE_DATABASE,
        duckdb_schema=tables.DUCKDB_SCHEMA,
        boards=tables.BOARDS_TABLE,
        board_company_links=tables.BOARD_COMPANY_LINKS_TABLE,
        board_snapshots=tables.BOARD_SNAPSHOTS_TABLE,
        versions=tables.VERSIONS_TABLE,
        events=tables.EVENTS_TABLE,
        current=tables.CURRENT_TABLE,
        locations=tables.LOCATIONS_TABLE,
        compensations=tables.COMPENSATIONS_TABLE,
        columns=tables.TABLE_COLUMNS,
    )


sweden_lever_snapshot_job = dg.define_asset_job(
    "sweden_lever_snapshot_job",
    selection=dg.AssetSelection.assets("sweden_lever_snapshot_clickhouse").upstream(),
)
sweden_lever_daily_schedule = dg.ScheduleDefinition(
    name="sweden_lever_daily_schedule",
    job=sweden_lever_snapshot_job,
    cron_schedule="20 2 * * *",
    execution_timezone="Europe/Stockholm",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
defs = dg.Definitions(
    assets=[
        sweden_lever_snapshot_s3,
        sweden_lever_snapshot_duckdb,
        sweden_lever_snapshot_clickhouse,
    ],
    jobs=[sweden_lever_snapshot_job],
    schedules=[sweden_lever_daily_schedule],
    resources={
        "sweden_lever_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
        "sweden_lever_duckdb": duckdb_resource(DUCKDB_PATH),
    },
)
