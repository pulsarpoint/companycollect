from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_tables_in_clickhouse,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.source import (
    GLEIF_RAW_BUCKET,
    GleifRawDownloadConfig,
    download_golden_copy_files,
    select_gleif_raw_keys_for_deletion,
)

GROUP_NAME = "gleif"
GLEIF_DUCKDB_PATH = Path("data/gleif.duckdb")
GLEIF_DUCKDB_SCHEMA = f"{GLEIF_DUCKDB_PATH.stem}.gleif"
GLEIF_DUCKDB_POOL = "gleif_duckdb"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads full GLEIF Golden Copy ZIP files into object storage for bootstrap/recovery.",
)
def gleif_full_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="full",
        delta=None,
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads LastDay GLEIF Golden Copy delta ZIP files into object storage.",
)
def gleif_delta_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="delta",
        delta="LastDay",
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )


@dg.asset(
    deps=[
        dg.AssetKey("gleif_full_raw_reference_files"),
        dg.AssetKey("gleif_delta_raw_reference_files"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Maintains current GLEIF normalized state in DuckDB from full or delta raw files.",
)
def gleif_reference_duckdb_state(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    from dagster_v3.defs.gleif.duckdb_state import refresh_gleif_duckdb_state

    return refresh_gleif_duckdb_state(
        context=context,
        object_store=object_store,
        database_path=GLEIF_DUCKDB_PATH,
    )


@dg.asset(
    deps=[dg.AssetKey("gleif_reference_duckdb_state")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Exports current GLEIF DuckDB state to ClickHouse corpscout.gleif_* tables.",
)
def gleif_reference_clickhouse(clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.GLEIF_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_tables_in_clickhouse(
            duckdb_path=GLEIF_DUCKDB_PATH,
            clickhouse_client=client,
            duckdb_schema=GLEIF_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table_name, tables.GLEIF_TABLE_COLUMNS[table_name])
                for table_name in tables.GLEIF_TABLES
            ),
        )
    return dg.MaterializeResult(
        metadata={f"{table_name}_row_count": count for table_name, count in row_counts.items()}
    )


@dg.asset(
    deps=[dg.AssetKey("gleif_reference_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Deletes old GLEIF raw blobs while preserving manifests and the newest raw snapshot.",
)
def gleif_raw_retention(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    keys = object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
    keys_to_delete = select_gleif_raw_keys_for_deletion(keys)
    deleted_count = object_store.delete_keys(tuple(keys_to_delete), bucket=GLEIF_RAW_BUCKET)
    return dg.MaterializeResult(metadata={"deleted_key_count": deleted_count})


gleif_reference_bootstrap_job = dg.define_asset_job(
    name="gleif_reference_bootstrap_job",
    selection=[
        "gleif_full_raw_reference_files",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_job = dg.define_asset_job(
    name="gleif_reference_delta_job",
    selection=[
        "gleif_delta_raw_reference_files",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_daily = dg.ScheduleDefinition(
    name="gleif_reference_delta_daily",
    job=gleif_reference_delta_job,
    cron_schedule="30 20 * * *",
    execution_timezone="UTC",
)

defs = dg.Definitions(
    assets=[
        gleif_full_raw_reference_files,
        gleif_delta_raw_reference_files,
        gleif_reference_duckdb_state,
        gleif_reference_clickhouse,
        gleif_raw_retention,
    ],
    jobs=[gleif_reference_bootstrap_job, gleif_reference_delta_job],
    schedules=[gleif_reference_delta_daily],
)
