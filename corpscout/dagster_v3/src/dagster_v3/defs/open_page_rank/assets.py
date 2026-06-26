from datetime import UTC, datetime
from pathlib import Path
import tempfile

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.open_page_rank import tables
from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_RAW_TABLE,
    ExtractedOpenPageRankCsv,
    extract_single_csv_member,
    load_open_page_rank_raw_table,
    raw_table_row_count,
)
from dagster_v3.defs.open_page_rank.source import (
    OPEN_PAGE_RANK_RAW_BUCKET,
    OpenPageRankDownloadConfig,
    download_raw_file,
    manifest_for_run,
    select_open_page_rank_raw_keys_for_deletion,
)
from dagster_v3.defs.open_page_rank.transforms import (
    OPEN_PAGE_RANK_DUCKDB_SCHEMA,
    replace_current_open_page_rank_domains,
)

GROUP_NAME = "open_page_rank"
OPEN_PAGE_RANK_DUCKDB_PATH = Path("data/open_page_rank_source.duckdb")
OPEN_PAGE_RANK_DUCKDB_POOL = "open_page_rank_duckdb"
MIN_OPEN_PAGE_RANK_ROWS = 9_000_000


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "open_page_rank"},
    description="Downloads the DomCop/Open PageRank top 10M domains ZIP into object storage.",
)
def open_page_rank_raw_archive(
    context: dg.AssetExecutionContext,
    config: OpenPageRankDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_raw_file(
        context=context,
        object_store=object_store,
        config=config,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_raw_archive")],
    group_name=GROUP_NAME,
    kinds={"python", "dlt", "duckdb", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Loads the Open PageRank CSV from the raw ZIP into DuckDB using dlt and Arrow.",
)
def open_page_rank_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    open_page_rank_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = manifest_for_run(object_store, context.run_id)
    with tempfile.TemporaryDirectory(prefix="open-page-rank-") as temp_path:
        temp_dir = Path(temp_path)
        zip_path = temp_dir / "source.csv.zip"
        object_store.download_file(
            str(manifest["file"]["s3_key"]),
            zip_path,
            bucket=OPEN_PAGE_RANK_RAW_BUCKET,
        )
        csv_path = extract_single_csv_member(zip_path=zip_path, output_dir=temp_dir)
        load_open_page_rank_raw_table(
            database_path=OPEN_PAGE_RANK_DUCKDB_PATH,
            extracted_file=ExtractedOpenPageRankCsv(csv_path=csv_path),
        )
        with open_page_rank_duckdb.get_connection() as connection:
            row_counts = {OPEN_PAGE_RANK_RAW_TABLE: raw_table_row_count(connection)}

    row_count = int(next(iter(row_counts.values())))
    if row_count < MIN_OPEN_PAGE_RANK_ROWS:
        raise ValueError(
            f"Open PageRank raw load produced too few rows: {row_count} < {MIN_OPEN_PAGE_RANK_ROWS}"
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "source_run_id": str(manifest["run_id"]),
            "source_url": str(manifest["file"]["source_url"]),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_raw_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Normalizes the Open PageRank raw CSV table into the current DuckDB export table.",
)
def open_page_rank_domains_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    open_page_rank_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = manifest_for_run(object_store, context.run_id)
    with open_page_rank_duckdb.get_connection() as connection:
        row_count = replace_current_open_page_rank_domains(
            connection=connection,
            source_url=str(manifest["file"]["source_url"]),
            source_run_id=str(manifest["run_id"]),
            retrieved_date=str(manifest["retrieved_date"]),
            retrieved_at=str(manifest["retrieved_at"]),
        )
    return dg.MaterializeResult(metadata={"row_count": row_count})


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_domains_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Exports current Open PageRank domains from DuckDB to ClickHouse.",
)
def open_page_rank_domains_clickhouse(
    clickhouse: ClickhouseResource,
    open_page_rank_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.OPEN_PAGE_RANK_TABLES,
    )
    with open_page_rank_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=OPEN_PAGE_RANK_DUCKDB_SCHEMA,
                duckdb_table=tables.OPEN_PAGE_RANK_DOMAINS_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=tables.OPEN_PAGE_RANK_DOMAINS_TABLE,
                columns=tables.OPEN_PAGE_RANK_DOMAINS_COLUMNS,
                truncate=True,
            )
    return dg.MaterializeResult(metadata={"row_count": row_count})


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_domains_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "open_page_rank"},
    description="Deletes old Open PageRank raw ZIP blobs while preserving manifests.",
)
def open_page_rank_raw_retention(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    keys = object_store.list_keys("raw/", bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    keys_to_delete = select_open_page_rank_raw_keys_for_deletion(keys)
    deleted_count = object_store.delete_keys(tuple(keys_to_delete), bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    return dg.MaterializeResult(metadata={"deleted_key_count": deleted_count})


open_page_rank_domains_refresh_job = dg.define_asset_job(
    name="open_page_rank_domains_refresh_job",
    selection=[
        "open_page_rank_raw_archive",
        "open_page_rank_raw_duckdb",
        "open_page_rank_domains_duckdb",
        "open_page_rank_domains_clickhouse",
        "open_page_rank_raw_retention",
    ],
)

open_page_rank_domains_process_existing_raw_job = dg.define_asset_job(
    name="open_page_rank_domains_process_existing_raw_job",
    selection=[
        "open_page_rank_raw_duckdb",
        "open_page_rank_domains_duckdb",
        "open_page_rank_domains_clickhouse",
        "open_page_rank_raw_retention",
    ],
)

open_page_rank_domains_weekly = dg.ScheduleDefinition(
    name="open_page_rank_domains_weekly",
    job=open_page_rank_domains_refresh_job,
    cron_schedule="45 2 * * 0",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        open_page_rank_raw_archive,
        open_page_rank_raw_duckdb,
        open_page_rank_domains_duckdb,
        open_page_rank_domains_clickhouse,
        open_page_rank_raw_retention,
    ],
    jobs=[
        open_page_rank_domains_refresh_job,
        open_page_rank_domains_process_existing_raw_job,
    ],
    schedules=[open_page_rank_domains_weekly],
    resources={"open_page_rank_duckdb": duckdb_resource(OPEN_PAGE_RANK_DUCKDB_PATH)},
)
