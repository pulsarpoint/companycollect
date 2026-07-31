"""GLEIF ISIN-to-LEI: download, stage in DuckDB, publish to ClickHouse.

Daily, because GLEIF republishes daily. Not partitioned: the file is a full
snapshot of the current mapping, so there is no period to backfill.

The CSV is handed to DuckDB's C++ reader rather than parsed in Python — 9.1M
rows through a row-at-a-time loader is the slow path this repo forbids.
"""

from datetime import UTC, datetime
from pathlib import Path
import tempfile

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.isin_lei import tables
from dagster_v3.defs.isin_lei.source import (
    GLEIF_ISIN_LEI_LISTING_URL,
    choose_latest_file,
    extract_isin_lei_csv,
    list_isin_lei_files,
    resolve_file_name,
)

GROUP_NAME = "isin_lei"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "gleif"},
    pool=tables.ISIN_LEI_DUCKDB_POOL,
    description=(
        "Downloads GLEIF's newest ISIN-to-LEI file and stages its 9.1M pairs "
        "in DuckDB."
    ),
)
def gleif_isin_lei_duckdb(
    context: AssetExecutionContext,
    isin_lei_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    listing = list_isin_lei_files()
    links = [
        ((item or {}).get("attributes") or {}).get("downloadLink")
        for item in (listing.get("data") or [])
    ]
    # GLEIF names the file in Content-Disposition, not in the listing, and the
    # name carries the only publication timestamp available.
    file_names = {url: resolve_file_name(url) for url in links if url}
    latest = choose_latest_file(listing, file_names=file_names)
    context.log.info("newest ISIN-to-LEI file: %s", latest.file_name)

    response = requests.get(latest.download_url, timeout=300)
    response.raise_for_status()

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = extract_isin_lei_csv(response.content, Path(tmp) / "isin_lei.csv")
        with isin_lei_duckdb.get_connection() as connection:
            connection.execute(
                f"CREATE SCHEMA IF NOT EXISTS {tables.ISIN_LEI_DUCKDB_SCHEMA}"
            )
            # DuckDB's multithreaded C++ CSV reader. Columns stay text: an ISIN
            # and an LEI are identifiers, never numbers.
            connection.execute(
                f"""
                CREATE OR REPLACE TABLE
                  {tables.ISIN_LEI_DUCKDB_SCHEMA}.{tables.ISIN_LEI_RAW_TABLE} AS
                SELECT
                    trim(ISIN) AS isin,
                    trim(LEI) AS lei,
                    '{GLEIF_ISIN_LEI_LISTING_URL}' AS source_url,
                    '{latest.file_name}' AS source_file_name,
                    '{context.run_id}' AS source_run_id,
                    TIMESTAMP '{datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")}' AS retrieved_at
                FROM read_csv(
                    '{csv_path}', header = true, all_varchar = true,
                    quote = '"', escape = '"'
                )
                WHERE trim(ISIN) != '' AND trim(LEI) != ''
                """
            )
            rows = connection.execute(
                f"SELECT count(*) FROM "
                f"{tables.ISIN_LEI_DUCKDB_SCHEMA}.{tables.ISIN_LEI_RAW_TABLE}"
            ).fetchone()[0]

    if rows < tables.MIN_ISIN_LEI_ROWS:
        # Refuse a short read rather than publish a mapping with holes in it.
        raise ValueError(
            f"ISIN-to-LEI yielded {rows} pairs, below the "
            f"{tables.MIN_ISIN_LEI_ROWS} floor"
        )
    return dg.MaterializeResult(
        metadata={"rows": int(rows), "source_file_name": latest.file_name}
    )


@dg.asset(
    deps=[dg.AssetKey("gleif_isin_lei_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse", "gleif"},
    pool=tables.ISIN_LEI_DUCKDB_POOL,
    description="Publishes the ISIN-to-LEI mapping from DuckDB to ClickHouse.",
)
def gleif_isin_lei_clickhouse(
    clickhouse: ClickhouseResource,
    isin_lei_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(tables.GLEIF_ISIN_LEI_TABLE,),
    )
    with isin_lei_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=tables.ISIN_LEI_DUCKDB_SCHEMA,
                duckdb_table=tables.ISIN_LEI_RAW_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=tables.GLEIF_ISIN_LEI_TABLE,
                columns=tables.GLEIF_ISIN_LEI_COLUMNS,
                truncate=True,
            )
    return dg.MaterializeResult(metadata={"row_count": rows})


isin_lei_job = dg.define_asset_job(
    name="isin_lei_job",
    selection=dg.AssetSelection.assets(
        gleif_isin_lei_duckdb, gleif_isin_lei_clickhouse
    ),
)

isin_lei_daily = dg.ScheduleDefinition(
    name="isin_lei_daily",
    job=isin_lei_job,
    cron_schedule="50 5 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[gleif_isin_lei_duckdb, gleif_isin_lei_clickhouse],
    jobs=[isin_lei_job],
    schedules=[isin_lei_daily],
    resources={"isin_lei_duckdb": duckdb_resource(tables.ISIN_LEI_DUCKDB_PATH)},
)
