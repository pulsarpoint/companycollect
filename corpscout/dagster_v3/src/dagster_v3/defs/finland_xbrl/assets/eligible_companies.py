from pathlib import Path
from typing import Callable

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_database_path
from dagster_v3.defs.finland_xbrl.assets.common import (
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_DLT_DATASET_NAME,
    XBRL_ELIGIBLE_COMPANIES_TABLE,
)

def _duckdb_string_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def materialize_eligible_companies_snapshot(
    *,
    source_duckdb: DuckDBResource,
    ytj_duckdb: DuckDBResource,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    ytj_database_path = duckdb_database_path(ytj_duckdb)
    if log_info is not None:
        log_info(f"Copying Finland XBRL eligible companies from YTJ DuckDB {ytj_database_path}")
    with source_duckdb.get_connection() as connection:
        connection.execute(f"create schema if not exists {XBRL_DLT_DATASET_NAME}")
        connection.execute(
            f"attach {_duckdb_string_literal(ytj_database_path)} as ytj_source (READ_ONLY)"
        )
        try:
            connection.execute(
                f"""
                create or replace table {XBRL_DLT_DATASET_NAME}.{XBRL_ELIGIBLE_COMPANIES_TABLE} as
                select
                    business_id,
                    primary_name,
                    website_normalized_url
                from ytj_source.finland_prhytj.all_companies
                where is_active = true
                  and coalesce(website_normalized_url, '') <> ''
                """
            )
            row_count = connection.execute(
                f"select count(*) from {XBRL_DLT_DATASET_NAME}.{XBRL_ELIGIBLE_COMPANIES_TABLE}"
            ).fetchone()[0]
        finally:
            connection.execute("detach ytj_source")

    if log_info is not None:
        log_info(f"Finland XBRL eligible companies copied: row_count={row_count}")
    return dg.MaterializeResult(
        metadata={
            "duckdb_schema": XBRL_DLT_DATASET_NAME,
            "duckdb_table": XBRL_ELIGIBLE_COMPANIES_TABLE,
            "row_count": row_count,
        }
    )


@dg.asset(
    name="finland_xbrl_eligible_companies",
    group_name="finland_xbrl",
    deps=["finland_ytj_all_companies_duckdb"],
    kinds={"python", "duckdb"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Active Finland YTJ companies with websites copied into the PRH XBRL DuckDB boundary.",
)
def finland_xbrl_eligible_companies(
    context: dg.AssetExecutionContext,
    source_duckdb: DuckDBResource,
    ytj_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return materialize_eligible_companies_snapshot(
        source_duckdb=source_duckdb,
        ytj_duckdb=ytj_duckdb,
        log_info=context.log.info,
    )
