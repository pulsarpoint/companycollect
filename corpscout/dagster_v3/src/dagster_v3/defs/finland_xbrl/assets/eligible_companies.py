from collections.abc import Callable

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    read_only_duckdb_connection,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource


def build_finland_xbrl_eligible_companies(
    *,
    ytj_duckdb: DuckDBResource,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    ytj_database_path = duckdb_database_path(ytj_duckdb)
    if log_info is not None:
        log_info(
            f"Building Finland XBRL eligible companies parquet from YTJ DuckDB {ytj_database_path}"
        )
    with read_only_duckdb_connection(ytj_duckdb) as connection:
        rows = connection.execute(
            """
            select
                business_id,
                primary_name,
                website_normalized_url
            from finland_prhytj.all_companies
            where is_active = true
              and coalesce(website_normalized_url, '') <> ''
            order by business_id
            """
        ).fetchall()
    eligible_companies = [
        {
            "business_id": business_id,
            "primary_name": primary_name,
            "website_normalized_url": website_normalized_url,
        }
        for business_id, primary_name, website_normalized_url in rows
    ]
    parquet_path = xbrl_parquet_storage.write_eligible_companies(eligible_companies)
    if log_info is not None:
        log_info(
            "Finland XBRL eligible companies parquet built: "
            f"eligible_companies_row_count={len(eligible_companies)}"
        )
    return dg.MaterializeResult(
        metadata={
            "parquet_path": str(parquet_path),
            "eligible_companies_row_count": len(eligible_companies),
            "row_count": len(eligible_companies),
        }
    )


@dg.asset(
    name="finland_xbrl_eligible_companies",
    group_name="finland_xbrl",
    deps=["finland_ytj_all_companies_duckdb"],
    kinds={"python", "parquet"},
    pool="finland_ytj_duckdb",
    description=(
        "Finland XBRL eligible-company parquet derived from YTJ companies with active status "
        "and a normalized website URL."
    ),
)
def finland_xbrl_eligible_companies(
    context: dg.AssetExecutionContext,
    ytj_duckdb: DuckDBResource,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    return build_finland_xbrl_eligible_companies(
        ytj_duckdb=ytj_duckdb,
        xbrl_parquet_storage=xbrl_parquet_storage,
        log_info=context.log.info,
    )
