from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.czech_ares import resources, tables
from dagster_v3.defs.czech_ares.clickhouse import (
    export_czech_ares_clickhouse_companies,
    export_czech_ares_clickhouse_industries,
)
from dagster_v3.defs.czech_ares.industries import build_czech_ares_industries
from dagster_v3.defs.common.duckdb_resources import duckdb_resource

GROUP_NAME = "czech_ares"
CZECH_ARES_DUCKDB_POOL = "czech_ares_duckdb"
CZECH_ARES_DUCKDB_PATH = Path("data/czech_ares_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RAW_ASSET_KEY = "czech_ares_res_raw_duckdb"


@dg.asset(
    name=RAW_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=CZECH_ARES_DUCKDB_POOL,
    description="Czech CSU RES (res_data.csv) loaded raw into DuckDB via multithreaded read_csv.",
)
def czech_ares_res_raw_duckdb(
    context: AssetExecutionContext,
    czech_ares_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Loading Czech RES: url=%s, duckdb_path=%s", tables.RES_DATA_URL, CZECH_ARES_DUCKDB_PATH
    )
    CZECH_ARES_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with czech_ares_duckdb.get_connection() as connection:
        rows = resources.load_czech_ares_res(connection=connection, log=context.log.info)
    return dg.MaterializeResult(metadata={"rows": rows, "source_url": tables.RES_DATA_URL})


@dg.asset(
    name="czech_ares_companies_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=CZECH_ARES_DUCKDB_POOL,
    description="Czech ARES normalized companies (name/legal form/status/address).",
)
def czech_ares_companies_duckdb(
    context: AssetExecutionContext,
    czech_ares_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with czech_ares_duckdb.get_connection() as connection:
        counts = resources.build_czech_ares_companies(
            connection=connection, source_run_id=context.run_id, log=context.log.info
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("czech_ares_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=CZECH_ARES_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    description="Czech ARES companies exported to ClickHouse corpscout.cz_companies.",
)
def czech_ares_clickhouse_companies(
    context: AssetExecutionContext,
    czech_ares_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with czech_ares_duckdb.get_connection() as connection:
        rows = export_czech_ares_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "table": tables.QUALIFIED_COMPANIES_TABLE})


@dg.asset(
    name="czech_ares_industries_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=CZECH_ARES_DUCKDB_POOL,
    description="Czech ARES industries from the main CZ-NACE activity (→ NACE).",
)
def czech_ares_industries_duckdb(
    context: AssetExecutionContext,
    czech_ares_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with czech_ares_duckdb.get_connection() as connection:
        counts = build_czech_ares_industries(
            connection=connection, source_run_id=context.run_id, log=context.log.info
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("czech_ares_industries_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=CZECH_ARES_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_INDUSTRIES_TABLE},
    description=(
        "Czech ARES industries exported to ClickHouse corpscout.cz_industries "
        "(joins nace_categories on nace_code/nace_revision)."
    ),
)
def czech_ares_clickhouse_industries(
    context: AssetExecutionContext,
    czech_ares_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with czech_ares_duckdb.get_connection() as connection:
        rows = export_czech_ares_clickhouse_industries(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "table": tables.QUALIFIED_INDUSTRIES_TABLE})


# --- Jobs & schedules --------------------------------------------------------
# Register + industries both derive from the ONE res_data.csv download.
czech_ares_register_job = dg.define_asset_job(
    "czech_ares_register_job",
    selection=dg.AssetSelection.assets(
        "czech_ares_clickhouse_companies", "czech_ares_clickhouse_industries"
    ).upstream(),
)
czech_ares_register_schedule = dg.ScheduleDefinition(
    name="czech_ares_register_schedule",
    job=czech_ares_register_job,
    cron_schedule="0 7 17 * *",  # monthly, 17th 07:00 (after the mid-month snapshot)
    execution_timezone="Europe/Belgrade",
)

defs = dg.Definitions(
    assets=[
        czech_ares_res_raw_duckdb,
        czech_ares_companies_duckdb,
        czech_ares_clickhouse_companies,
        czech_ares_industries_duckdb,
        czech_ares_clickhouse_industries,
    ],
    jobs=[
        czech_ares_register_job,
    ],
    schedules=[czech_ares_register_schedule],
    resources={
        "czech_ares_duckdb": duckdb_resource(CZECH_ARES_DUCKDB_PATH),
    },
)
