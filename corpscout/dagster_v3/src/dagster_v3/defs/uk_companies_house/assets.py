from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.uk_companies_house import resources, tables
from dagster_v3.defs.uk_companies_house.clickhouse import (
    export_uk_companies_house_clickhouse_companies,
    export_uk_companies_house_clickhouse_industries,
)
from dagster_v3.defs.uk_companies_house.industries import (
    build_uk_companies_house_industries,
)

GROUP_NAME = "uk_companies_house"
UK_DUCKDB_POOL = "uk_companies_house_duckdb"
UK_DUCKDB_PATH = Path("data/uk_companies_house_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RAW_ASSET_KEY = "uk_companies_house_raw_duckdb"


@dg.asset(
    name=RAW_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description=(
        "UK Companies House BasicCompanyData loaded raw into DuckDB via the "
        "multithreaded read_csv (URL resolved live from the download index)."
    ),
)
def uk_companies_house_raw_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    download_url = resources.resolve_basic_company_data_url()
    context.log.info(
        "Loading UK Companies House: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        UK_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.COMPANIES_RAW_TABLE,
    )
    rows = resources.load_uk_companies_house_raw(
        database_path=UK_DUCKDB_PATH,
        download_url=download_url,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata={"rows": rows, "source_url": download_url})


@dg.asset(
    name="uk_companies_house_companies_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description="UK Companies House normalized companies (name/category/status/address).",
)
def uk_companies_house_companies_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    source_url = resources.resolve_basic_company_data_url()
    counts = resources.build_uk_companies_house_companies(
        database_path=UK_DUCKDB_PATH,
        source_run_id=context.run_id,
        source_url=source_url,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("uk_companies_house_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    description="UK Companies House companies exported to ClickHouse corpscout.gb_companies.",
)
def uk_companies_house_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_uk_companies_house_clickhouse_companies(
        database_path=UK_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_COMPANIES_TABLE},
    )


@dg.asset(
    name="uk_companies_house_industries_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description=(
        "UK Companies House industries from the 4 SIC columns "
        "(UK SIC 2007 → NACE by first-4-digit truncation)."
    ),
)
def uk_companies_house_industries_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    counts = build_uk_companies_house_industries(
        database_path=UK_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("uk_companies_house_industries_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_INDUSTRIES_TABLE},
    description=(
        "UK Companies House industries exported to ClickHouse corpscout.gb_industries "
        "(joins nace_categories on nace_code/nace_revision)."
    ),
)
def uk_companies_house_clickhouse_industries(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_uk_companies_house_clickhouse_industries(
        database_path=UK_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_INDUSTRIES_TABLE},
    )


# --- Jobs & schedules --------------------------------------------------------
# Register + industries both derive from the ONE monthly bulk download.
uk_companies_house_register_job = dg.define_asset_job(
    "uk_companies_house_register_job",
    selection=dg.AssetSelection.assets(
        "uk_companies_house_clickhouse_companies",
        "uk_companies_house_clickhouse_industries",
    ).upstream(),
)
uk_companies_house_register_schedule = dg.ScheduleDefinition(
    name="uk_companies_house_register_schedule",
    job=uk_companies_house_register_job,
    cron_schedule="0 7 7 * *",  # monthly, 7th 07:00 (after the new snapshot)
    execution_timezone="Europe/Belgrade",
)

uk_companies_house_full_refresh_job = dg.define_asset_job(
    "uk_companies_house_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)
