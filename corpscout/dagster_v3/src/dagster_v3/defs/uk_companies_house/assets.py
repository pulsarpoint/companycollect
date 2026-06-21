from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.uk_companies_house import resources, tables
from dagster_v3.defs.uk_companies_house.clickhouse import (
    export_uk_companies_house_clickhouse_companies,
    export_uk_companies_house_clickhouse_financial_metrics,
    export_uk_companies_house_clickhouse_industries,
)
from dagster_v3.defs.uk_companies_house.documents_api import (
    CompaniesHouseClient,
    build_financials_for_company_numbers,
)
from dagster_v3.defs.uk_companies_house.financials import (
    apply_uk_usd_conversion,
    build_uk_companies_house_financials,
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


# --- Financials (XBRL accounts → metrics, GBP + USD) -------------------------
# Separate source/cadence from the register: the Accounts Data Product (daily
# iXBRL archives). Phase 1 ingests the latest archive (full-refresh); broader
# coverage accumulates by ingesting more archives (incremental = future).
@dg.asset(
    name="uk_companies_house_financial_metrics_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description=(
        "UK Companies House native-GBP financial metrics parsed from the latest "
        "iXBRL accounts archive (via the shared xbrl_common extractor)."
    ),
)
def uk_companies_house_financial_metrics_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    counts = build_uk_companies_house_financials(
        database_path=UK_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="uk_companies_house_financial_metrics_usd_duckdb",
    deps=[dg.AssetKey("uk_companies_house_financial_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description="Separate step: fill USD + fx columns via the shared exchange-rate client (GBP→USD).",
)
def uk_companies_house_financial_metrics_usd_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    counts = apply_uk_usd_conversion(
        database_path=UK_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("uk_companies_house_financial_metrics_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description=(
        "UK Companies House financial metrics exported to ClickHouse "
        "corpscout.gb_financial_metrics."
    ),
)
def uk_companies_house_clickhouse_financial_metrics(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_uk_companies_house_clickhouse_financial_metrics(
        database_path=UK_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    )


# --- On-demand: latest accounts per company via the CH API -------------------
class CompaniesHouseApiConfig(dg.Config):
    # Provided list of company numbers to fetch the latest accounts for.
    company_numbers: list[str] = []


@dg.asset(
    name="uk_companies_house_api_financial_metrics",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description=(
        "On-demand: fetch each provided company's latest accounts iXBRL via the "
        "Companies House API, parse to metrics (GBP+USD), and APPEND to "
        "corpscout.gb_financial_metrics (ReplacingMergeTree dedups by company)."
    ),
)
def uk_companies_house_api_financial_metrics(
    context: AssetExecutionContext,
    config: CompaniesHouseApiConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    client = CompaniesHouseClient.from_env()
    counts = build_financials_for_company_numbers(
        database_path=UK_DUCKDB_PATH,
        company_numbers=config.company_numbers,
        source_run_id=context.run_id,
        client=client,
        log=context.log.info,
    )
    apply_uk_usd_conversion(
        database_path=UK_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    rows = export_uk_companies_house_clickhouse_financial_metrics(
        database_path=UK_DUCKDB_PATH,
        clickhouse=clickhouse,
        truncate=False,  # append; do not wipe the archive-sourced rows
        log=context.log.info,
    )
    context.log.info("Appended UK API financial metrics to ClickHouse: rows=%s", rows)
    return dg.MaterializeResult(metadata={**counts, "exported_rows": rows})


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

# Financials (XBRL accounts) refresh on a separate cadence from the register.
uk_companies_house_financials_job = dg.define_asset_job(
    "uk_companies_house_financials_job",
    selection=dg.AssetSelection.assets(
        "uk_companies_house_clickhouse_financial_metrics"
    ).upstream(),
)
uk_companies_house_financials_schedule = dg.ScheduleDefinition(
    name="uk_companies_house_financials_schedule",
    job=uk_companies_house_financials_job,
    cron_schedule="0 8 8 * *",  # monthly, 8th 08:00
    execution_timezone="Europe/Belgrade",
)

# On-demand job: launch with config {company_numbers: [...]}; not scheduled.
uk_companies_house_api_financials_job = dg.define_asset_job(
    "uk_companies_house_api_financials_job",
    selection=dg.AssetSelection.assets("uk_companies_house_api_financial_metrics"),
)

uk_companies_house_full_refresh_job = dg.define_asset_job(
    "uk_companies_house_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)
