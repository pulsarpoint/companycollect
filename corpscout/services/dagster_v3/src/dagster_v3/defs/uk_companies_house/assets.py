from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.uk_companies_house import raw_archives, resources, tables
from dagster_v3.defs.uk_companies_house.clickhouse import (
    export_uk_companies_house_clickhouse_companies,
    export_uk_companies_house_clickhouse_financial_metrics,
    export_uk_companies_house_clickhouse_industries,
)
from dagster_v3.defs.uk_companies_house.documents_api import (
    load_api_financial_metrics_from_object_store,
    sync_api_accounts_documents,
)
from dagster_v3.defs.uk_companies_house.financials import (
    apply_uk_usd_conversion,
    build_uk_companies_house_financials,
    write_metrics_table,
)
from dagster_v3.defs.uk_companies_house.pdf_extract import extract_pdf_financials
from dagster_v3.defs.uk_companies_house.incremental import (
    build_incremental_metrics,
    write_cursor,
)
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS
from dagster_v3.defs.uk_companies_house.industries import (
    build_uk_companies_house_industries,
)

GROUP_NAME = "uk_companies_house"
UK_DUCKDB_POOL = "uk_companies_house_duckdb"
UK_DUCKDB_PATH = Path("data/uk_companies_house_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RAW_ASSET_KEY = "uk_companies_house_raw_duckdb"
REGISTER_ARCHIVE_ASSET_KEY = "uk_companies_house_register_archive_s3"
ACCOUNTS_ARCHIVES_ASSET_KEY = "uk_companies_house_accounts_archives_s3"


class CompaniesHouseAccountsConfig(dg.Config):
    max_archives: int = 10


@dg.asset(
    name=REGISTER_ARCHIVE_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip"},
    description=(
        "UK Companies House monthly BasicCompanyData ZIP persisted immutably "
        "in RustFS/S3 before DuckDB processing."
    ),
)
def uk_companies_house_register_archive_s3(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = raw_archives.sync_register_archive(
        object_store=object_store,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name=RAW_ASSET_KEY,
    deps=[dg.AssetKey(REGISTER_ARCHIVE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description=(
        "UK Companies House BasicCompanyData loaded from RustFS/S3 into DuckDB "
        "via the multithreaded read_csv."
    ),
)
def uk_companies_house_raw_duckdb(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    UK_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with uk_companies_house_duckdb.get_connection() as connection:
        result = resources.load_uk_companies_house_raw_from_object_store(
            connection=connection,
            object_store=object_store,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=result)


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
    object_store: ObjectStoreResource,
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    archive = raw_archives.latest_stored_archive(
        object_store,
        kind=raw_archives.REGISTER_KIND,
    )
    with uk_companies_house_duckdb.get_connection() as connection:
        counts = resources.build_uk_companies_house_companies(
            connection=connection,
            source_run_id=context.run_id,
            source_url=archive.source_url,
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
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with uk_companies_house_duckdb.get_connection() as connection:
        rows = export_uk_companies_house_clickhouse_companies(
            duckdb_connection=connection,
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
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with uk_companies_house_duckdb.get_connection() as connection:
        counts = build_uk_companies_house_industries(
            connection=connection,
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
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with uk_companies_house_duckdb.get_connection() as connection:
        rows = export_uk_companies_house_clickhouse_industries(
            duckdb_connection=connection,
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
    name=ACCOUNTS_ARCHIVES_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip"},
    description=(
        "UK Companies House daily Accounts Data Product ZIPs persisted immutably "
        "in RustFS/S3 before parsing."
    ),
)
def uk_companies_house_accounts_archives_s3(
    context: AssetExecutionContext,
    config: CompaniesHouseAccountsConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = raw_archives.sync_accounts_archives(
        object_store=object_store,
        max_archives=config.max_archives,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name="uk_companies_house_financial_metrics_duckdb",
    deps=[dg.AssetKey(ACCOUNTS_ARCHIVES_ASSET_KEY)],
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
    object_store: ObjectStoreResource,
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    UK_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with uk_companies_house_duckdb.get_connection() as connection:
        counts = build_uk_companies_house_financials(
            connection=connection,
            object_store=object_store,
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
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with uk_companies_house_duckdb.get_connection() as connection:
        counts = apply_uk_usd_conversion(
            connection=connection,
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
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with uk_companies_house_duckdb.get_connection() as connection:
        rows = export_uk_companies_house_clickhouse_financial_metrics(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            truncate=False,  # append; gb_financial_metrics accumulates (ReplacingMergeTree)
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    )


# --- PoC: PDF-only accounts via OCR + LLM ------------------------------------
class CompaniesHousePdfConfig(dg.Config):
    company_numbers: list[str] = []
    max_pages: int = 12


@dg.asset(
    name="uk_companies_house_pdf_financial_metrics",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description=(
        "PoC: for companies whose latest accounts are PDF-only (no XBRL), OCR the "
        "PDF and extract metrics with an LLM. APPENDs to gb_financial_metrics with "
        "source_slug='uk_companies_house_accounts_pdf' (lower trust than XBRL)."
    ),
)
def uk_companies_house_pdf_financial_metrics(
    context: AssetExecutionContext,
    config: CompaniesHousePdfConfig,
    companies_house_api: resources.CompaniesHouseResource,
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    import datetime as dt
    import os

    from exchange_rates import ExchangeRateClient

    base_url = os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"]
    model = os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
    api_key = os.environ["TRANSLATION_PROVIDER_LOCAL_API_KEY"]
    rows: list[tuple] = []
    fetched = 0
    missing = 0
    for company_number in config.company_numbers:
        cn = str(company_number).strip()
        pdf = companies_house_api.latest_accounts_pdf(cn)
        if not pdf:
            missing += 1
            continue
        result = extract_pdf_financials(
            pdf, base_url=base_url, model=model, api_key=api_key,
            max_pages=config.max_pages, log=context.log.info,
        )
        period = (result or {}).get("period_end_date")
        try:
            period_end = dt.date.fromisoformat(period) if period else None
        except ValueError:
            period_end = None
        if not result or period_end is None:
            missing += 1
            continue
        rows.append((
            cn, period_end, period_end.year, result["currency"],
            *(result["metrics"].get(m) for m in tables.UK_FINANCIAL_METRIC_NAMES),
        ))
        fetched += 1
        context.log.info("PDF financials for %s: confidence=%s", cn, result.get("confidence"))

    with uk_companies_house_duckdb.get_connection() as connection:
        counts = write_metrics_table(
            connection=connection, rows=rows, source_run_id=context.run_id,
            source_slug="uk_companies_house_accounts_pdf", allow_empty=True,
        )
        if rows:
            apply_uk_usd_conversion(
                connection=connection,
                exchange_rates=ExchangeRateClient.from_env(),
                log=context.log.info,
            )
        if rows:
            export_uk_companies_house_clickhouse_financial_metrics(
                duckdb_connection=connection,
                clickhouse=clickhouse,
                truncate=False,
                log=context.log.info,
            )
    return dg.MaterializeResult(metadata={**counts, "fetched": fetched, "missing": missing})


# --- Forward-only incremental: latest annual report for all filers -----------
@dg.asset(
    name="uk_companies_house_accounts_incremental",
    deps=[dg.AssetKey(ACCOUNTS_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description=(
        "Forward-only incremental: parse the accounts archives published since the "
        "cursor, GBP+USD, and APPEND to corpscout.gb_financial_metrics. Over ~12 "
        "months this converges on the latest annual report for every iXBRL filer."
    ),
)
def uk_companies_house_accounts_incremental(
    context: AssetExecutionContext,
    config: CompaniesHouseAccountsConfig,
    object_store: ObjectStoreResource,
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    UK_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with uk_companies_house_duckdb.get_connection() as connection:
        counts = build_incremental_metrics(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            max_archives=config.max_archives,
            log=context.log.info,
        )
        if counts["processed_archives"]:
            apply_uk_usd_conversion(
                connection=connection,
                exchange_rates=ExchangeRateClient.from_env(),
                log=context.log.info,
            )
        if not counts["processed_archives"]:
            context.log.info(
                "No new UK accounts archives since cursor=%s", counts["cursor_before"]
            )
            return dg.MaterializeResult(metadata=counts)

        rows = export_uk_companies_house_clickhouse_financial_metrics(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            truncate=False,
            log=context.log.info,
        )
        # Advance the cursor only after a successful append.
        write_cursor(connection, max(counts["processed_archives"]))
    context.log.info("Appended UK incremental metrics: rows=%s", rows)
    return dg.MaterializeResult(metadata={**counts, "exported_rows": rows})


# --- On-demand: latest accounts per company via the CH API -------------------
class CompaniesHouseApiConfig(dg.Config):
    company_numbers: list[str]
    request_delay_seconds: float = 0.5


API_ACCOUNTS_DOCUMENTS_ASSET_KEY = "uk_companies_house_api_accounts_documents_s3"
API_FINANCIAL_METRICS_DUCKDB_ASSET_KEY = "uk_companies_house_api_financial_metrics_duckdb"
API_FINANCIAL_METRICS_USD_DUCKDB_ASSET_KEY = "uk_companies_house_api_financial_metrics_usd_duckdb"


@dg.asset(
    name=API_ACCOUNTS_DOCUMENTS_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "ixbrl"},
    description=(
        "On-demand Companies House latest-accounts iXBRL documents persisted "
        "immutably in RustFS/S3 for configured company numbers."
    ),
)
def uk_companies_house_api_accounts_documents_s3(
    context: AssetExecutionContext,
    config: CompaniesHouseApiConfig,
    object_store: ObjectStoreResource,
    companies_house_api: resources.CompaniesHouseResource,
) -> dg.MaterializeResult:
    result = sync_api_accounts_documents(
        object_store=object_store,
        company_numbers=config.company_numbers,
        run_id=context.run_id,
        client=companies_house_api,
        request_delay_seconds=config.request_delay_seconds,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name=API_FINANCIAL_METRICS_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(API_ACCOUNTS_DOCUMENTS_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "ixbrl"},
    pool=UK_DUCKDB_POOL,
    description=(
        "Parse the current run's persisted API iXBRL documents into native-currency "
        "financial metrics in DuckDB."
    ),
)
def uk_companies_house_api_financial_metrics_duckdb(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    UK_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with uk_companies_house_duckdb.get_connection() as connection:
        counts = load_api_financial_metrics_from_object_store(
            connection=connection,
            object_store=object_store,
            run_id=context.run_id,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=API_FINANCIAL_METRICS_USD_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(API_FINANCIAL_METRICS_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=UK_DUCKDB_POOL,
    description="Fill API-derived financial USD and FX columns in a separate DuckDB step.",
)
def uk_companies_house_api_financial_metrics_usd_duckdb(
    context: AssetExecutionContext,
    uk_companies_house_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with uk_companies_house_duckdb.get_connection() as connection:
        counts = apply_uk_usd_conversion(
            connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="uk_companies_house_api_financial_metrics",
    deps=[dg.AssetKey(API_FINANCIAL_METRICS_USD_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=UK_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description=(
        "On-demand: append persisted and parsed Companies House API metrics to "
        "corpscout.gb_financial_metrics (ReplacingMergeTree dedups by company)."
    ),
)
def uk_companies_house_api_financial_metrics(
    context: AssetExecutionContext,
    uk_companies_house_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with uk_companies_house_duckdb.get_connection() as connection:
        rows = export_uk_companies_house_clickhouse_financial_metrics(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            truncate=False,  # append; do not wipe the archive-sourced rows
            log=context.log.info,
        )
    context.log.info("Appended UK API financial metrics to ClickHouse: rows=%s", rows)
    return dg.MaterializeResult(metadata={"exported_rows": rows})


# --- Jobs & schedules --------------------------------------------------------
# Register + industries both derive from the ONE monthly bulk download.
uk_companies_house_register_job = dg.define_asset_job(
    "uk_companies_house_register_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=dg.AssetSelection.assets(
        "uk_companies_house_clickhouse_companies",
        "uk_companies_house_clickhouse_industries",
    ).upstream(),
)
uk_companies_house_register_schedule = dg.ScheduleDefinition(
    name="uk_companies_house_register_schedule",
    job=uk_companies_house_register_job,
    cron_schedule="30 7 7 * *",  # monthly, 7th 07:30 (after the new snapshot)
    execution_timezone="Europe/Belgrade",
)

# Financials (XBRL accounts) refresh on a separate cadence from the register.
uk_companies_house_financials_job = dg.define_asset_job(
    "uk_companies_house_financials_job",
    tags=HEAVY_BULK_RUN_TAGS,
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
    selection=dg.AssetSelection.assets(
        "uk_companies_house_api_financial_metrics"
    ).upstream(),
)

# On-demand PDF-only (OCR+LLM) job; launch with config {company_numbers: [...]}.
uk_companies_house_pdf_financials_job = dg.define_asset_job(
    "uk_companies_house_pdf_financials_job",
    selection=dg.AssetSelection.assets("uk_companies_house_pdf_financial_metrics"),
)

# Forward-only incremental: the daily archives publish daily → daily schedule.
uk_companies_house_accounts_incremental_job = dg.define_asset_job(
    "uk_companies_house_accounts_incremental_job",
    selection=dg.AssetSelection.assets(
        "uk_companies_house_accounts_incremental"
    ).upstream(),
)
uk_companies_house_accounts_incremental_schedule = dg.ScheduleDefinition(
    name="uk_companies_house_accounts_incremental_schedule",
    job=uk_companies_house_accounts_incremental_job,
    cron_schedule="0 9 * * *",  # daily 09:00 (after the new archive publishes)
    execution_timezone="Europe/Belgrade",
)

uk_companies_house_full_refresh_job = dg.define_asset_job(
    "uk_companies_house_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)


defs = dg.Definitions(
    assets=[
        uk_companies_house_register_archive_s3,
        uk_companies_house_raw_duckdb,
        uk_companies_house_companies_duckdb,
        uk_companies_house_clickhouse_companies,
        uk_companies_house_industries_duckdb,
        uk_companies_house_clickhouse_industries,
        uk_companies_house_accounts_archives_s3,
        uk_companies_house_financial_metrics_duckdb,
        uk_companies_house_financial_metrics_usd_duckdb,
        uk_companies_house_clickhouse_financial_metrics,
        uk_companies_house_pdf_financial_metrics,
        uk_companies_house_accounts_incremental,
        uk_companies_house_api_accounts_documents_s3,
        uk_companies_house_api_financial_metrics_duckdb,
        uk_companies_house_api_financial_metrics_usd_duckdb,
        uk_companies_house_api_financial_metrics,
    ],
    jobs=[
        uk_companies_house_register_job,
        uk_companies_house_financials_job,
        uk_companies_house_api_financials_job,
        uk_companies_house_pdf_financials_job,
        uk_companies_house_accounts_incremental_job,
        uk_companies_house_full_refresh_job,
    ],
    schedules=[
        uk_companies_house_register_schedule,
        uk_companies_house_financials_schedule,
        uk_companies_house_accounts_incremental_schedule,
    ],
    resources={
        "companies_house_api": resources.CompaniesHouseResource(),
        "uk_companies_house_duckdb": duckdb_resource(UK_DUCKDB_PATH),
    },
)
