from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.estonia_ar import resources, tables
from dagster_v3.defs.estonia_ar.clickhouse import (
    export_estonia_ar_clickhouse_companies,
    export_estonia_ar_clickhouse_company_contacts,
    export_estonia_ar_clickhouse_company_domains,
    export_estonia_ar_clickhouse_financial_metrics,
    export_estonia_ar_clickhouse_financial_statements,
)
from dagster_v3.defs.estonia_ar.company_domains import build_estonia_ar_company_domains
from dagster_v3.defs.estonia_ar.contacts import build_estonia_ar_company_contacts
from dagster_v3.defs.estonia_ar.financials import (
    build_estonia_ar_financial_statements,
    load_estonia_ar_financial_csv,
    resolve_financial_url,
)
from dagster_v3.defs.estonia_ar.metrics import (
    apply_estonia_ar_usd_conversion,
    build_estonia_ar_financial_metrics,
)

GROUP_NAME = "estonia_ar"
ESTONIA_AR_DUCKDB_POOL = "estonia_ar_duckdb"
# File stem (catalog) must differ from DLT_DATASET_NAME or DuckDB's binder cannot
# disambiguate "<dataset>.<table>".
ESTONIA_AR_DUCKDB_PATH = Path("data/estonia_ar_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
ENTITIES_ASSET_KEY = "estonia_ar_entities_duckdb"


class EstoniaArDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != ENTITIES_TABLE:
            return spec
        return spec.replace_attributes(
            key=dg.AssetKey(ENTITIES_ASSET_KEY),
            deps=[],
            group_name=GROUP_NAME,
            description="Estonia AR register entity bulk data loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


def estonia_ar_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "pipeline_name": "estonia_ar_register",
        "destination": dlt.destinations.duckdb(str(database_path)),
        "dataset_name": DLT_DATASET_NAME,
        "dev_mode": False,
    }
    if pipelines_dir is not None:
        kwargs["pipelines_dir"] = str(pipelines_dir)
    return dlt.pipeline(**kwargs)


def run_estonia_ar_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: resources.HttpSession | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    return estonia_ar_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        resources.estonia_ar_source(run_id=run_id, session=session)
    )


@dlt_assets(
    dlt_source=resources.estonia_ar_source(),
    dlt_pipeline=estonia_ar_pipeline(ESTONIA_AR_DUCKDB_PATH),
    name=ENTITIES_ASSET_KEY,
    dagster_dlt_translator=EstoniaArDltTranslator(),
    pool=ESTONIA_AR_DUCKDB_POOL,
)
def estonia_ar_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    """Load Estonia AR register entity bulk data to local DuckDB with dlt."""
    ESTONIA_AR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Estonia AR register dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        resources.REGISTER_DOWNLOAD_URL,
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(context=context)
    row_count = _duckdb_table_count(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
    )
    context.log.info(
        "Completed Estonia AR register dlt load: duckdb_path=%s, dataset=%s, table=%s, rows=%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        row_count,
    )


@dg.asset(
    deps=[dg.AssetKey(ENTITIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_EE_COMPANIES_TABLE},
    description="Estonia AR register companies exported to ClickHouse corpscout.ee_companies.",
)
def estonia_ar_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia AR companies ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_COMPANIES_TABLE,
    )
    rows = export_estonia_ar_clickhouse_companies(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info("Completed Estonia AR companies ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_COMPANIES_TABLE},
    )


def _load_financial_raw(
    context: AssetExecutionContext,
    *,
    raw_table: str,
) -> dg.MaterializeResult:
    # Resolve the current (datestamp-rotated) snapshot URL from the dataset index.
    download_url = resolve_financial_url(raw_table, log=context.log.warning)
    context.log.info(
        "Loading Estonia AR financial CSV: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        raw_table,
    )
    rows = load_estonia_ar_financial_csv(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        download_url=download_url,
        raw_table=raw_table,
    )
    context.log.info(
        "Loaded Estonia AR financial CSV: table=%s.%s, rows=%s",
        DLT_DATASET_NAME,
        raw_table,
        rows,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": f"{DLT_DATASET_NAME}.{raw_table}"}
    )


def _raw_financial_asset(*, asset_key: str, raw_table: str, description: str):
    @dg.asset(
        name=asset_key,
        group_name=GROUP_NAME,
        kinds={"python", "duckdb"},
        pool=ESTONIA_AR_DUCKDB_POOL,
        description=description,
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        return _load_financial_raw(context, raw_table=raw_table)

    return _asset


# 8 raw checkpoints: report-general spine + one Key Indicators (EAV) file per year.
REPORT_GENERAL_ASSET_KEY = "estonia_ar_report_general_raw_duckdb"
estonia_ar_report_general_raw_duckdb = _raw_financial_asset(
    asset_key=REPORT_GENERAL_ASSET_KEY,
    raw_table=tables.REPORT_GENERAL_RAW_TABLE,
    description="Estonia AR annual-report general data (statement spine) loaded raw into DuckDB.",
)

RAW_FINANCIAL_ASSET_KEYS = [REPORT_GENERAL_ASSET_KEY]
for _year in tables.EE_FINANCIAL_YEARS:
    _key = f"estonia_ar_key_indicators_{_year}_raw_duckdb"
    RAW_FINANCIAL_ASSET_KEYS.append(_key)
    # Module-level binding so dg.load_from_defs_folder discovers each generated asset.
    globals()[_key] = _raw_financial_asset(
        asset_key=_key,
        raw_table=tables.key_indicators_raw_table(_year),
        description=f"Estonia AR {_year} annual-report key indicators (EAV) loaded raw into DuckDB.",
    )


@dg.asset(
    name="estonia_ar_financial_statements_duckdb",
    deps=[dg.AssetKey(key) for key in RAW_FINANCIAL_ASSET_KEYS],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description="Estonia AR wide financial statements pivoted from the per-year EAV element tables.",
)
def estonia_ar_financial_statements_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia AR wide financial statements: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
    )
    counts = build_estonia_ar_financial_statements(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("estonia_ar_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE},
    description=(
        "Estonia AR financial statements exported to ClickHouse "
        "corpscout.ee_financial_statements."
    ),
)
def estonia_ar_clickhouse_financial_statements(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia AR financial statements ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE,
    )
    rows = export_estonia_ar_clickhouse_financial_statements(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info(
        "Completed Estonia AR financial statements ClickHouse export: rows=%s", rows
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE},
    )


@dg.asset(
    name="estonia_ar_financial_metrics_duckdb",
    deps=[dg.AssetKey("estonia_ar_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description="Estonia AR headline financial metrics (native EUR) from the wide statements.",
)
def estonia_ar_financial_metrics_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia AR native financial metrics: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    counts = build_estonia_ar_financial_metrics(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="estonia_ar_financial_metrics_usd_duckdb",
    deps=[dg.AssetKey("estonia_ar_financial_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description=(
        "Separate step: fill USD + fx columns on the Estonia metrics table via the "
        "shared exchange-rate client (no-op-safe when exchange_rates is empty)."
    ),
)
def estonia_ar_financial_metrics_usd_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    context.log.info(
        "Applying Estonia AR USD conversion: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    counts = apply_estonia_ar_usd_conversion(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("estonia_ar_financial_metrics_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE},
    description=(
        "Estonia AR financial metrics exported to ClickHouse corpscout.ee_financial_metrics."
    ),
)
def estonia_ar_clickhouse_financial_metrics(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia AR financial metrics ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE,
    )
    rows = export_estonia_ar_clickhouse_financial_metrics(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info("Completed Estonia AR financial metrics ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE},
    )


# --- Company contacts (websites / email / phone — §8b mandatory) -------------
@dg.asset(
    name="estonia_ar_company_contacts_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description=(
        "Estonia AR company contacts (websites/email/phone) extracted from the "
        "yldandmed general-data JSON `sidevahendid` array, normalized one row per contact."
    ),
)
def estonia_ar_company_contacts_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia AR company contacts: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.GENERAL_DATA_RAW_TABLE,
    )
    counts = build_estonia_ar_company_contacts(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("estonia_ar_company_contacts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE},
    description=(
        "Estonia AR company contacts exported to ClickHouse corpscout.ee_company_contacts."
    ),
)
def estonia_ar_clickhouse_company_contacts(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia AR company contacts ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE,
    )
    rows = export_estonia_ar_clickhouse_company_contacts(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info("Completed Estonia AR company contacts ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE},
    )


@dg.asset(
    name="estonia_ar_company_domains_duckdb",
    deps=[dg.AssetKey("estonia_ar_company_contacts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description=(
        "Estonia AR deduped per-company domain feeder (website-derived + unique "
        "email-suffix domains) for the cross-source domain graph."
    ),
)
def estonia_ar_company_domains_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia AR company domains: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.COMPANY_DOMAINS_TABLE,
    )
    counts = build_estonia_ar_company_domains(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("estonia_ar_company_domains_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_EE_COMPANY_DOMAINS_TABLE},
    description=(
        "Estonia AR company domains exported to ClickHouse corpscout.ee_company_domains "
        "(feeds the cross-source company_website_domains graph)."
    ),
)
def estonia_ar_clickhouse_company_domains(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia AR company domains ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_COMPANY_DOMAINS_TABLE,
    )
    rows = export_estonia_ar_clickhouse_company_domains(
        database_path=ESTONIA_AR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info("Completed Estonia AR company domains ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_COMPANY_DOMAINS_TABLE},
    )


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)


# --- Jobs & schedules --------------------------------------------------------
# Cadence-matched, non-partitioned full-refresh. .upstream() pulls the FULL
# transitive chain (unlike the `dg launch +leaf` CLI, which resolves one hop).
# All steps serialize on the estonia_ar_duckdb pool.
estonia_ar_register_job = dg.define_asset_job(
    "estonia_ar_register_job",
    selection=dg.AssetSelection.assets(ENTITIES_ASSET_KEY)
    | dg.AssetSelection.assets("estonia_ar_clickhouse_companies"),
)
estonia_ar_financials_job = dg.define_asset_job(
    "estonia_ar_financials_job",
    selection=dg.AssetSelection.assets(
        "estonia_ar_clickhouse_financial_statements",
        "estonia_ar_clickhouse_financial_metrics",
    ).upstream(),
)

# Register snapshot refreshes daily → daily 04:00. Financials are a monthly
# cumulative snapshot → run early each month (5th, 05:00) once the new datestamp
# has published; resolve_financial_url picks up whatever datestamp is current.
estonia_ar_register_schedule = dg.ScheduleDefinition(
    name="estonia_ar_register_schedule",
    job=estonia_ar_register_job,
    cron_schedule="0 4 * * *",
    execution_timezone="Europe/Belgrade",
)
estonia_ar_financials_schedule = dg.ScheduleDefinition(
    name="estonia_ar_financials_schedule",
    job=estonia_ar_financials_job,
    cron_schedule="0 5 5 * *",
    execution_timezone="Europe/Belgrade",
)

# Contacts come from the ~4.5 GB yldandmed general-data JSON — too heavy for daily.
# Refresh monthly (8th, 06:00), staggered after the financials snapshot run. The
# domain feeder (websites + unique email suffixes) rides the same chain.
estonia_ar_contacts_job = dg.define_asset_job(
    "estonia_ar_contacts_job",
    selection=dg.AssetSelection.assets(
        "estonia_ar_clickhouse_company_contacts",
        "estonia_ar_clickhouse_company_domains",
    ).upstream(),
)
estonia_ar_contacts_schedule = dg.ScheduleDefinition(
    name="estonia_ar_contacts_schedule",
    job=estonia_ar_contacts_job,
    cron_schedule="0 6 8 * *",
    execution_timezone="Europe/Belgrade",
)

# Whole-source trigger: register + financials end-to-end in dependency order
# (all estonia_ar assets, serialized on the pool). For a manual full run or the
# first backfill. NOT scheduled — the two cadence-split jobs above own recurring runs.
estonia_ar_full_refresh_job = dg.define_asset_job(
    "estonia_ar_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)
