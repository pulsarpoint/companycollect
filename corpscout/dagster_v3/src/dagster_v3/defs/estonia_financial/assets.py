from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import read_only_duckdb_connection
from dagster_v3.defs.estonia_ar import tables
from dagster_v3.defs.estonia_financial.clickhouse import (
    export_estonia_financial_metrics_clickhouse,
    export_estonia_financial_statements_clickhouse,
)
from dagster_v3.defs.estonia_financial.financials import (
    build_estonia_ar_financial_statements,
    load_estonia_ar_financial_csv,
)
from dagster_v3.defs.estonia_financial.metrics import (
    apply_estonia_ar_usd_conversion,
    build_estonia_ar_financial_metrics,
)
from dagster_v3.defs.estonia_financial.resources import EstoniaFinancialResource

GROUP_NAME = "estonia_financial"
ESTONIA_AR_DUCKDB_POOL = "estonia_ar_duckdb"
ESTONIA_AR_DUCKDB_PATH = Path("data/estonia_ar_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME

REPORT_GENERAL_ASSET_KEY = "estonia_ar_report_general_raw_duckdb"
RAW_FINANCIAL_ASSET_KEYS = [REPORT_GENERAL_ASSET_KEY]


def _load_financial_raw(
    context: dg.AssetExecutionContext,
    *,
    estonia_ar_duckdb: DuckDBResource,
    estonia_financial: EstoniaFinancialResource,
    raw_table: str,
) -> dg.MaterializeResult:
    download_url = estonia_financial.resolve_financial_url(
        raw_table,
        log=context.log.warning,
    )
    context.log.info(
        "Loading Estonia financial CSV: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        raw_table,
    )
    ESTONIA_AR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with estonia_ar_duckdb.get_connection() as connection:
        rows = load_estonia_ar_financial_csv(
            duckdb_connection=connection,
            download_url=download_url,
            raw_table=raw_table,
            financial_resource=estonia_financial,
        )
    context.log.info(
        "Loaded Estonia financial CSV: table=%s.%s, rows=%s",
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
    def _asset(
        context: dg.AssetExecutionContext,
        estonia_ar_duckdb: DuckDBResource,
        estonia_financial: EstoniaFinancialResource,
    ) -> dg.MaterializeResult:
        return _load_financial_raw(
            context,
            estonia_ar_duckdb=estonia_ar_duckdb,
            estonia_financial=estonia_financial,
            raw_table=raw_table,
        )

    return _asset


estonia_ar_report_general_raw_duckdb = _raw_financial_asset(
    asset_key=REPORT_GENERAL_ASSET_KEY,
    raw_table=tables.REPORT_GENERAL_RAW_TABLE,
    description="Estonia annual-report general data loaded raw into DuckDB.",
)

for _year in tables.EE_FINANCIAL_YEARS:
    _key = f"estonia_ar_key_indicators_{_year}_raw_duckdb"
    RAW_FINANCIAL_ASSET_KEYS.append(_key)
    globals()[_key] = _raw_financial_asset(
        asset_key=_key,
        raw_table=tables.key_indicators_raw_table(_year),
        description=f"Estonia {_year} annual-report key indicators loaded raw into DuckDB.",
    )


@dg.asset(
    name="estonia_ar_financial_statements_duckdb",
    deps=[dg.AssetKey(key) for key in RAW_FINANCIAL_ASSET_KEYS],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description="Estonia wide financial statements pivoted from the per-year EAV element tables.",
)
def estonia_ar_financial_statements_duckdb(
    context: dg.AssetExecutionContext,
    estonia_ar_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia wide financial statements: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
    )
    ESTONIA_AR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with estonia_ar_duckdb.get_connection() as connection:
        counts = build_estonia_ar_financial_statements(
            duckdb_connection=connection,
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
    description="Estonia financial statements exported to ClickHouse corpscout.ee_financial_statements.",
)
def estonia_ar_clickhouse_financial_statements(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    estonia_ar_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia financial statements ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE,
    )
    with read_only_duckdb_connection(estonia_ar_duckdb) as connection:
        rows = export_estonia_financial_statements_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info("Completed Estonia financial statements ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE},
    )


@dg.asset(
    name="estonia_ar_financial_metrics_duckdb",
    deps=[dg.AssetKey("estonia_ar_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=ESTONIA_AR_DUCKDB_POOL,
    description="Estonia headline financial metrics in native EUR from the wide statements.",
)
def estonia_ar_financial_metrics_duckdb(
    context: dg.AssetExecutionContext,
    estonia_ar_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Estonia native financial metrics: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    ESTONIA_AR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with estonia_ar_duckdb.get_connection() as connection:
        counts = build_estonia_ar_financial_metrics(
            duckdb_connection=connection,
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
    description="Adds USD and FX metadata columns to Estonia financial metrics in DuckDB.",
)
def estonia_ar_financial_metrics_usd_duckdb(
    context: dg.AssetExecutionContext,
    estonia_ar_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    context.log.info(
        "Applying Estonia financial USD conversion: duckdb_path=%s, table=%s.%s",
        ESTONIA_AR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    ESTONIA_AR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with estonia_ar_duckdb.get_connection() as connection:
        counts = apply_estonia_ar_usd_conversion(
            duckdb_connection=connection,
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
    description="Estonia financial metrics exported to ClickHouse corpscout.ee_financial_metrics.",
)
def estonia_ar_clickhouse_financial_metrics(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    estonia_ar_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Estonia financial metrics ClickHouse export: duckdb_path=%s, table=%s",
        ESTONIA_AR_DUCKDB_PATH,
        tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE,
    )
    with read_only_duckdb_connection(estonia_ar_duckdb) as connection:
        rows = export_estonia_financial_metrics_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info("Completed Estonia financial metrics ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE},
    )


estonia_financials_job = dg.define_asset_job(
    "estonia_financials_job",
    selection=dg.AssetSelection.assets(
        "estonia_ar_clickhouse_financial_statements",
        "estonia_ar_clickhouse_financial_metrics",
    ).upstream(),
)
estonia_financials_schedule = dg.ScheduleDefinition(
    name="estonia_financials_schedule",
    job=estonia_financials_job,
    cron_schedule="0 5 5 * *",
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[
        estonia_ar_report_general_raw_duckdb,
        *(globals()[key] for key in RAW_FINANCIAL_ASSET_KEYS if key != REPORT_GENERAL_ASSET_KEY),
        estonia_ar_financial_statements_duckdb,
        estonia_ar_clickhouse_financial_statements,
        estonia_ar_financial_metrics_duckdb,
        estonia_ar_financial_metrics_usd_duckdb,
        estonia_ar_clickhouse_financial_metrics,
    ],
    jobs=[estonia_financials_job],
    schedules=[estonia_financials_schedule],
    resources={"estonia_financial": EstoniaFinancialResource()},
)
