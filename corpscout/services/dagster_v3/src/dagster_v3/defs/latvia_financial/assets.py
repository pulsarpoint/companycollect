from collections.abc import Iterator
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import read_only_duckdb_connection
from dagster_v3.defs.latvia_financial.clickhouse import (
    export_latvia_financial_metrics_clickhouse,
    export_latvia_financial_statements_clickhouse,
)
from dagster_v3.defs.latvia_financial.financials import (
    build_latvia_financial_statements,
    load_latvia_financial_csv,
)
from dagster_v3.defs.latvia_financial.metrics import (
    apply_latvia_financial_usd_conversion,
    build_latvia_financial_metrics,
)
from dagster_v3.defs.latvia_ur import tables

GROUP_NAME = "latvia_financial"
LATVIA_UR_DUCKDB_POOL = "latvia_ur_duckdb"
LATVIA_UR_DUCKDB_PATH = Path("data/latvia_ur_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME

FINANCIAL_STATEMENTS_RAW_ASSET = "latvia_financial_statements_raw_duckdb"
BALANCE_SHEETS_RAW_ASSET = "latvia_balance_sheets_raw_duckdb"
INCOME_STATEMENTS_RAW_ASSET = "latvia_income_statements_raw_duckdb"
CASH_FLOW_STATEMENTS_RAW_ASSET = "latvia_cash_flow_statements_raw_duckdb"

RAW_TABLE_ASSET_NAMES = {
    tables.FINANCIAL_STATEMENTS_RAW_TABLE: FINANCIAL_STATEMENTS_RAW_ASSET,
    tables.BALANCE_SHEETS_RAW_TABLE: BALANCE_SHEETS_RAW_ASSET,
    tables.INCOME_STATEMENTS_RAW_TABLE: INCOME_STATEMENTS_RAW_ASSET,
    tables.CASH_FLOW_STATEMENTS_RAW_TABLE: CASH_FLOW_STATEMENTS_RAW_ASSET,
}
RAW_FINANCIAL_ASSET_KEYS = tuple(
    dg.AssetKey(asset_name) for asset_name in RAW_TABLE_ASSET_NAMES.values()
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            asset_name,
            group_name=GROUP_NAME,
            kinds={"python", "csv", "duckdb"},
            metadata={"table": f"{DLT_DATASET_NAME}.{raw_table}"},
            description=(
                "Latvia financial CSV raw DuckDB staging table. This table is one "
                "output of the shared full-download financial CSV multi-asset."
            ),
        )
        for raw_table, asset_name in RAW_TABLE_ASSET_NAMES.items()
    ],
    pool=LATVIA_UR_DUCKDB_POOL,
)
def latvia_financial_raw_duckdb(
    context: dg.AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    """Download all four Latvia financial CSV files and replace their raw DuckDB tables."""
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Latvia financial raw CSV full refresh: duckdb_path=%s, files=%d",
        LATVIA_UR_DUCKDB_PATH,
        len(tables.FINANCIAL_RAW_SOURCES),
    )
    with latvia_ur_duckdb.get_connection() as connection:
        for raw_table, download_url in tables.FINANCIAL_RAW_SOURCES.items():
            asset_name = RAW_TABLE_ASSET_NAMES[raw_table]
            context.log.info(
                "Loading Latvia financial CSV: asset=%s url=%s table=%s.%s",
                asset_name,
                download_url,
                DLT_DATASET_NAME,
                raw_table,
            )
            rows = load_latvia_financial_csv(
                duckdb_connection=connection,
                download_url=download_url,
                raw_table=raw_table,
            )
            context.log.info(
                "Loaded Latvia financial CSV: asset=%s table=%s.%s rows=%s",
                asset_name,
                DLT_DATASET_NAME,
                raw_table,
                rows,
            )
            yield dg.MaterializeResult(
                asset_key=asset_name,
                metadata={
                    "rows": rows,
                    "download_url": download_url,
                    "duckdb_path": str(LATVIA_UR_DUCKDB_PATH),
                    "table": f"{DLT_DATASET_NAME}.{raw_table}",
                },
            )
    context.log.info("Completed Latvia financial raw CSV full refresh")


@dg.asset(
    name="latvia_financial_statements_duckdb",
    deps=RAW_FINANCIAL_ASSET_KEYS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description=(
        "Builds the wide Latvia financial statements DuckDB table from the four raw "
        "financial CSV staging tables."
    ),
)
def latvia_financial_statements_duckdb(
    context: dg.AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Latvia wide financial statements: duckdb_path=%s table=%s.%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
    )
    with latvia_ur_duckdb.get_connection() as connection:
        counts = build_latvia_financial_statements(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "table": f"{DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_WIDE_TABLE}",
            **counts,
        }
    )


@dg.asset(
    name="latvia_financial_statements_clickhouse",
    deps=[dg.AssetKey("latvia_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE},
    description="Latvia financial statements exported to ClickHouse corpscout.lv_financial_statements.",
)
def latvia_financial_statements_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia financial statements ClickHouse export: duckdb_path=%s table=%s",
        LATVIA_UR_DUCKDB_PATH,
        tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE,
    )
    with read_only_duckdb_connection(latvia_ur_duckdb) as connection:
        rows = export_latvia_financial_statements_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info(
        "Completed Latvia financial statements ClickHouse export: rows=%s", rows
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE},
    )


@dg.asset(
    name="latvia_financial_metrics_duckdb",
    deps=[dg.AssetKey("latvia_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia headline financial metrics in native currency from the wide statements table.",
)
def latvia_financial_metrics_duckdb(
    context: dg.AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Latvia native financial metrics: duckdb_path=%s table=%s.%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with latvia_ur_duckdb.get_connection() as connection:
        counts = build_latvia_financial_metrics(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="latvia_financial_metrics_usd_duckdb",
    deps=[dg.AssetKey("latvia_financial_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Adds USD and FX metadata columns to Latvia financial metrics in DuckDB.",
)
def latvia_financial_metrics_usd_duckdb(
    context: dg.AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    context.log.info(
        "Applying Latvia financial USD conversion: duckdb_path=%s table=%s.%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_METRICS_WIDE_TABLE,
    )
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with latvia_ur_duckdb.get_connection() as connection:
        counts = apply_latvia_financial_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="latvia_financial_metrics_clickhouse",
    deps=[dg.AssetKey("latvia_financial_metrics_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE},
    description="Latvia financial metrics exported to ClickHouse corpscout.lv_financial_metrics.",
)
def latvia_financial_metrics_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia financial metrics ClickHouse export: duckdb_path=%s table=%s",
        LATVIA_UR_DUCKDB_PATH,
        tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE,
    )
    with read_only_duckdb_connection(latvia_ur_duckdb) as connection:
        rows = export_latvia_financial_metrics_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info("Completed Latvia financial metrics ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE},
    )


latvia_financials_job = dg.define_asset_job(
    "latvia_financials_job",
    selection=dg.AssetSelection.assets(
        "latvia_financial_statements_clickhouse",
        "latvia_financial_metrics_clickhouse",
    ).upstream(),
)
latvia_financials_schedule = dg.ScheduleDefinition(
    name="latvia_financials_schedule",
    job=latvia_financials_job,
    cron_schedule="20 5 * * 1",  # weekly Monday 05:20; source publishes full CSV snapshots.
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[
        latvia_financial_raw_duckdb,
        latvia_financial_statements_duckdb,
        latvia_financial_statements_clickhouse,
        latvia_financial_metrics_duckdb,
        latvia_financial_metrics_usd_duckdb,
        latvia_financial_metrics_clickhouse,
    ],
    jobs=[latvia_financials_job],
    schedules=[latvia_financials_schedule],
)
