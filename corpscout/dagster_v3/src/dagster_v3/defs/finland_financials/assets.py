from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.finland_financials import metrics, tables
from dagster_v3.defs.finland_financials.clickhouse import (
    export_finland_financials_clickhouse_metrics,
)

GROUP_NAME = "finland_financials"
FINLAND_FINANCIALS_DUCKDB_POOL = "finland_financials_duckdb"
FINLAND_FINANCIALS_DUCKDB_PATH = Path("data/finland_financials_source.duckdb")


class FinlandFinancialsConfig(dg.Config):
    # Registration-date window to discover statements (bounded). Defaults to a
    # recent week; widen for more coverage.
    registered_date_start: str = "2025-01-01"
    registered_date_end: str = "2025-01-07"
    max_reports: int = 200


@dg.asset(
    name="finland_financials_metrics_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FINLAND_FINANCIALS_DUCKDB_POOL,
    description=(
        "Finland PRH financial metrics parsed from XBRL statements via the shared "
        "xbrl_common parser (plain dimensional XBRL → concept+member metric map). "
        "Replaces the Arelle path."
    ),
)
def finland_financials_metrics_duckdb(
    context: AssetExecutionContext,
    config: FinlandFinancialsConfig,
) -> dg.MaterializeResult:
    counts = metrics.build_finland_financials(
        database_path=FINLAND_FINANCIALS_DUCKDB_PATH,
        source_run_id=context.run_id,
        registered_date_start=config.registered_date_start,
        registered_date_end=config.registered_date_end,
        max_reports=config.max_reports,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="finland_financials_metrics_usd_duckdb",
    deps=[dg.AssetKey("finland_financials_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FINLAND_FINANCIALS_DUCKDB_POOL,
    description="Separate step: fill USD + fx columns (EUR→USD) via the shared exchange-rate client.",
)
def finland_financials_metrics_usd_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    counts = metrics.apply_finland_usd_conversion(
        database_path=FINLAND_FINANCIALS_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("finland_financials_metrics_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FINLAND_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description="Finland financial metrics appended to ClickHouse corpscout.fi_financial_metrics.",
)
def finland_financials_clickhouse_metrics(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_finland_financials_clickhouse_metrics(
        database_path=FINLAND_FINANCIALS_DUCKDB_PATH,
        clickhouse=clickhouse,
        truncate=False,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_METRICS_TABLE},
    )


finland_financials_job = dg.define_asset_job(
    "finland_financials_job",
    selection=dg.AssetSelection.assets("finland_financials_clickhouse_metrics").upstream(),
)
