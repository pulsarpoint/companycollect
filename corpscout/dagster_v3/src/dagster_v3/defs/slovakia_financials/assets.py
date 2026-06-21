from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.slovakia_financials import incremental, metrics, tables
from dagster_v3.defs.slovakia_financials.clickhouse import (
    export_slovakia_financials_clickhouse_metrics,
)

GROUP_NAME = "slovakia_financials"
SLOVAKIA_FINANCIALS_DUCKDB_POOL = "slovakia_financials_duckdb"
SLOVAKIA_FINANCIALS_DUCKDB_PATH = Path("data/slovakia_financials_source.duckdb")


class SlovakiaFinancialsConfig(dg.Config):
    # Statements processed per run (each is ~3-4 RÚZ API calls). The id cursor
    # makes runs resumable; raise this / run repeatedly to sweep history faster.
    max_statements: int = 2000


@dg.asset(
    name="slovakia_financials_incremental",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description=(
        "Forward sweep of RÚZ financial statements after the id cursor: fetch "
        "statement + reports, decode the statutory tables to canonical metrics, "
        "EUR→USD, APPEND to sk_financial_metrics, then advance the id cursor. "
        "Walks all history in bounded chunks and then picks up new filings."
    ),
)
def slovakia_financials_incremental(
    context: AssetExecutionContext,
    config: SlovakiaFinancialsConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    after_id = incremental.read_cursor(SLOVAKIA_FINANCIALS_DUCKDB_PATH)
    context.log.info("Slovak RÚZ sweep starting after statement id=%s", after_id)
    counts = metrics.build_slovakia_financials(
        database_path=SLOVAKIA_FINANCIALS_DUCKDB_PATH,
        source_run_id=context.run_id,
        after_id=after_id,
        max_statements=config.max_statements,
        log=context.log.info,
    )
    if counts.get("statements", 0) > 0:
        from exchange_rates import ExchangeRateClient

        metrics.apply_slovakia_usd_conversion(
            database_path=SLOVAKIA_FINANCIALS_DUCKDB_PATH,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
        counts["exported_rows"] = export_slovakia_financials_clickhouse_metrics(
            database_path=SLOVAKIA_FINANCIALS_DUCKDB_PATH,
            clickhouse=clickhouse,
            truncate=False,
            log=context.log.info,
        )
    last_id = counts.get("last_id", after_id)
    if last_id > after_id:
        incremental.write_cursor(SLOVAKIA_FINANCIALS_DUCKDB_PATH, last_id)
    return dg.MaterializeResult(metadata={**counts, "after_id": after_id})


# --- Jobs & schedules --------------------------------------------------------
slovakia_financials_incremental_job = dg.define_asset_job(
    "slovakia_financials_incremental_job",
    selection=dg.AssetSelection.assets("slovakia_financials_incremental"),
)
slovakia_financials_incremental_schedule = dg.ScheduleDefinition(
    name="slovakia_financials_incremental_schedule",
    job=slovakia_financials_incremental_job,
    cron_schedule="0 5 * * *",  # daily 05:00
    execution_timezone="Europe/Belgrade",
)
