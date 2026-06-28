from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.finland_financials import incremental, metrics, tables
from dagster_v3.defs.finland_financials.clickhouse import (
    export_finland_financials_clickhouse_metrics,
)

GROUP_NAME = "finland_financials"
FINLAND_FINANCIALS_DUCKDB_POOL = "finland_financials_duckdb"
FINLAND_FINANCIALS_DUCKDB_PATH = Path("data/finland_financials_source.duckdb")
BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2025-06-01", end_date="2026-06-01"
)
DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
    hour_offset=6,
    timezone="Europe/Belgrade",
)


def _ingest_window(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    finland_financials_duckdb: DuckDBResource,
    *,
    start: str,
    end: str,
) -> dict:
    """Discover+parse the registration-date window, EUR→USD, append to ClickHouse."""
    with finland_financials_duckdb.get_connection() as connection:
        counts = metrics.build_finland_financials(
            connection=connection,
            source_run_id=context.run_id,
            registered_date_start=start,
            registered_date_end=end,
            log=context.log.info,
        )
        if counts.get("statements", 0) > 0:
            from exchange_rates import ExchangeRateClient

            metrics.apply_finland_usd_conversion(
                connection=connection,
                exchange_rates=ExchangeRateClient.from_env(),
                log=context.log.info,
            )
            counts["exported_rows"] = export_finland_financials_clickhouse_metrics(
                duckdb_connection=connection,
                clickhouse=clickhouse,
                truncate=False,
                log=context.log.info,
            )
    return counts


@dg.asset(
    name="finland_financials_incremental",
    partitions_def=DAILY_PARTITIONS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FINLAND_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description=(
        "Forward-only: fetch statements registered in the daily PRH registration-date "
        "partition, parse via xbrl_common, EUR→USD, APPEND to fi_financial_metrics."
    ),
)
def finland_financials_incremental(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    finland_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = incremental.day_window(context.partition_key)
    context.log.info(
        "Finland incremental registration day: %s..%s",
        start,
        end,
    )
    counts = _ingest_window(
        context,
        clickhouse,
        finland_financials_duckdb,
        start=start,
        end=end,
    )
    return dg.MaterializeResult(metadata={**counts, "partition": context.partition_key})


@dg.asset(
    name="finland_financials_backfill",
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FINLAND_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description=(
        "Historical backfill: one registration-month per partition → APPEND to "
        "fi_financial_metrics. Launch a Dagster backfill over the range; multi_run(1) "
        "+ the pool walk it one bounded month-run at a time."
    ),
)
def finland_financials_backfill(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    finland_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = incremental.month_window(context.partition_key)
    context.log.info("Finland backfill registration month: %s..%s", start, end)
    counts = _ingest_window(
        context,
        clickhouse,
        finland_financials_duckdb,
        start=start,
        end=end,
    )
    return dg.MaterializeResult(metadata={**counts, "partition": context.partition_key})


# --- Jobs & schedules --------------------------------------------------------
finland_financials_incremental_job = dg.define_asset_job(
    "finland_financials_incremental_job",
    selection=dg.AssetSelection.assets("finland_financials_incremental"),
    partitions_def=DAILY_PARTITIONS,
)

finland_financials_incremental_schedule = dg.build_schedule_from_partitioned_job(
    finland_financials_incremental_job,
    name="finland_financials_incremental_schedule",
)
# Backfill job (launch partition-range backfills from the UI/daemon).
finland_financials_backfill_job = dg.define_asset_job(
    "finland_financials_backfill_job",
    selection=dg.AssetSelection.assets("finland_financials_backfill"),
    partitions_def=BACKFILL_PARTITIONS,
)

defs = dg.Definitions(
    assets=[finland_financials_incremental, finland_financials_backfill],
    jobs=[finland_financials_incremental_job, finland_financials_backfill_job],
    schedules=[finland_financials_incremental_schedule],
    resources={
        "finland_financials_duckdb": duckdb_resource(FINLAND_FINANCIALS_DUCKDB_PATH),
    },
)
