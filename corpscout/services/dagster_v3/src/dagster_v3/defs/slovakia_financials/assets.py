from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.slovakia_financials import download, incremental, metrics, raw_store, tables
from dagster_v3.defs.slovakia_financials.clickhouse import (
    export_slovakia_financials_clickhouse_metrics,
)

GROUP_NAME = "slovakia_financials"
SLOVAKIA_FINANCIALS_DUCKDB_POOL = "slovakia_financials_duckdb"
SLOVAKIA_FINANCIALS_DUCKDB_PATH = Path("data/slovakia_financials_source.duckdb")


class SlovakiaFinancialsConfig(dg.Config):
    # Statements downloaded per run (each is ~3-4 RÚZ API calls). The id cursor
    # makes runs resumable; raise this / run repeatedly to sweep history faster.
    max_statements: int = 2000


@dg.asset(
    name="slovakia_financials_raw_statements_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    description=(
        "Forward sweep of RÚZ financial statements after the id cursor: fetch "
        "statement + entity + reports RAW and store one NDJSON batch in S3 "
        "(templates deduplicated under a shared prefix), then advance the "
        "cursor. Walks all history in bounded chunks, then picks up new filings."
    ),
)
def slovakia_financials_raw_statements_s3(
    context: AssetExecutionContext,
    config: SlovakiaFinancialsConfig,
    object_store: ObjectStoreResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(raw_store.RAW_BUCKET)
    with slovakia_financials_duckdb.get_connection() as connection:
        after_id = incremental.read_cursor(connection)
    context.log.info("Slovak RÚZ sweep starting after statement id=%s", after_id)
    counts = download.sweep_statements_to_s3(
        object_store=object_store,
        source_run_id=context.run_id,
        after_id=after_id,
        max_statements=config.max_statements,
        log=context.log.info,
    )
    last_id = counts["last_id"]
    if last_id > after_id:
        with slovakia_financials_duckdb.get_connection() as connection:
            incremental.write_cursor(connection, last_id)
    return dg.MaterializeResult(
        metadata={
            **{key: value for key, value in counts.items() if key != "batch_key"},
            "batch_key": counts["batch_key"] or "",
            "s3_bucket": raw_store.RAW_BUCKET,
            "after_id": after_id,
        }
    )


@dg.asset(
    name="slovakia_financials_metrics_duckdb",
    deps=["slovakia_financials_raw_statements_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    metadata={"table": f"{tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"},
    description=(
        "Decodes not-yet-processed raw S3 statement batches (statutory tables "
        "→ canonical metrics) and APPENDS them to the accumulating DuckDB "
        "metrics table; per-batch delete+insert keeps reprocessing idempotent."
    ),
)
def slovakia_financials_metrics_duckdb(
    context: AssetExecutionContext,
    object_store: ObjectStoreResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with slovakia_financials_duckdb.get_connection() as connection:
        counts = metrics.build_metrics_from_batches(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="slovakia_financials_usd_duckdb",
    deps=["slovakia_financials_metrics_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    description=(
        "Fills *_amount_usd + fx_* columns on the DuckDB metrics table via the "
        "shared ExchangeRateClient (EUR→USD at each statement's period_end). "
        "Set-based and re-runnable in isolation."
    ),
)
def slovakia_financials_usd_duckdb(
    context: AssetExecutionContext,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with slovakia_financials_duckdb.get_connection() as connection:
        counts = metrics.apply_slovakia_usd_conversion(
            connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="slovakia_financials_metrics_clickhouse",
    deps=["slovakia_financials_usd_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=SLOVAKIA_FINANCIALS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_METRICS_TABLE},
    description=(
        "Atomically replaces corpscout.sk_financial_metrics from the full "
        "accumulated DuckDB metrics table (stage + EXCHANGE TABLES)."
    ),
)
def slovakia_financials_metrics_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    slovakia_financials_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with slovakia_financials_duckdb.get_connection() as connection:
        rows = export_slovakia_financials_clickhouse_metrics(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"row_count": rows, "clickhouse_table": tables.QUALIFIED_METRICS_TABLE}
    )


# --- Jobs & schedules --------------------------------------------------------
SLOVAKIA_FINANCIALS_ASSETS = [
    slovakia_financials_raw_statements_s3,
    slovakia_financials_metrics_duckdb,
    slovakia_financials_usd_duckdb,
    slovakia_financials_metrics_clickhouse,
]

slovakia_financials_incremental_job = dg.define_asset_job(
    "slovakia_financials_incremental_job",
    selection=dg.AssetSelection.assets(
        "slovakia_financials_raw_statements_s3",
        "slovakia_financials_metrics_duckdb",
        "slovakia_financials_usd_duckdb",
        "slovakia_financials_metrics_clickhouse",
    ),
)
slovakia_financials_incremental_schedule = dg.ScheduleDefinition(
    name="slovakia_financials_incremental_schedule",
    job=slovakia_financials_incremental_job,
    cron_schedule="0 5 * * *",  # daily 05:00
    execution_timezone="Europe/Belgrade",
)

defs = dg.Definitions(
    assets=SLOVAKIA_FINANCIALS_ASSETS,
    jobs=[slovakia_financials_incremental_job],
    schedules=[slovakia_financials_incremental_schedule],
    resources={
        "slovakia_financials_duckdb": duckdb_resource(SLOVAKIA_FINANCIALS_DUCKDB_PATH),
    },
)
