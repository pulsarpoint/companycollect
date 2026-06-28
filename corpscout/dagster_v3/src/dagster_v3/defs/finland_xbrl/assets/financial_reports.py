import time
from collections.abc import Callable

import dagster as dg
from pydantic import field_validator

from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    FINLAND_XBRL_DUCKDB_POOL,
    _registration_window,
)
from dagster_v3.defs.finland_xbrl.resources import (
    XbrlApiResource,
    XbrlParquetStorageResource,
)


class XbrlFinancialReportsConfig(dg.Config):
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_non_negative_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("delay values must be zero or greater")
        return value


def materialize_financial_reports_window(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    parquet_storage: XbrlParquetStorageResource,
    xbrl_api: XbrlApiResource,
    *,
    registered_date_start: str,
    registered_date_end: str,
    run_id: str,
    write_financial_reports: Callable[[str, list[dict]], object],
) -> dg.MaterializeResult:
    context.log.info(
        "XBRL financial reports partition %s: loading reports registered %s..%s",
        context.partition_key,
        registered_date_start,
        registered_date_end,
    )
    rows = list(
        xbrl_api.iter_financial_report_rows(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            request_delay_seconds=config.request_delay_seconds,
            run_id=run_id,
            sleep=time.sleep,
            log_info=context.log.info,
        )
    )
    parquet_path = write_financial_reports(context.partition_key, rows)
    context.log.info(
        "XBRL financial reports partition %s complete: registered %s..%s rows=%d parquet_path=%s",
        context.partition_key,
        registered_date_start,
        registered_date_end,
        len(rows),
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "fetched_count": len(rows),
            "row_count": len(rows),
            "parquet_path": str(parquet_path),
        }
    )


@dg.asset(
    name="finland_xbrl_financial_reports_backfill_duckdb",
    group_name="finland_xbrl",
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "parquet"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Monthly backfill writer for PRH XBRL financial report listings by registration date.",
)
def finland_xbrl_financial_reports_backfill_duckdb(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    xbrl_api: XbrlApiResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return materialize_financial_reports_window(
        context,
        config,
        xbrl_parquet_storage,
        xbrl_api,
        registered_date_start=start,
        registered_date_end=end,
        run_id=context.run.run_id,
        write_financial_reports=xbrl_parquet_storage.write_financial_reports_backfill,
    )


@dg.asset(
    name="finland_xbrl_financial_reports_incremental_duckdb",
    group_name="finland_xbrl",
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "parquet"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Daily incremental writer for PRH XBRL financial report listings by registration date.",
)
def finland_xbrl_financial_reports_incremental_duckdb(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    xbrl_api: XbrlApiResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return materialize_financial_reports_window(
        context,
        config,
        xbrl_parquet_storage,
        xbrl_api,
        registered_date_start=start,
        registered_date_end=end,
        run_id=context.run.run_id,
        write_financial_reports=xbrl_parquet_storage.write_financial_reports_incremental,
    )


@dg.asset(
    name="finland_xbrl_financial_reports_duckdb",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_backfill_duckdb,
        finland_xbrl_financial_reports_incremental_duckdb,
    ],
    kinds={"parquet"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Catalog marker for the shared PRH XBRL financial report listing DuckDB table.",
)
def finland_xbrl_financial_reports_duckdb(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info("Loading Finland XBRL financial reports parquet marker")
    backfill_row_count = xbrl_parquet_storage.financial_reports_backfill_row_count()
    incremental_row_count = xbrl_parquet_storage.financial_reports_incremental_row_count()
    row_count = backfill_row_count + incremental_row_count
    context.log.info(
        "Finland XBRL financial reports parquet row_count=%d backfill_row_count=%d incremental_row_count=%d",
        row_count,
        backfill_row_count,
        incremental_row_count,
    )
    return dg.MaterializeResult(
        metadata={
            "backfill_row_count": backfill_row_count,
            "incremental_row_count": incremental_row_count,
            "row_count": row_count,
        }
    )
