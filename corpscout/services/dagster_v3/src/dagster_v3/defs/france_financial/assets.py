from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.france_financial import tables
from dagster_v3.defs.france_financial.clickhouse import (
    export_france_financial_metrics_clickhouse,
)
from dagster_v3.defs.france_financial.metrics import (
    apply_france_financial_usd_conversion,
)
from dagster_v3.defs.france_financial.records import (
    build_france_financial_metrics,
    load_france_financial_parquet,
)

GROUP_NAME = "france_financial"
DUCKDB_POOL = "france_financial_duckdb"
DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME


@dg.asset(
    name="france_financial_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb"},
    pool=DUCKDB_POOL,
    metadata={"table": f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"},
    description=("Full BCE/INPI financial-ratios Parquet export staged in DuckDB."),
)
def france_financial_raw_duckdb(
    context: dg.AssetExecutionContext,
    france_financial_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with france_financial_duckdb.get_connection() as connection:
        rows = load_france_financial_parquet(
            duckdb_connection=connection,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": tables.PARQUET_EXPORT_URL,
            "table": f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}",
        }
    )


@dg.asset(
    name="france_financial_metrics_duckdb",
    deps=[dg.AssetKey("france_financial_raw_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description="Typed France financial metrics and ratios in native EUR.",
)
def france_financial_metrics_duckdb(
    context: dg.AssetExecutionContext,
    france_financial_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with france_financial_duckdb.get_connection() as connection:
        counts = build_france_financial_metrics(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="france_financial_metrics_usd_duckdb",
    deps=[dg.AssetKey("france_financial_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=DUCKDB_POOL,
    description="Adds USD companions and FX provenance to France financial metrics.",
)
def france_financial_metrics_usd_duckdb(
    context: dg.AssetExecutionContext,
    france_financial_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with france_financial_duckdb.get_connection() as connection:
        counts = apply_france_financial_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="france_financial_metrics_clickhouse",
    deps=[dg.AssetKey("france_financial_metrics_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE},
    description="France BCE/INPI financial metrics exported to ClickHouse.",
)
def france_financial_metrics_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    france_financial_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(france_financial_duckdb) as connection:
        rows = export_france_financial_metrics_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_FINANCIAL_METRICS_TABLE}
    )


france_financial_job = dg.define_asset_job(
    "france_financial_job",
    selection=dg.AssetSelection.assets(
        "france_financial_metrics_clickhouse"
    ).upstream(),
)
france_financial_schedule = dg.ScheduleDefinition(
    name="france_financial_schedule",
    job=france_financial_job,
    cron_schedule="10 7 12 * *",
    execution_timezone="Europe/Belgrade",
)

defs = dg.Definitions(
    assets=[
        france_financial_raw_duckdb,
        france_financial_metrics_duckdb,
        france_financial_metrics_usd_duckdb,
        france_financial_metrics_clickhouse,
    ],
    jobs=[france_financial_job],
    schedules=[france_financial_schedule],
    resources={
        "france_financial_duckdb": duckdb_resource(DUCKDB_PATH),
    },
)
