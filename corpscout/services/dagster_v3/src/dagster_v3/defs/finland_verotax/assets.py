from collections.abc import Iterator
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.finland_verotax import tables
from dagster_v3.defs.finland_verotax.clickhouse import (
    export_finland_verotax_records_clickhouse,
)
from dagster_v3.defs.finland_verotax.metrics import (
    apply_finland_verotax_usd_conversion,
)
from dagster_v3.defs.finland_verotax.records import (
    build_finland_verotax_tax_records,
    load_finland_verotax_csv,
)
from dagster_v3.defs.finland_verotax.resources import resolve_year_sources

GROUP_NAME = "finland_verotax"
FINLAND_VEROTAX_DUCKDB_POOL = "finland_verotax_duckdb"
FINLAND_VEROTAX_DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def _raw_asset_name(year: int) -> str:
    return f"finland_verotax_raw_{year}_duckdb"


RAW_YEAR_ASSET_NAMES = {
    year: _raw_asset_name(year) for year in tables.EXPECTED_YEARS
}
RAW_YEAR_ASSET_KEYS = tuple(
    dg.AssetKey(asset_name) for asset_name in RAW_YEAR_ASSET_NAMES.values()
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            asset_name,
            group_name=GROUP_NAME,
            kinds={"python", "csv", "duckdb"},
            metadata={"table": f"{DLT_DATASET_NAME}.{tables.raw_table_for_year(year)}"},
            description=(
                f"Verohallinto public corporate income tax CSV for tax year {year}, "
                "staged raw in DuckDB. One output of the shared full-download "
                "multi-asset; the year->URL map is resolved from the vero.fi "
                "open-data page at runtime."
            ),
        )
        for year, asset_name in RAW_YEAR_ASSET_NAMES.items()
    ],
    pool=FINLAND_VEROTAX_DUCKDB_POOL,
)
def finland_verotax_raw_duckdb(
    context: dg.AssetExecutionContext,
    finland_verotax_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    """Download every expected tax-year CSV and replace its raw DuckDB table."""
    FINLAND_VEROTAX_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    year_sources = resolve_year_sources(log=context.log.info)
    context.log.info(
        "Starting Finland verotax raw CSV full refresh: duckdb_path=%s years=%s",
        FINLAND_VEROTAX_DUCKDB_PATH,
        sorted(year_sources),
    )
    with finland_verotax_duckdb.get_connection() as connection:
        for year in tables.EXPECTED_YEARS:
            raw_table = tables.raw_table_for_year(year)
            download_url = year_sources[year]
            context.log.info(
                "Loading Finland verotax CSV: year=%s url=%s table=%s.%s",
                year,
                download_url,
                DLT_DATASET_NAME,
                raw_table,
            )
            rows = load_finland_verotax_csv(
                duckdb_connection=connection,
                download_url=download_url,
                raw_table=raw_table,
            )
            context.log.info(
                "Loaded Finland verotax CSV: year=%s rows=%s", year, rows
            )
            yield dg.MaterializeResult(
                asset_key=RAW_YEAR_ASSET_NAMES[year],
                metadata={
                    "rows": rows,
                    "download_url": download_url,
                    "duckdb_path": str(FINLAND_VEROTAX_DUCKDB_PATH),
                    "table": f"{DLT_DATASET_NAME}.{raw_table}",
                },
            )
    context.log.info("Completed Finland verotax raw CSV full refresh")


@dg.asset(
    name="finland_verotax_records_duckdb",
    deps=RAW_YEAR_ASSET_KEYS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FINLAND_VEROTAX_DUCKDB_POOL,
    description=(
        "Typed Finland tax records table unioned from the per-year raw CSV "
        "staging tables (decimal-comma amounts cast, municipality split)."
    ),
)
def finland_verotax_records_duckdb(
    context: dg.AssetExecutionContext,
    finland_verotax_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with finland_verotax_duckdb.get_connection() as connection:
        counts = build_finland_verotax_tax_records(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "table": f"{DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}",
            **counts,
        }
    )


@dg.asset(
    name="finland_verotax_records_usd_duckdb",
    deps=[dg.AssetKey("finland_verotax_records_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FINLAND_VEROTAX_DUCKDB_POOL,
    description="Adds USD and FX metadata columns to Finland tax records in DuckDB.",
)
def finland_verotax_records_usd_duckdb(
    context: dg.AssetExecutionContext,
    finland_verotax_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with finland_verotax_duckdb.get_connection() as connection:
        counts = apply_finland_verotax_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="finland_verotax_records_clickhouse",
    deps=[dg.AssetKey("finland_verotax_records_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FINLAND_VEROTAX_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_FI_TAX_RECORDS_TABLE},
    description=(
        "Finland public corporate income tax records exported to ClickHouse "
        "corpscout.fi_tax_records."
    ),
)
def finland_verotax_records_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    finland_verotax_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(finland_verotax_duckdb) as connection:
        rows = export_finland_verotax_records_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_FI_TAX_RECORDS_TABLE},
    )


finland_verotax_job = dg.define_asset_job(
    "finland_verotax_job",
    selection=dg.AssetSelection.assets("finland_verotax_records_clickhouse").upstream(),
)
finland_verotax_schedule = dg.ScheduleDefinition(
    name="finland_verotax_schedule",
    job=finland_verotax_job,
    # Yearly: Vero publishes the new tax-year CSV in early November.
    cron_schedule="50 5 12 11 *",
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[
        finland_verotax_raw_duckdb,
        finland_verotax_records_duckdb,
        finland_verotax_records_usd_duckdb,
        finland_verotax_records_clickhouse,
    ],
    jobs=[finland_verotax_job],
    schedules=[finland_verotax_schedule],
    resources={
        "finland_verotax_duckdb": duckdb_resource(FINLAND_VEROTAX_DUCKDB_PATH),
    },
)
