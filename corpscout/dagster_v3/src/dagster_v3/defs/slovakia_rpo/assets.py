from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.slovakia_rpo import resources, tables
from dagster_v3.defs.slovakia_rpo.clickhouse import (
    export_slovakia_rpo_clickhouse_companies,
    export_slovakia_rpo_clickhouse_industries,
)
from dagster_v3.defs.slovakia_rpo.industries import build_slovakia_rpo_industries

GROUP_NAME = "slovakia_rpo"
SLOVAKIA_RPO_DUCKDB_POOL = "slovakia_rpo_duckdb"
SLOVAKIA_RPO_DUCKDB_PATH = Path("data/slovakia_rpo_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RAW_ASSET_KEY = "slovakia_rpo_raw_duckdb"


@dg.asset(
    name=RAW_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=SLOVAKIA_RPO_DUCKDB_POOL,
    description="Slovak RPO weekly pg_dump (rpo2.organizations) parsed + flattened into DuckDB.",
)
def slovakia_rpo_raw_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        "Loading Slovak RPO dump: url=%s, duckdb_path=%s",
        tables.RPO_DUMP_URL, SLOVAKIA_RPO_DUCKDB_PATH,
    )
    rows = resources.load_slovakia_rpo_dump(
        database_path=SLOVAKIA_RPO_DUCKDB_PATH, log=context.log.info
    )
    return dg.MaterializeResult(metadata={"rows": rows, "source_url": tables.RPO_DUMP_URL})


@dg.asset(
    name="slovakia_rpo_companies_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=SLOVAKIA_RPO_DUCKDB_POOL,
    description="Slovak RPO normalized companies (name/legal form/status/address).",
)
def slovakia_rpo_companies_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = resources.build_slovakia_rpo_companies(
        database_path=SLOVAKIA_RPO_DUCKDB_PATH, source_run_id=context.run_id, log=context.log.info
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("slovakia_rpo_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=SLOVAKIA_RPO_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    description="Slovak RPO companies exported to ClickHouse corpscout.sk_companies.",
)
def slovakia_rpo_clickhouse_companies(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    rows = export_slovakia_rpo_clickhouse_companies(
        database_path=SLOVAKIA_RPO_DUCKDB_PATH, clickhouse=clickhouse, log=context.log.info
    )
    return dg.MaterializeResult(metadata={"rows": rows, "table": tables.QUALIFIED_COMPANIES_TABLE})


@dg.asset(
    name="slovakia_rpo_industries_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=SLOVAKIA_RPO_DUCKDB_POOL,
    description="Slovak RPO industries from the main SK-NACE activity (→ NACE).",
)
def slovakia_rpo_industries_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = build_slovakia_rpo_industries(
        database_path=SLOVAKIA_RPO_DUCKDB_PATH, source_run_id=context.run_id, log=context.log.info
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("slovakia_rpo_industries_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=SLOVAKIA_RPO_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_INDUSTRIES_TABLE},
    description=(
        "Slovak RPO industries exported to ClickHouse corpscout.sk_industries "
        "(joins nace_categories on nace_code/nace_revision)."
    ),
)
def slovakia_rpo_clickhouse_industries(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    rows = export_slovakia_rpo_clickhouse_industries(
        database_path=SLOVAKIA_RPO_DUCKDB_PATH, clickhouse=clickhouse, log=context.log.info
    )
    return dg.MaterializeResult(metadata={"rows": rows, "table": tables.QUALIFIED_INDUSTRIES_TABLE})


# --- Jobs & schedules --------------------------------------------------------
# Register + industries both derive from the ONE weekly rpo2.sql.gz dump
# (regenerated every Sunday ~04:00 CET) → run Monday morning.
slovakia_rpo_register_job = dg.define_asset_job(
    "slovakia_rpo_register_job",
    selection=dg.AssetSelection.assets(
        "slovakia_rpo_clickhouse_companies", "slovakia_rpo_clickhouse_industries"
    ).upstream(),
)
slovakia_rpo_register_schedule = dg.ScheduleDefinition(
    name="slovakia_rpo_register_schedule",
    job=slovakia_rpo_register_job,
    cron_schedule="0 7 * * 1",  # weekly, Monday 07:00 (after the Sunday dump)
    execution_timezone="Europe/Belgrade",
)

slovakia_rpo_full_refresh_job = dg.define_asset_job(
    "slovakia_rpo_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)
