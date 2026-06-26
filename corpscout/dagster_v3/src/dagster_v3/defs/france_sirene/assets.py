from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.france_sirene import resources, tables
from dagster_v3.defs.france_sirene.clickhouse import (
    export_france_sirene_clickhouse_companies,
    export_france_sirene_clickhouse_industries,
)
from dagster_v3.defs.france_sirene.industries import build_france_sirene_industries

GROUP_NAME = "france_sirene"
FRANCE_SIRENE_DUCKDB_POOL = "france_sirene_duckdb"
# File stem (catalog) must differ from DLT_DATASET_NAME or DuckDB's binder cannot
# disambiguate "<dataset>.<table>".
FRANCE_SIRENE_DUCKDB_PATH = Path("data/france_sirene_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RAW_ASSET_KEY = "france_sirene_unite_legale_raw_duckdb"


@dg.asset(
    name=RAW_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    description=(
        "France SIRENE StockUniteLegale (legal units) loaded raw into DuckDB via "
        "the multithreaded read_csv (URL resolved live from data.gouv.fr)."
    ),
)
def france_sirene_unite_legale_raw_duckdb(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    download_url = resources.resolve_unite_legale_url()
    context.log.info(
        "Loading France SIRENE legal units: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        FRANCE_SIRENE_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.UNITE_LEGALE_RAW_TABLE,
    )
    FRANCE_SIRENE_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with france_sirene_duckdb.get_connection() as connection:
        rows = resources.load_france_sirene_unite_legale(
            connection=connection,
            download_url=download_url,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "source_url": download_url},
    )


SIEGE_ASSET_KEY = "france_sirene_etablissement_siege_raw_duckdb"


@dg.asset(
    name=SIEGE_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    description=(
        "France SIRENE siège (HQ) addresses — one row per legal unit from the "
        "StockEtablissement file, filtered to etablissementSiege='true'."
    ),
)
def france_sirene_etablissement_siege_raw_duckdb(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    download_url = resources.resolve_etablissement_url()
    context.log.info(
        "Loading France SIRENE siège addresses: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        FRANCE_SIRENE_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.ETABLISSEMENT_SIEGE_TABLE,
    )
    FRANCE_SIRENE_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with france_sirene_duckdb.get_connection() as connection:
        rows = resources.load_france_sirene_etablissement_siege(
            connection=connection,
            download_url=download_url,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata={"rows": rows, "source_url": download_url})


@dg.asset(
    name="france_sirene_companies_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY), dg.AssetKey(SIEGE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    description="France SIRENE normalized legal units (name/legal form/status/NAF/siège address).",
)
def france_sirene_companies_duckdb(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    source_url = resources.resolve_unite_legale_url()
    with france_sirene_duckdb.get_connection() as connection:
        counts = resources.build_france_sirene_companies(
            connection=connection,
            source_run_id=context.run_id,
            source_url=source_url,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("france_sirene_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    description="France SIRENE companies exported to ClickHouse corpscout.fr_companies.",
)
def france_sirene_clickhouse_companies(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with france_sirene_duckdb.get_connection() as connection:
        rows = export_france_sirene_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_COMPANIES_TABLE},
    )


@dg.asset(
    name="france_sirene_industries_duckdb",
    deps=[dg.AssetKey(RAW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    description=(
        "France SIRENE industries from each unit's principal NAF activity "
        "(NAF → NACE by trailing-letter truncation)."
    ),
)
def france_sirene_industries_duckdb(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with france_sirene_duckdb.get_connection() as connection:
        counts = build_france_sirene_industries(
            connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("france_sirene_industries_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FRANCE_SIRENE_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_INDUSTRIES_TABLE},
    description=(
        "France SIRENE industries exported to ClickHouse corpscout.fr_industries "
        "(joins nace_categories on nace_code/nace_revision)."
    ),
)
def france_sirene_clickhouse_industries(
    context: AssetExecutionContext,
    france_sirene_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with france_sirene_duckdb.get_connection() as connection:
        rows = export_france_sirene_clickhouse_industries(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_INDUSTRIES_TABLE},
    )


def _duckdb_table_count(
    *, connection: Any, table_name: str
) -> int:
    return int(connection.execute(f"select count(*) from {table_name}").fetchone()[0])


# --- Jobs & schedules --------------------------------------------------------
# Register + industries both derive from the ONE monthly StockUniteLegale download
# (the raw asset), so .upstream() of the two exports pulls a single download.
france_sirene_register_job = dg.define_asset_job(
    "france_sirene_register_job",
    selection=dg.AssetSelection.assets(
        "france_sirene_clickhouse_companies",
        "france_sirene_clickhouse_industries",
    ).upstream(),
)
france_sirene_register_schedule = dg.ScheduleDefinition(
    name="france_sirene_register_schedule",
    job=france_sirene_register_job,
    cron_schedule="0 7 6 * *",  # monthly, 6th 07:00 (after the new INSEE snapshot)
    execution_timezone="Europe/Belgrade",
)

# Whole-source trigger (all france_sirene assets), unscheduled.
france_sirene_full_refresh_job = dg.define_asset_job(
    "france_sirene_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)


defs = dg.Definitions(
    assets=[
        france_sirene_unite_legale_raw_duckdb,
        france_sirene_etablissement_siege_raw_duckdb,
        france_sirene_companies_duckdb,
        france_sirene_clickhouse_companies,
        france_sirene_industries_duckdb,
        france_sirene_clickhouse_industries,
    ],
    jobs=[
        france_sirene_register_job,
        france_sirene_full_refresh_job,
    ],
    schedules=[france_sirene_register_schedule],
    resources={
        "france_sirene_duckdb": duckdb_resource(FRANCE_SIRENE_DUCKDB_PATH),
    },
)
