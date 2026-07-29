from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.france_annuaire import resources, tables
from dagster_v3.defs.france_annuaire.clickhouse import (
    export_france_company_enrichments_clickhouse,
)
from dagster_v3.defs.france_annuaire.records import (
    build_france_company_enrichments,
    load_france_annuaire_parquet,
)

GROUP_NAME = "france_annuaire"
DUCKDB_POOL = "france_annuaire_duckdb"
DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME


@dg.asset(
    name="france_annuaire_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb"},
    pool=DUCKDB_POOL,
    metadata={"table": f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"},
    description="Current Annuaire legal-unit enrichment Parquet staged in DuckDB.",
)
def france_annuaire_raw_duckdb(
    context: dg.AssetExecutionContext,
    france_annuaire_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    download_url = resources.resolve_legal_units_resource_url()
    with france_annuaire_duckdb.get_connection() as connection:
        rows = load_france_annuaire_parquet(
            duckdb_connection=connection,
            download_url=download_url,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": download_url,
            "table": f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}",
        }
    )


@dg.asset(
    name="france_annuaire_enrichments_duckdb",
    deps=[dg.AssetKey("france_annuaire_raw_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description="One normalized Annuaire enrichment row per French SIREN.",
)
def france_annuaire_enrichments_duckdb(
    context: dg.AssetExecutionContext,
    france_annuaire_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with france_annuaire_duckdb.get_connection() as connection:
        counts = build_france_company_enrichments(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="france_annuaire_enrichments_clickhouse",
    deps=[dg.AssetKey("france_annuaire_enrichments_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANY_ENRICHMENTS_TABLE},
    description="France Annuaire company enrichments exported to ClickHouse.",
)
def france_annuaire_enrichments_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    france_annuaire_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(france_annuaire_duckdb) as connection:
        rows = export_france_company_enrichments_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_COMPANY_ENRICHMENTS_TABLE,
        }
    )


france_annuaire_job = dg.define_asset_job(
    "france_annuaire_job",
    selection=dg.AssetSelection.assets(
        "france_annuaire_enrichments_clickhouse"
    ).upstream(),
)
france_annuaire_schedule = dg.ScheduleDefinition(
    name="france_annuaire_schedule",
    job=france_annuaire_job,
    cron_schedule="25 4 * * *",
    execution_timezone="Europe/Belgrade",
)

defs = dg.Definitions(
    assets=[
        france_annuaire_raw_duckdb,
        france_annuaire_enrichments_duckdb,
        france_annuaire_enrichments_clickhouse,
    ],
    jobs=[france_annuaire_job],
    schedules=[france_annuaire_schedule],
    resources={
        "france_annuaire_duckdb": duckdb_resource(DUCKDB_PATH),
    },
)
