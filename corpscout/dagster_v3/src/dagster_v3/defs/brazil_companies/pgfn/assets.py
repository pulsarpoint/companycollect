from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.brazil_companies.pgfn import parsing, source, tables
from dagster_v3.defs.brazil_companies.pgfn.clickhouse import (
    export_brazil_comp_pgfn_company_debts_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

GROUP_NAME = source.BRAZIL_COMP_PGFN_GROUP_NAME
BRAZIL_COMP_PGFN_DUCKDB_POOL = "brazil_comp_pgfn_duckdb"
BRAZIL_COMP_PGFN_DUCKDB_PATH = Path("data/brazil_pgfn_source.duckdb")
RAW_ARCHIVES_ASSET_KEY = "brazil_comp_pgfn_raw_archives_s3"
COMPANY_DEBTS_DUCKDB_ASSET_KEY = "brazil_comp_pgfn_company_debts_duckdb"
COMPANY_DEBTS_CLICKHOUSE_ASSET_KEY = "brazil_comp_pgfn_company_debts_clickhouse"

BRAZIL_COMP_PGFN_PARTITIONS = dg.StaticPartitionsDefinition(
    source.pgfn_partition_keys()
)


@dg.asset(
    name=RAW_ARCHIVES_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "pgfn"},
    partitions_def=BRAZIL_COMP_PGFN_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Downloads Brazil PGFN quarterly Dívida Ativa ZIP archives into object storage."
    ),
)
def brazil_comp_pgfn_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_comp_pgfn: source.BrazilPgfnResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_comp_pgfn.sync_snapshot_archives(
        snapshot_quarter=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name=COMPANY_DEBTS_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "pgfn"},
    partitions_def=BRAZIL_COMP_PGFN_PARTITIONS,
    pool=BRAZIL_COMP_PGFN_DUCKDB_POOL,
    description=(
        "Parses PGFN Dívida Ativa ZIP CSV members into a normalized company-debt "
        "DuckDB table."
    ),
)
def brazil_comp_pgfn_company_debts_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_pgfn_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_COMP_PGFN_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_comp_pgfn_duckdb.get_connection() as connection:
        counts = parsing.parse_brazil_comp_pgfn_archives_from_object_store(
            connection=connection,
            object_store=object_store,
            snapshot_quarter=context.partition_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "snapshot_quarter": context.partition_key,
            "duckdb_path": str(BRAZIL_COMP_PGFN_DUCKDB_PATH),
            "duckdb_table": f"{parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}",
        }
    )


@dg.asset(
    name=COMPANY_DEBTS_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(COMPANY_DEBTS_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "pgfn"},
    partitions_def=BRAZIL_COMP_PGFN_PARTITIONS,
    pool=BRAZIL_COMP_PGFN_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE},
    description="Brazil PGFN company debt rows exported to ClickHouse.",
)
def brazil_comp_pgfn_company_debts_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(
        duckdb_resource(BRAZIL_COMP_PGFN_DUCKDB_PATH)
    ) as connection:
        rows = export_brazil_comp_pgfn_company_debts_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "snapshot_quarter": context.partition_key,
            "table": tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE,
        }
    )


brazil_comp_pgfn_refresh_job = dg.define_asset_job(
    "brazil_comp_pgfn_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)


defs = dg.Definitions(
    assets=[
        brazil_comp_pgfn_raw_archives_s3,
        brazil_comp_pgfn_company_debts_duckdb,
        brazil_comp_pgfn_company_debts_clickhouse,
    ],
    jobs=[brazil_comp_pgfn_refresh_job],
    resources={
        "brazil_comp_pgfn": source.BrazilPgfnResource(),
        "brazil_comp_pgfn_duckdb": duckdb_resource(BRAZIL_COMP_PGFN_DUCKDB_PATH),
    },
)
