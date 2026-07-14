from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.brazil_companies.cgu import parsing, source, tables
from dagster_v3.defs.brazil_companies.cgu.clickhouse import (
    export_brazil_comp_cgu_table_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

GROUP_NAME = source.BRAZIL_COMP_CGU_GROUP_NAME
BRAZIL_COMP_CGU_DUCKDB_POOL = "brazil_comp_cgu_duckdb"
BRAZIL_COMP_CGU_DUCKDB_PATH = Path("data/brazil_cgu_source.duckdb")

RAW_ARCHIVES_ASSET_KEY = "brazil_comp_cgu_raw_archives_s3"
CEIS_DUCKDB_ASSET_KEY = "brazil_comp_cgu_ceis_company_sanctions_duckdb"
CNEP_DUCKDB_ASSET_KEY = "brazil_comp_cgu_cnep_company_sanctions_duckdb"
CEPIM_DUCKDB_ASSET_KEY = "brazil_comp_cgu_cepim_blocked_entities_duckdb"
LENIENCY_AGREEMENTS_DUCKDB_ASSET_KEY = "brazil_comp_cgu_leniency_agreements_duckdb"
LENIENCY_EFFECTS_DUCKDB_ASSET_KEY = "brazil_comp_cgu_leniency_agreement_effects_duckdb"
CEIS_CLICKHOUSE_ASSET_KEY = "brazil_comp_cgu_ceis_company_sanctions_clickhouse"
CNEP_CLICKHOUSE_ASSET_KEY = "brazil_comp_cgu_cnep_company_sanctions_clickhouse"
CEPIM_CLICKHOUSE_ASSET_KEY = "brazil_comp_cgu_cepim_blocked_entities_clickhouse"
LENIENCY_AGREEMENTS_CLICKHOUSE_ASSET_KEY = (
    "brazil_comp_cgu_leniency_agreements_clickhouse"
)
LENIENCY_EFFECTS_CLICKHOUSE_ASSET_KEY = (
    "brazil_comp_cgu_leniency_agreement_effects_clickhouse"
)


@dg.asset(
    name=RAW_ARCHIVES_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "cgu"},
    description=(
        "Downloads latest Brazil CGU/Portal da Transparencia sanctions ZIP "
        "archives into object storage."
    ),
)
def brazil_comp_cgu_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_comp_cgu: source.BrazilCguResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_comp_cgu.sync_latest_archives(
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    name=CEIS_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    description="Parses CEIS company sanctions from CGU ZIP CSV files into DuckDB.",
)
def brazil_comp_cgu_ceis_company_sanctions_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_cgu_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _parse_duckdb_asset(
        context=context,
        object_store=object_store,
        duckdb=brazil_comp_cgu_duckdb,
        parse_fn=parsing.parse_brazil_comp_cgu_ceis_company_sanctions_from_object_store,
        table=tables.CEIS_COMPANY_SANCTIONS_TABLE,
    )


@dg.asset(
    name=CNEP_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    description="Parses CNEP company sanctions from CGU ZIP CSV files into DuckDB.",
)
def brazil_comp_cgu_cnep_company_sanctions_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_cgu_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _parse_duckdb_asset(
        context=context,
        object_store=object_store,
        duckdb=brazil_comp_cgu_duckdb,
        parse_fn=parsing.parse_brazil_comp_cgu_cnep_company_sanctions_from_object_store,
        table=tables.CNEP_COMPANY_SANCTIONS_TABLE,
    )


@dg.asset(
    name=CEPIM_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    description="Parses CEPIM blocked entities from CGU ZIP CSV files into DuckDB.",
)
def brazil_comp_cgu_cepim_blocked_entities_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_cgu_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _parse_duckdb_asset(
        context=context,
        object_store=object_store,
        duckdb=brazil_comp_cgu_duckdb,
        parse_fn=parsing.parse_brazil_comp_cgu_cepim_blocked_entities_from_object_store,
        table=tables.CEPIM_BLOCKED_ENTITIES_TABLE,
    )


@dg.asset(
    name=LENIENCY_AGREEMENTS_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    description=(
        "Parses company leniency agreements from CGU ZIP CSV files into DuckDB."
    ),
)
def brazil_comp_cgu_leniency_agreements_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_cgu_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _parse_duckdb_asset(
        context=context,
        object_store=object_store,
        duckdb=brazil_comp_cgu_duckdb,
        parse_fn=parsing.parse_brazil_comp_cgu_leniency_agreements_from_object_store,
        table=tables.LENIENCY_AGREEMENTS_TABLE,
    )


@dg.asset(
    name=LENIENCY_EFFECTS_DUCKDB_ASSET_KEY,
    deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    description="Parses CGU leniency agreement effects from ZIP CSV files into DuckDB.",
)
def brazil_comp_cgu_leniency_agreement_effects_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_comp_cgu_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _parse_duckdb_asset(
        context=context,
        object_store=object_store,
        duckdb=brazil_comp_cgu_duckdb,
        parse_fn=(
            parsing.parse_brazil_comp_cgu_leniency_agreement_effects_from_object_store
        ),
        table=tables.LENIENCY_AGREEMENT_EFFECTS_TABLE,
    )


@dg.asset(
    name=CEIS_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(CEIS_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    metadata={"table": tables.BR_CGU_CEIS_COMPANY_SANCTIONS_TABLE_CH},
    description="Brazil CGU CEIS company sanctions exported to ClickHouse.",
)
def brazil_comp_cgu_ceis_company_sanctions_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_clickhouse_asset(
        context=context,
        clickhouse=clickhouse,
        table=tables.CGU_TABLES[tables.CEIS_COMPANY_SANCTIONS_TABLE],
    )


@dg.asset(
    name=CNEP_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(CNEP_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    metadata={"table": tables.BR_CGU_CNEP_COMPANY_SANCTIONS_TABLE_CH},
    description="Brazil CGU CNEP company sanctions exported to ClickHouse.",
)
def brazil_comp_cgu_cnep_company_sanctions_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_clickhouse_asset(
        context=context,
        clickhouse=clickhouse,
        table=tables.CGU_TABLES[tables.CNEP_COMPANY_SANCTIONS_TABLE],
    )


@dg.asset(
    name=CEPIM_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(CEPIM_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    metadata={"table": tables.BR_CGU_CEPIM_BLOCKED_ENTITIES_TABLE_CH},
    description="Brazil CGU CEPIM blocked entities exported to ClickHouse.",
)
def brazil_comp_cgu_cepim_blocked_entities_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_clickhouse_asset(
        context=context,
        clickhouse=clickhouse,
        table=tables.CGU_TABLES[tables.CEPIM_BLOCKED_ENTITIES_TABLE],
    )


@dg.asset(
    name=LENIENCY_AGREEMENTS_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(LENIENCY_AGREEMENTS_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    metadata={"table": tables.BR_CGU_LENIENCY_AGREEMENTS_TABLE_CH},
    description="Brazil CGU leniency agreements exported to ClickHouse.",
)
def brazil_comp_cgu_leniency_agreements_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_clickhouse_asset(
        context=context,
        clickhouse=clickhouse,
        table=tables.CGU_TABLES[tables.LENIENCY_AGREEMENTS_TABLE],
    )


@dg.asset(
    name=LENIENCY_EFFECTS_CLICKHOUSE_ASSET_KEY,
    deps=[dg.AssetKey(LENIENCY_EFFECTS_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cgu"},
    pool=BRAZIL_COMP_CGU_DUCKDB_POOL,
    metadata={"table": tables.BR_CGU_LENIENCY_AGREEMENT_EFFECTS_TABLE_CH},
    description="Brazil CGU leniency agreement effects exported to ClickHouse.",
)
def brazil_comp_cgu_leniency_agreement_effects_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_clickhouse_asset(
        context=context,
        clickhouse=clickhouse,
        table=tables.CGU_TABLES[tables.LENIENCY_AGREEMENT_EFFECTS_TABLE],
    )


def _parse_duckdb_asset(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    duckdb: DuckDBResource,
    parse_fn,
    table: str,
) -> dg.MaterializeResult:
    BRAZIL_COMP_CGU_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.get_connection() as connection:
        counts = parse_fn(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_path": str(BRAZIL_COMP_CGU_DUCKDB_PATH),
            "duckdb_table": f"{parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{table}",
        }
    )


def _export_clickhouse_asset(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    table: tables.CguTable,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(
        duckdb_resource(BRAZIL_COMP_CGU_DUCKDB_PATH)
    ) as connection:
        rows = export_brazil_comp_cgu_table_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            table=table,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": f"{tables.BRAZIL_COMP_CGU_DATABASE}.{table.clickhouse_table}",
        }
    )


brazil_comp_cgu_refresh_job = dg.define_asset_job(
    "brazil_comp_cgu_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)


defs = dg.Definitions(
    assets=[
        brazil_comp_cgu_raw_archives_s3,
        brazil_comp_cgu_ceis_company_sanctions_duckdb,
        brazil_comp_cgu_cnep_company_sanctions_duckdb,
        brazil_comp_cgu_cepim_blocked_entities_duckdb,
        brazil_comp_cgu_leniency_agreements_duckdb,
        brazil_comp_cgu_leniency_agreement_effects_duckdb,
        brazil_comp_cgu_ceis_company_sanctions_clickhouse,
        brazil_comp_cgu_cnep_company_sanctions_clickhouse,
        brazil_comp_cgu_cepim_blocked_entities_clickhouse,
        brazil_comp_cgu_leniency_agreements_clickhouse,
        brazil_comp_cgu_leniency_agreement_effects_clickhouse,
    ],
    jobs=[brazil_comp_cgu_refresh_job],
    resources={
        "brazil_comp_cgu": source.BrazilCguResource(),
        "brazil_comp_cgu_duckdb": duckdb_resource(BRAZIL_COMP_CGU_DUCKDB_PATH),
    },
)
