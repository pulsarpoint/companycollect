from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.brazil_cvm import tables
from dagster_v3.defs.brazil_cvm.clickhouse import export_brazil_cvm_dfp_clickhouse
from dagster_v3.defs.brazil_cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_AUDITOR_REPORTS_TABLE,
    DFP_CAPITAL_COMPOSITION_TABLE,
    DFP_DOCUMENTS_TABLE,
    DFP_STATEMENT_ROWS_TABLE,
    parse_brazil_cvm_dfp_archive_from_object_store,
)
from dagster_v3.defs.brazil_cvm.source import (
    BRAZIL_CVM_DFP_END_YEAR,
    BRAZIL_CVM_DFP_START_YEAR,
    BRAZIL_CVM_GROUP_NAME,
    BrazilCvmDfpResource,
)
from dagster_v3.defs.brazil_cvm.usd_conversion import (
    apply_brazil_cvm_statement_rows_usd_conversion,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_CVM_DUCKDB_PATH = Path("data/brazil_cvm_source.duckdb")

BRAZIL_CVM_DFP_RAW_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        str(year)
        for year in range(BRAZIL_CVM_DFP_START_YEAR, BRAZIL_CVM_DFP_END_YEAR + 1)
    ]
)


@dg.asset(
    group_name=BRAZIL_CVM_GROUP_NAME,
    kinds={"python", "s3", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_CVM_DFP_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Downloads Brazil CVM DFP yearly ZIP archives for 2010-2026 into object storage.",
)
def brazil_cvm_dfp_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_cvm_dfp: BrazilCvmDfpResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_cvm_dfp.sync_year_archive(
        year=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    deps=[dg.AssetKey("brazil_cvm_dfp_raw_archives_s3")],
    group_name=BRAZIL_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "csv", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_CVM_DFP_RAW_PARTITIONS,
    pool="brazil_cvm_duckdb",
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Parses Brazil CVM DFP yearly ZIP archives from object storage into DuckDB.",
)
def brazil_cvm_dfp_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_cvm_duckdb.get_connection() as connection:
        counts = parse_brazil_cvm_dfp_archive_from_object_store(
            connection=connection,
            object_store=object_store,
            year=context.partition_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "dfp_year": context.partition_key,
            "duckdb_path": str(BRAZIL_CVM_DUCKDB_PATH),
            "duckdb_schema": BRAZIL_CVM_DUCKDB_SCHEMA,
            "document_table": DFP_DOCUMENTS_TABLE,
            "statement_rows_table": DFP_STATEMENT_ROWS_TABLE,
            "capital_composition_table": DFP_CAPITAL_COMPOSITION_TABLE,
            "auditor_reports_table": DFP_AUDITOR_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=[
        dg.AssetDep(
            dg.AssetKey("brazil_cvm_dfp_raw_duckdb"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=BRAZIL_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "currency", "fx", "cvm", "dfp"},
    pool="brazil_cvm_duckdb",
    description="Adds USD and FX metadata columns to Brazil CVM DFP statement rows in DuckDB.",
)
def brazil_cvm_dfp_statement_rows_usd_duckdb(
    context: dg.AssetExecutionContext,
    brazil_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    BRAZIL_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_cvm_duckdb.get_connection() as connection:
        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("brazil_cvm_dfp_statement_rows_usd_duckdb")],
    group_name=BRAZIL_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cvm", "dfp"},
    pool="brazil_cvm_duckdb",
    metadata={"tables": ", ".join(tables.BR_CVM_DFP_TABLES)},
    description="Exports converted Brazil CVM DFP DuckDB tables to ClickHouse.",
)
def brazil_cvm_dfp_raw_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    brazil_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Exporting Brazil CVM DFP DuckDB tables to ClickHouse: tables=%s",
        tables.BR_CVM_DFP_TABLES,
    )
    with read_only_duckdb_connection(brazil_cvm_duckdb) as connection:
        counts = export_brazil_cvm_dfp_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


brazil_cvm_dfp_raw_backfill_job = dg.define_asset_job(
    "brazil_cvm_dfp_raw_backfill_job",
    selection=dg.AssetSelection.assets(
        "brazil_cvm_dfp_raw_archives_s3",
        "brazil_cvm_dfp_raw_duckdb",
    ),
)


defs = dg.Definitions(
    assets=[
        brazil_cvm_dfp_raw_archives_s3,
        brazil_cvm_dfp_raw_duckdb,
        brazil_cvm_dfp_statement_rows_usd_duckdb,
        brazil_cvm_dfp_raw_clickhouse,
    ],
    jobs=[brazil_cvm_dfp_raw_backfill_job],
    resources={
        "brazil_cvm_dfp": BrazilCvmDfpResource(),
        "brazil_cvm_duckdb": duckdb_resource(BRAZIL_CVM_DUCKDB_PATH),
    },
)
