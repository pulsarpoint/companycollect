from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.clickhouse import (
    export_brazil_fin_cvm_companies_clickhouse,
    export_brazil_fin_cvm_dfp_clickhouse,
    export_brazil_fin_cvm_itr_clickhouse,
)
from dagster_v3.defs.brazil_financial.cvm.companies import (
    CVM_COMPANIES_SOURCE_URL,
    CVM_COMPANIES_TABLE,
    load_brazil_fin_cvm_companies_from_url,
)
from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_AUDITOR_REPORTS_TABLE,
    DFP_CAPITAL_COMPOSITION_TABLE,
    DFP_DOCUMENTS_TABLE,
    DFP_STATEMENT_ROWS_TABLE,
    parse_brazil_fin_cvm_dfp_archive_from_object_store,
)
from dagster_v3.defs.brazil_financial.cvm.source import (
    BRAZIL_CVM_DFP_END_YEAR,
    BRAZIL_CVM_DFP_START_YEAR,
    BRAZIL_CVM_ITR_END_YEAR,
    BRAZIL_CVM_ITR_START_YEAR,
    BRAZIL_FIN_CVM_GROUP_NAME,
    BrazilCvmDfpResource,
    BrazilCvmItrResource,
)
from dagster_v3.defs.brazil_financial.cvm.usd_conversion import (
    apply_brazil_cvm_statement_rows_usd_conversion_for_table,
    apply_brazil_cvm_statement_rows_usd_conversion,
)
from dagster_v3.defs.brazil_financial.cvm.itr_parsing import (
    ITR_AUDITOR_REPORTS_TABLE,
    ITR_CAPITAL_COMPOSITION_TABLE,
    ITR_DOCUMENTS_TABLE,
    ITR_STATEMENT_ROWS_TABLE,
    parse_brazil_fin_cvm_itr_archive_from_object_store,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_FIN_CVM_DUCKDB_PATH = Path("data/brazil_cvm_source.duckdb")

BRAZIL_FIN_CVM_DFP_RAW_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        str(year)
        for year in range(BRAZIL_CVM_DFP_START_YEAR, BRAZIL_CVM_DFP_END_YEAR + 1)
    ]
)
BRAZIL_FIN_CVM_ITR_RAW_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        str(year)
        for year in range(BRAZIL_CVM_ITR_START_YEAR, BRAZIL_CVM_ITR_END_YEAR + 1)
    ]
)


@dg.asset(
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "s3", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_FIN_CVM_DFP_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Downloads Brazil CVM DFP yearly ZIP archives for 2010-2026 into object storage.",
)
def brazil_fin_cvm_dfp_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_fin_cvm_dfp: BrazilCvmDfpResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_fin_cvm_dfp.sync_year_archive(
        year=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "s3", "zip", "cvm", "itr"},
    partitions_def=BRAZIL_FIN_CVM_ITR_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Downloads Brazil CVM ITR yearly ZIP archives for 2011-2026 into object storage.",
)
def brazil_fin_cvm_itr_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_fin_cvm_itr: BrazilCvmItrResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_fin_cvm_itr.sync_year_archive(
        year=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cvm", "companies"},
    pool="brazil_fin_cvm_duckdb",
    description="Loads the Brazil CVM public-company support table into DuckDB.",
)
def brazil_fin_cvm_companies_duckdb(
    context: dg.AssetExecutionContext,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_FIN_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = load_brazil_fin_cvm_companies_from_url(
            connection=connection,
            source_run_id=context.run_id,
            source_url=CVM_COMPANIES_SOURCE_URL,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "source_url": CVM_COMPANIES_SOURCE_URL,
            "duckdb_path": str(BRAZIL_FIN_CVM_DUCKDB_PATH),
            "duckdb_schema": BRAZIL_CVM_DUCKDB_SCHEMA,
            "company_table": CVM_COMPANIES_TABLE,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_companies_duckdb")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cvm", "companies"},
    pool="brazil_fin_cvm_duckdb",
    metadata={"table": tables.QUALIFIED_BR_CVM_COMPANIES_TABLE},
    description="Exports the Brazil CVM public-company support table to ClickHouse.",
)
def brazil_fin_cvm_companies_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(brazil_fin_cvm_duckdb) as connection:
        counts = export_brazil_fin_cvm_companies_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_dfp_raw_archives_s3")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "csv", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_FIN_CVM_DFP_RAW_PARTITIONS,
    pool="brazil_fin_cvm_duckdb",
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Parses Brazil CVM DFP yearly ZIP archives from object storage into DuckDB.",
)
def brazil_fin_cvm_dfp_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_FIN_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = parse_brazil_fin_cvm_dfp_archive_from_object_store(
            connection=connection,
            object_store=object_store,
            year=context.partition_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "dfp_year": context.partition_key,
            "duckdb_path": str(BRAZIL_FIN_CVM_DUCKDB_PATH),
            "duckdb_schema": BRAZIL_CVM_DUCKDB_SCHEMA,
            "document_table": DFP_DOCUMENTS_TABLE,
            "statement_rows_table": DFP_STATEMENT_ROWS_TABLE,
            "capital_composition_table": DFP_CAPITAL_COMPOSITION_TABLE,
            "auditor_reports_table": DFP_AUDITOR_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_itr_raw_archives_s3")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "csv", "zip", "cvm", "itr"},
    partitions_def=BRAZIL_FIN_CVM_ITR_RAW_PARTITIONS,
    pool="brazil_fin_cvm_duckdb",
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Parses Brazil CVM ITR yearly ZIP archives from object storage into DuckDB.",
)
def brazil_fin_cvm_itr_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_FIN_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = parse_brazil_fin_cvm_itr_archive_from_object_store(
            connection=connection,
            object_store=object_store,
            year=context.partition_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "itr_year": context.partition_key,
            "duckdb_path": str(BRAZIL_FIN_CVM_DUCKDB_PATH),
            "duckdb_schema": BRAZIL_CVM_DUCKDB_SCHEMA,
            "document_table": ITR_DOCUMENTS_TABLE,
            "statement_rows_table": ITR_STATEMENT_ROWS_TABLE,
            "capital_composition_table": ITR_CAPITAL_COMPOSITION_TABLE,
            "auditor_reports_table": ITR_AUDITOR_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=[
        dg.AssetDep(
            dg.AssetKey("brazil_fin_cvm_dfp_raw_duckdb"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "currency", "fx", "cvm", "dfp"},
    pool="brazil_fin_cvm_duckdb",
    description="Adds USD and FX metadata columns to Brazil CVM DFP statement rows in DuckDB.",
)
def brazil_fin_cvm_dfp_statement_rows_usd_duckdb(
    context: dg.AssetExecutionContext,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    BRAZIL_FIN_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        dg.AssetDep(
            dg.AssetKey("brazil_fin_cvm_itr_raw_duckdb"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "currency", "fx", "cvm", "itr"},
    pool="brazil_fin_cvm_duckdb",
    description="Adds USD and FX metadata columns to Brazil CVM ITR statement rows in DuckDB.",
)
def brazil_fin_cvm_itr_statement_rows_usd_duckdb(
    context: dg.AssetExecutionContext,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    BRAZIL_FIN_CVM_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = apply_brazil_cvm_statement_rows_usd_conversion_for_table(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            statement_rows_table=ITR_STATEMENT_ROWS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        dg.AssetKey("brazil_fin_cvm_dfp_statement_rows_usd_duckdb"),
        dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
    ],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cvm", "dfp"},
    pool="brazil_fin_cvm_duckdb",
    metadata={"tables": ", ".join(tables.BR_CVM_DFP_TABLES)},
    description="Exports converted Brazil CVM DFP DuckDB tables to ClickHouse.",
)
def brazil_fin_cvm_dfp_raw_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Exporting Brazil CVM DFP DuckDB tables to ClickHouse: tables=%s",
        tables.BR_CVM_DFP_TABLES,
    )
    with read_only_duckdb_connection(brazil_fin_cvm_duckdb) as connection:
        counts = export_brazil_fin_cvm_dfp_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        dg.AssetKey("brazil_fin_cvm_itr_statement_rows_usd_duckdb"),
        dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
    ],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cvm", "itr"},
    pool="brazil_fin_cvm_duckdb",
    metadata={"tables": ", ".join(tables.BR_CVM_ITR_TABLES)},
    description="Exports converted Brazil CVM ITR DuckDB tables to ClickHouse.",
)
def brazil_fin_cvm_itr_raw_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Exporting Brazil CVM ITR DuckDB tables to ClickHouse: tables=%s",
        tables.BR_CVM_ITR_TABLES,
    )
    with read_only_duckdb_connection(brazil_fin_cvm_duckdb) as connection:
        counts = export_brazil_fin_cvm_itr_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


brazil_fin_cvm_dfp_raw_backfill_job = dg.define_asset_job(
    "brazil_fin_cvm_dfp_raw_backfill_job",
    selection=dg.AssetSelection.assets(
        "brazil_fin_cvm_dfp_raw_archives_s3",
        "brazil_fin_cvm_dfp_raw_duckdb",
    ),
)

brazil_fin_cvm_itr_raw_backfill_job = dg.define_asset_job(
    "brazil_fin_cvm_itr_raw_backfill_job",
    selection=dg.AssetSelection.assets(
        "brazil_fin_cvm_itr_raw_archives_s3",
        "brazil_fin_cvm_itr_raw_duckdb",
    ),
)


defs = dg.Definitions(
    assets=[
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        brazil_fin_cvm_dfp_raw_clickhouse,
        brazil_fin_cvm_itr_raw_archives_s3,
        brazil_fin_cvm_itr_raw_duckdb,
        brazil_fin_cvm_itr_statement_rows_usd_duckdb,
        brazil_fin_cvm_itr_raw_clickhouse,
    ],
    jobs=[brazil_fin_cvm_dfp_raw_backfill_job, brazil_fin_cvm_itr_raw_backfill_job],
    resources={
        "brazil_fin_cvm_dfp": BrazilCvmDfpResource(),
        "brazil_fin_cvm_itr": BrazilCvmItrResource(),
        "brazil_fin_cvm_duckdb": duckdb_resource(BRAZIL_FIN_CVM_DUCKDB_PATH),
    },
)
