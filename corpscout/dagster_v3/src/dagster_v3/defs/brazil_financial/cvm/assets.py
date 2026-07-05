import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.clickhouse import (
    EXPORT_TABLES,
    ITR_EXPORT_TABLES,
    export_brazil_fin_cvm_clickhouse_table,
    export_brazil_fin_cvm_companies_clickhouse,
)
from dagster_v3.defs.brazil_financial.cvm.checks import (
    financial_metrics_dataset_check,
    statement_rows_row_count_matches_duckdb_check,
    statement_rows_usd_coverage_check,
)
from dagster_v3.defs.brazil_financial.cvm.companies import (
    CVM_COMPANIES_SOURCE_URL,
    CVM_COMPANIES_TABLE,
    load_brazil_fin_cvm_companies_from_object_store,
    sync_brazil_fin_cvm_companies_csv,
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
from dagster_v3.defs.brazil_financial.cvm.storage import (
    BRAZIL_FIN_CVM_COMPANIES_DUCKDB_PATH,
    brazil_fin_cvm_existing_source_duckdb_connection,
    brazil_fin_cvm_read_only_partitioned_connection,
    brazil_fin_cvm_source_duckdb_connection,
    brazil_fin_cvm_source_duckdb_path,
    existing_brazil_fin_cvm_source_duckdb_paths,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

DFP_CLICKHOUSE_DEPENDENCIES = [
    dg.AssetDep(
        dg.AssetKey("brazil_fin_cvm_dfp_statement_rows_usd_duckdb"),
        partition_mapping=dg.AllPartitionMapping(),
    ),
    dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
]
ITR_CLICKHOUSE_DEPENDENCIES = [
    dg.AssetDep(
        dg.AssetKey("brazil_fin_cvm_itr_statement_rows_usd_duckdb"),
        partition_mapping=dg.AllPartitionMapping(),
    ),
    dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
]
DFP_CLICKHOUSE_ASSET_SPECS = (
    ("brazil_fin_cvm_dfp_documents_clickhouse", *EXPORT_TABLES[0]),
    ("brazil_fin_cvm_dfp_statement_rows_clickhouse", *EXPORT_TABLES[1]),
    ("brazil_fin_cvm_dfp_capital_composition_clickhouse", *EXPORT_TABLES[2]),
    ("brazil_fin_cvm_dfp_auditor_reports_clickhouse", *EXPORT_TABLES[3]),
)
ITR_CLICKHOUSE_ASSET_SPECS = (
    ("brazil_fin_cvm_itr_documents_clickhouse", *ITR_EXPORT_TABLES[0]),
    ("brazil_fin_cvm_itr_statement_rows_clickhouse", *ITR_EXPORT_TABLES[1]),
    ("brazil_fin_cvm_itr_capital_composition_clickhouse", *ITR_EXPORT_TABLES[2]),
    ("brazil_fin_cvm_itr_auditor_reports_clickhouse", *ITR_EXPORT_TABLES[3]),
)

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
    kinds={"python", "s3", "csv", "cvm", "companies"},
    description="Downloads the Brazil CVM public-company cadastro CSV into object storage.",
)
def brazil_fin_cvm_companies_raw_csv_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = sync_brazil_fin_cvm_companies_csv(
        object_store=object_store,
        source_url=CVM_COMPANIES_SOURCE_URL,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_companies_raw_csv_s3")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "csv", "cvm", "companies"},
    pool="brazil_fin_cvm_duckdb",
    description="Loads the Brazil CVM public-company support table into DuckDB.",
)
def brazil_fin_cvm_companies_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    brazil_fin_cvm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    BRAZIL_FIN_CVM_COMPANIES_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with brazil_fin_cvm_duckdb.get_connection() as connection:
        counts = load_brazil_fin_cvm_companies_from_object_store(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "source_url": CVM_COMPANIES_SOURCE_URL,
            "duckdb_path": str(BRAZIL_FIN_CVM_COMPANIES_DUCKDB_PATH),
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
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Parses Brazil CVM DFP yearly ZIP archives from object storage into DuckDB.",
)
def brazil_fin_cvm_dfp_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = brazil_fin_cvm_source_duckdb_path(
        family="dfp",
        year=context.partition_key,
    )
    with brazil_fin_cvm_source_duckdb_connection(
        family="dfp",
        year=context.partition_key,
    ) as connection:
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
            "duckdb_path": str(duckdb_path),
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
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Parses Brazil CVM ITR yearly ZIP archives from object storage into DuckDB.",
)
def brazil_fin_cvm_itr_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = brazil_fin_cvm_source_duckdb_path(
        family="itr",
        year=context.partition_key,
    )
    with brazil_fin_cvm_source_duckdb_connection(
        family="itr",
        year=context.partition_key,
    ) as connection:
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
            "duckdb_path": str(duckdb_path),
            "duckdb_schema": BRAZIL_CVM_DUCKDB_SCHEMA,
            "document_table": ITR_DOCUMENTS_TABLE,
            "statement_rows_table": ITR_STATEMENT_ROWS_TABLE,
            "capital_composition_table": ITR_CAPITAL_COMPOSITION_TABLE,
            "auditor_reports_table": ITR_AUDITOR_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_dfp_raw_duckdb")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "currency", "fx", "cvm", "dfp"},
    partitions_def=BRAZIL_FIN_CVM_DFP_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Adds USD and FX metadata columns to Brazil CVM DFP statement rows in the "
        "yearly DuckDB partition file."
    ),
)
def brazil_fin_cvm_dfp_statement_rows_usd_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    duckdb_path = brazil_fin_cvm_source_duckdb_path(
        family="dfp",
        year=context.partition_key,
    )
    with brazil_fin_cvm_existing_source_duckdb_connection(
        family="dfp",
        year=context.partition_key,
    ) as connection:
        counts = apply_brazil_cvm_statement_rows_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "dfp_year": context.partition_key,
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("brazil_fin_cvm_itr_raw_duckdb")],
    group_name=BRAZIL_FIN_CVM_GROUP_NAME,
    kinds={"python", "duckdb", "currency", "fx", "cvm", "itr"},
    partitions_def=BRAZIL_FIN_CVM_ITR_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Adds USD and FX metadata columns to Brazil CVM ITR statement rows in the "
        "yearly DuckDB partition file."
    ),
)
def brazil_fin_cvm_itr_statement_rows_usd_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    duckdb_path = brazil_fin_cvm_source_duckdb_path(
        family="itr",
        year=context.partition_key,
    )
    with brazil_fin_cvm_existing_source_duckdb_connection(
        family="itr",
        year=context.partition_key,
    ) as connection:
        counts = apply_brazil_cvm_statement_rows_usd_conversion_for_table(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            statement_rows_table=ITR_STATEMENT_ROWS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "itr_year": context.partition_key,
            "duckdb_path": str(duckdb_path),
        }
    )


def _build_brazil_fin_cvm_clickhouse_table_asset(
    *,
    asset_name: str,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
    family: str,
    deps: list[dg.AssetDep | dg.AssetKey],
) -> dg.AssetsDefinition:
    @dg.asset(
        name=asset_name,
        deps=deps,
        group_name=BRAZIL_FIN_CVM_GROUP_NAME,
        kinds={"python", "duckdb", "clickhouse", "cvm", family.lower()},
        metadata={"table": f"{tables.BRAZIL_CVM_DATABASE}.{clickhouse_table}"},
        description=(
            f"Exports Brazil CVM {family} DuckDB table {duckdb_table} to "
            f"ClickHouse table {clickhouse_table}."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        clickhouse: ClickhouseResource,
    ) -> dg.MaterializeResult:
        source_family = family.lower()
        partition_years = _partition_years_for_family(source_family)
        duckdb_paths = existing_brazil_fin_cvm_source_duckdb_paths(
            family=source_family,
            years=partition_years,
        )
        with brazil_fin_cvm_read_only_partitioned_connection(
            family=source_family,
            years=partition_years,
            table_names=(duckdb_table,),
        ) as connection:
            rows = export_brazil_fin_cvm_clickhouse_table(
                duckdb_connection=connection,
                clickhouse=clickhouse,
                duckdb_table=duckdb_table,
                clickhouse_table=clickhouse_table,
                columns=columns,
                family=family,
                log=context.log.info,
            )
        return dg.MaterializeResult(
            metadata={
                "row_count": rows,
                f"{clickhouse_table}_row_count": rows,
                "duckdb_table": f"{BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}",
                "duckdb_paths": [str(path) for path in duckdb_paths],
                "clickhouse_table": f"{tables.BRAZIL_CVM_DATABASE}.{clickhouse_table}",
            }
        )

    return _asset


def _partition_years_for_family(family: str) -> tuple[str, ...]:
    if family == "dfp":
        return tuple(BRAZIL_FIN_CVM_DFP_RAW_PARTITIONS.get_partition_keys())
    if family == "itr":
        return tuple(BRAZIL_FIN_CVM_ITR_RAW_PARTITIONS.get_partition_keys())
    raise ValueError(f"Unsupported Brazil CVM source family: {family}")


brazil_fin_cvm_dfp_documents_clickhouse = _build_brazil_fin_cvm_clickhouse_table_asset(
    asset_name=DFP_CLICKHOUSE_ASSET_SPECS[0][0],
    duckdb_table=DFP_CLICKHOUSE_ASSET_SPECS[0][1],
    clickhouse_table=DFP_CLICKHOUSE_ASSET_SPECS[0][2],
    columns=DFP_CLICKHOUSE_ASSET_SPECS[0][3],
    family="DFP",
    deps=DFP_CLICKHOUSE_DEPENDENCIES,
)
brazil_fin_cvm_dfp_statement_rows_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=DFP_CLICKHOUSE_ASSET_SPECS[1][0],
        duckdb_table=DFP_CLICKHOUSE_ASSET_SPECS[1][1],
        clickhouse_table=DFP_CLICKHOUSE_ASSET_SPECS[1][2],
        columns=DFP_CLICKHOUSE_ASSET_SPECS[1][3],
        family="DFP",
        deps=DFP_CLICKHOUSE_DEPENDENCIES,
    )
)
brazil_fin_cvm_dfp_capital_composition_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=DFP_CLICKHOUSE_ASSET_SPECS[2][0],
        duckdb_table=DFP_CLICKHOUSE_ASSET_SPECS[2][1],
        clickhouse_table=DFP_CLICKHOUSE_ASSET_SPECS[2][2],
        columns=DFP_CLICKHOUSE_ASSET_SPECS[2][3],
        family="DFP",
        deps=DFP_CLICKHOUSE_DEPENDENCIES,
    )
)
brazil_fin_cvm_dfp_auditor_reports_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=DFP_CLICKHOUSE_ASSET_SPECS[3][0],
        duckdb_table=DFP_CLICKHOUSE_ASSET_SPECS[3][1],
        clickhouse_table=DFP_CLICKHOUSE_ASSET_SPECS[3][2],
        columns=DFP_CLICKHOUSE_ASSET_SPECS[3][3],
        family="DFP",
        deps=DFP_CLICKHOUSE_DEPENDENCIES,
    )
)
brazil_fin_cvm_itr_documents_clickhouse = _build_brazil_fin_cvm_clickhouse_table_asset(
    asset_name=ITR_CLICKHOUSE_ASSET_SPECS[0][0],
    duckdb_table=ITR_CLICKHOUSE_ASSET_SPECS[0][1],
    clickhouse_table=ITR_CLICKHOUSE_ASSET_SPECS[0][2],
    columns=ITR_CLICKHOUSE_ASSET_SPECS[0][3],
    family="ITR",
    deps=ITR_CLICKHOUSE_DEPENDENCIES,
)
brazil_fin_cvm_itr_statement_rows_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=ITR_CLICKHOUSE_ASSET_SPECS[1][0],
        duckdb_table=ITR_CLICKHOUSE_ASSET_SPECS[1][1],
        clickhouse_table=ITR_CLICKHOUSE_ASSET_SPECS[1][2],
        columns=ITR_CLICKHOUSE_ASSET_SPECS[1][3],
        family="ITR",
        deps=ITR_CLICKHOUSE_DEPENDENCIES,
    )
)
brazil_fin_cvm_itr_capital_composition_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=ITR_CLICKHOUSE_ASSET_SPECS[2][0],
        duckdb_table=ITR_CLICKHOUSE_ASSET_SPECS[2][1],
        clickhouse_table=ITR_CLICKHOUSE_ASSET_SPECS[2][2],
        columns=ITR_CLICKHOUSE_ASSET_SPECS[2][3],
        family="ITR",
        deps=ITR_CLICKHOUSE_DEPENDENCIES,
    )
)
brazil_fin_cvm_itr_auditor_reports_clickhouse = (
    _build_brazil_fin_cvm_clickhouse_table_asset(
        asset_name=ITR_CLICKHOUSE_ASSET_SPECS[3][0],
        duckdb_table=ITR_CLICKHOUSE_ASSET_SPECS[3][1],
        clickhouse_table=ITR_CLICKHOUSE_ASSET_SPECS[3][2],
        columns=ITR_CLICKHOUSE_ASSET_SPECS[3][3],
        family="ITR",
        deps=ITR_CLICKHOUSE_DEPENDENCIES,
    )
)


@dg.asset_check(
    asset="brazil_fin_cvm_dfp_statement_rows_clickhouse",
    name="dfp_statement_rows_usd_complete",
)
def brazil_fin_cvm_dfp_statement_rows_usd_complete(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return statement_rows_usd_coverage_check(
        clickhouse=clickhouse,
        clickhouse_table=tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
        year_column="dfp_year",
    )


@dg.asset_check(
    asset="brazil_fin_cvm_itr_statement_rows_clickhouse",
    name="itr_statement_rows_usd_complete",
)
def brazil_fin_cvm_itr_statement_rows_usd_complete(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return statement_rows_usd_coverage_check(
        clickhouse=clickhouse,
        clickhouse_table=tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE,
        year_column="itr_year",
    )


@dg.asset_check(
    asset="brazil_fin_cvm_dfp_statement_rows_clickhouse",
    name="dfp_statement_rows_row_count_matches_duckdb",
)
def brazil_fin_cvm_dfp_statement_rows_row_count_matches_duckdb(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return statement_rows_row_count_matches_duckdb_check(
        clickhouse=clickhouse,
        family="DFP",
        clickhouse_table=tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
        duckdb_table=DFP_STATEMENT_ROWS_TABLE,
        years=_partition_years_for_family("dfp"),
    )


@dg.asset_check(
    asset="brazil_fin_cvm_itr_statement_rows_clickhouse",
    name="itr_statement_rows_row_count_matches_duckdb",
)
def brazil_fin_cvm_itr_statement_rows_row_count_matches_duckdb(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return statement_rows_row_count_matches_duckdb_check(
        clickhouse=clickhouse,
        family="ITR",
        clickhouse_table=tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE,
        duckdb_table=ITR_STATEMENT_ROWS_TABLE,
        years=_partition_years_for_family("itr"),
    )


@dg.asset_check(
    asset="brazil_fin_cvm_dfp_statement_rows_clickhouse",
    name="dfp_financial_metrics_present",
)
def brazil_fin_cvm_dfp_financial_metrics_present(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return financial_metrics_dataset_check(
        clickhouse=clickhouse,
        source_dataset="DFP",
    )


@dg.asset_check(
    asset="brazil_fin_cvm_itr_statement_rows_clickhouse",
    name="itr_financial_metrics_present",
)
def brazil_fin_cvm_itr_financial_metrics_present(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    return financial_metrics_dataset_check(
        clickhouse=clickhouse,
        source_dataset="ITR",
    )


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
        brazil_fin_cvm_companies_raw_csv_s3,
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        brazil_fin_cvm_dfp_documents_clickhouse,
        brazil_fin_cvm_dfp_statement_rows_clickhouse,
        brazil_fin_cvm_dfp_capital_composition_clickhouse,
        brazil_fin_cvm_dfp_auditor_reports_clickhouse,
        brazil_fin_cvm_itr_raw_archives_s3,
        brazil_fin_cvm_itr_raw_duckdb,
        brazil_fin_cvm_itr_statement_rows_usd_duckdb,
        brazil_fin_cvm_itr_documents_clickhouse,
        brazil_fin_cvm_itr_statement_rows_clickhouse,
        brazil_fin_cvm_itr_capital_composition_clickhouse,
        brazil_fin_cvm_itr_auditor_reports_clickhouse,
    ],
    asset_checks=[
        brazil_fin_cvm_dfp_statement_rows_usd_complete,
        brazil_fin_cvm_itr_statement_rows_usd_complete,
        brazil_fin_cvm_dfp_statement_rows_row_count_matches_duckdb,
        brazil_fin_cvm_itr_statement_rows_row_count_matches_duckdb,
        brazil_fin_cvm_dfp_financial_metrics_present,
        brazil_fin_cvm_itr_financial_metrics_present,
    ],
    jobs=[brazil_fin_cvm_dfp_raw_backfill_job, brazil_fin_cvm_itr_raw_backfill_job],
    resources={
        "brazil_fin_cvm_dfp": BrazilCvmDfpResource(),
        "brazil_fin_cvm_itr": BrazilCvmItrResource(),
        "brazil_fin_cvm_duckdb": duckdb_resource(BRAZIL_FIN_CVM_COMPANIES_DUCKDB_PATH),
    },
)
