import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from exchange_rates import ExchangeRateClient
from dagster_v3.defs.norway_brreg_financial.annual_account_clickhouse import (
    FACT_COLUMNS,
    FACTS_TABLE,
    METRIC_COLUMNS,
    METRICS_TABLE,
    REPORT_COLUMNS,
    REPORTS_TABLE,
    publish_annual_account_partition,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_financials import (
    ANNUAL_ACCOUNT_DATASET,
    apply_annual_account_usd_conversion,
    apply_llm_concept_mappings,
    build_annual_account_metrics,
    llm_settings,
    load_annual_account_documents,
    replace_annual_account_facts,
)
from dagster_v3.defs.norway_brreg_financial.assets.annual_accounts import (
    NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    _partition_values,
)
from dagster_v3.defs.norway_brreg_financial.constants import GROUP_NAME
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)

ANNUAL_ACCOUNT_DUCKDB_POOL = "norway_brreg_annual_accounts_duckdb"
ANNUAL_ACCOUNT_DUCKDB_PATH = "data/norway_brreg_annual_accounts.duckdb"


class AnnualAccountLlmMappingConfig(dg.Config):
    batch_size: int = 2
    workers: int = 2
    timeout_seconds: int = 120


@dg.asset(
    name="norway_brreg_annual_account_documents_duckdb",
    deps=[dg.AssetKey("norway_brreg_annual_account_documents_json")],
    group_name=GROUP_NAME,
    kinds={"python", "json", "s3", "duckdb"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Validates all processed annual-account JSON objects for one year/chunk "
        "and stores their document catalog in the Norway annual-account DuckDB."
    ),
)
def norway_brreg_annual_account_documents_duckdb(
    context: AssetExecutionContext,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    with norway_brreg_annual_accounts_duckdb.get_connection() as connection:
        counts = load_annual_account_documents(
            connection=connection,
            storage=norway_brreg_financial_storage,
            filing_year=filing_year,
            chunk_key=chunk_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={"filing_year": filing_year, "chunk_key": chunk_key, **counts}
    )


@dg.asset(
    name="norway_brreg_annual_account_facts_duckdb",
    deps=[dg.AssetKey("norway_brreg_annual_account_documents_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "ocr", "duckdb"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Reconstructs every numeric annual-account table row from OCR word geometry. "
        "The LLM is not allowed to create or alter numeric values."
    ),
)
def norway_brreg_annual_account_facts_duckdb(
    context: AssetExecutionContext,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    with norway_brreg_annual_accounts_duckdb.get_connection() as connection:
        counts = replace_annual_account_facts(
            connection=connection,
            storage=norway_brreg_financial_storage,
            filing_year=filing_year,
            chunk_key=chunk_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={"filing_year": filing_year, "chunk_key": chunk_key, **counts}
    )


@dg.asset(
    name="norway_brreg_annual_account_fact_mappings_duckdb",
    deps=[dg.AssetKey("norway_brreg_annual_account_facts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "llm", "duckdb"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Applies deterministic Norwegian accounting mappings, then asks the local "
        "Qwen model to preserve remaining meaningful labels as core or extended concepts."
    ),
)
def norway_brreg_annual_account_fact_mappings_duckdb(
    context: AssetExecutionContext,
    config: AnnualAccountLlmMappingConfig,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    base_url, model, api_key = llm_settings()
    with norway_brreg_annual_accounts_duckdb.get_connection() as connection:
        counts = apply_llm_concept_mappings(
            connection=connection,
            filing_year=filing_year,
            chunk_key=chunk_key,
            base_url=base_url,
            api_key=api_key,
            model=model,
            batch_size=config.batch_size,
            workers=config.workers,
            timeout_seconds=config.timeout_seconds,
        )
    return dg.MaterializeResult(
        metadata={
            "filing_year": filing_year,
            "chunk_key": chunk_key,
            "llm_model": model,
            **counts,
        }
    )


@dg.asset(
    name="norway_brreg_annual_account_facts_usd_duckdb",
    deps=[dg.AssetKey("norway_brreg_annual_account_facts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "fx"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Converts every available unconverted annual-account monetary fact to USD "
        "and records the exact rate, date, and provider."
    ),
)
def norway_brreg_annual_account_facts_usd_duckdb(
    context: AssetExecutionContext,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    with norway_brreg_annual_accounts_duckdb.get_connection() as connection:
        counts = apply_annual_account_usd_conversion(
            connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            filing_year=filing_year,
            chunk_key=chunk_key,
        )
    return dg.MaterializeResult(
        metadata={"filing_year": filing_year, "chunk_key": chunk_key, **counts}
    )


@dg.asset(
    name="norway_brreg_annual_account_metrics_duckdb",
    deps=[
        dg.AssetKey("norway_brreg_annual_account_fact_mappings_duckdb"),
        dg.AssetKey("norway_brreg_annual_account_facts_usd_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Builds one validated native/USD canonical metric row per document and "
        "reported year while preserving every source fact separately."
    ),
)
def norway_brreg_annual_account_metrics_duckdb(
    context: AssetExecutionContext,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    with norway_brreg_annual_accounts_duckdb.get_connection() as connection:
        counts = build_annual_account_metrics(
            connection=connection,
            filing_year=filing_year,
            chunk_key=chunk_key,
            source_run_id=context.run_id,
        )
    return dg.MaterializeResult(
        metadata={"filing_year": filing_year, "chunk_key": chunk_key, **counts}
    )


@dg.asset(
    name="norway_brreg_annual_account_reports_clickhouse",
    deps=[dg.AssetKey("norway_brreg_annual_account_documents_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Atomically replaces one Norway annual-account year/chunk partition in "
        "corpscout.no_financial_reports."
    ),
)
def norway_brreg_annual_account_reports_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _publish_clickhouse_asset(
        context,
        clickhouse=clickhouse,
        duckdb_resource=norway_brreg_annual_accounts_duckdb,
        duckdb_table="documents",
        clickhouse_table=REPORTS_TABLE,
        columns=REPORT_COLUMNS,
    )


@dg.asset(
    name="norway_brreg_annual_account_facts_clickhouse",
    deps=[
        dg.AssetKey("norway_brreg_annual_account_fact_mappings_duckdb"),
        dg.AssetKey("norway_brreg_annual_account_facts_usd_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Atomically replaces one mapped and USD-enriched Norway annual-account "
        "year/chunk partition in corpscout.no_financial_facts."
    ),
)
def norway_brreg_annual_account_facts_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _publish_clickhouse_asset(
        context,
        clickhouse=clickhouse,
        duckdb_resource=norway_brreg_annual_accounts_duckdb,
        duckdb_table="facts",
        clickhouse_table=FACTS_TABLE,
        columns=FACT_COLUMNS,
    )


@dg.asset(
    name="norway_brreg_annual_account_metrics_clickhouse",
    deps=[dg.AssetKey("norway_brreg_annual_account_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ANNUAL_ACCOUNT_DUCKDB_POOL,
    description=(
        "Atomically replaces one Norway annual-account year/chunk partition in "
        "corpscout.no_financial_metrics."
    ),
)
def norway_brreg_annual_account_metrics_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_annual_accounts_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _publish_clickhouse_asset(
        context,
        clickhouse=clickhouse,
        duckdb_resource=norway_brreg_annual_accounts_duckdb,
        duckdb_table="metrics",
        clickhouse_table=METRICS_TABLE,
        columns=METRIC_COLUMNS,
    )


def _publish_clickhouse_asset(
    context: AssetExecutionContext,
    *,
    clickhouse: ClickhouseResource,
    duckdb_resource: DuckDBResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    with duckdb_resource.get_connection() as connection:
        row_count = publish_annual_account_partition(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            duckdb_table=duckdb_table,
            clickhouse_table=clickhouse_table,
            columns=columns,
            filing_year=filing_year,
            chunk_key=chunk_key,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "filing_year": filing_year,
            "chunk_key": chunk_key,
            "duckdb_schema": ANNUAL_ACCOUNT_DATASET,
            "clickhouse_table": f"corpscout.{clickhouse_table}",
            "row_count": row_count,
        }
    )
