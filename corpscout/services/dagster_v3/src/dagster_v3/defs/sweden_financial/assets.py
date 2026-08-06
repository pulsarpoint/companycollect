from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from exchange_rates import ExchangeRateClient

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.archive_state import (
    changed_sweden_financial_archive_keys_for_run,
    read_sweden_financial_archive_sync_manifest,
    record_sweden_financial_archive_sync,
    write_sweden_financial_archive_sync_manifest,
)
from dagster_v3.defs.sweden_financial.clickhouse import (
    QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
    QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
    reconcile_sweden_financial_facts_clickhouse,
    reconcile_sweden_financial_reports_clickhouse,
    upsert_sweden_financial_facts_partition,
    upsert_sweden_financial_reports_partition,
)
from dagster_v3.defs.sweden_financial.history import (
    QUALIFIED_SE_FINANCIAL_HISTORY_TABLE,
    replace_se_financial_history_clickhouse,
)
from dagster_v3.defs.sweden_financial.parsing import (
    extract_sweden_financial_report_xhtml_catalog,
    parse_sweden_financial_report_xhtml_catalog,
    sweden_financial_source_duckdb_path,
)
from dagster_v3.defs.sweden_financial.metrics import (
    QUALIFIED_SE_FINANCIAL_METRICS_TABLE,
    replace_sweden_financial_metrics_clickhouse,
)
from dagster_v3.defs.sweden_financial.officers import (
    QUALIFIED_SE_COMPANY_OFFICERS_TABLE,
    replace_se_company_officers_clickhouse,
)
from dagster_v3.defs.sweden_financial.audits import (
    QUALIFIED_SE_COMPANY_AUDITS_TABLE,
    replace_se_company_audits_clickhouse,
)
from dagster_v3.defs.sweden_financial.concepts import (
    se_financial_facts_concepts,
    se_financial_taxonomy_concepts,
    se_financial_taxonomy_official_translations,
    sweden_financial_taxonomy_translation_coverage,
    sweden_financial_taxonomy_translation_load,
)
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialReportsResource
from dagster_v3.defs.sweden_financial.storage import (
    sweden_financial_year_duckdb_connection,
    sweden_financial_year_duckdb_write_connection,
)
from dagster_v3.defs.sweden_financial.usd_conversion import (
    apply_sweden_financial_facts_usd_conversion,
)

GROUP_NAME = "sweden_financial"
# ONE pool for EVERY asset that opens ANY Sweden year DuckDB file (backfill
# catalog/parse/exports AND the whole weekly chain's DuckDB steps). The
# instance defaults every pool to limit 1, so Dagster serializes these
# steps across runs -- weekly and yearly chains can be launched in any
# order AND in parallel; steps interleave instead of colliding on the
# DuckDB cross-process file lock. (Cost: two backfill years cannot parse
# concurrently -- acceptable; correctness over parallelism.)
SWEDEN_FINANCIAL_DUCKDB_POOL = "sweden_financial_duckdb"


SWEDEN_FINANCIAL_BACKFILL_YEARS = tuple(str(year) for year in range(2020, 2027))
SWEDEN_FINANCIAL_CURRENT_YEAR = "2026"
SWEDEN_FINANCIAL_TIMEZONE = "Europe/Belgrade"
SWEDEN_FINANCIAL_BACKFILL_PARTITIONS = dg.StaticPartitionsDefinition(
    list(SWEDEN_FINANCIAL_BACKFILL_YEARS)
)
# The current (weekly refresh) chain is deliberately UNPARTITIONED
# (2026-07-20 order-independence design): weekly partition identities
# existed only to give each week's export a bookkeeping scope, and that
# bookkeeping was exactly what a yearly re-parse destroyed (the 2026-07-18
# incident). The current sync/catalog manifest slot uses this constant key.
SWEDEN_FINANCIAL_CURRENT_MANIFEST_KEY = "current"


def _sync_raw_archives(
    *,
    context: dg.AssetExecutionContext,
    sweden_financial_reports: SwedenFinancialReportsResource,
    object_store: ObjectStoreResource,
    sync_kind: str,
    archive_year: str,
    load_partition_key: str,
) -> dg.MaterializeResult:
    started_at = datetime.now(UTC)
    sync_result = sweden_financial_reports.sync_raw_archives(
        object_store=object_store,
        year=archive_year,
        log_info=context.log.info,
    )
    manifest_key = write_sweden_financial_archive_sync_manifest(
        object_store=object_store,
        sync_result=sync_result,
        sync_kind=sync_kind,
        source_run_id=context.run_id,
        load_partition_key=load_partition_key,
    )
    return dg.MaterializeResult(
        metadata={
            **sync_result.metadata,
            "archive_year": archive_year,
            "archive_sync_manifest_key": manifest_key,
            "sync_kind": sync_kind,
            "load_partition_key": load_partition_key,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
        }
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "bolagsverket", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description="Downloads Sweden annual-report outer ZIP archives for 2020-2026 backfill.",
)
def sweden_financial_backfill_raw_archives_s3(
    context: dg.AssetExecutionContext,
    sweden_financial_reports: SwedenFinancialReportsResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _sync_raw_archives(
        context=context,
        sweden_financial_reports=sweden_financial_reports,
        object_store=object_store,
        sync_kind="backfill",
        archive_year=context.partition_key,
        load_partition_key=context.partition_key,
    )


@dg.asset(
    deps=["sweden_financial_backfill_raw_archives_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description="Extracts Sweden backfill report XHTML files and replaces the year catalog.",
)
def sweden_financial_backfill_report_xhtml_catalog_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(context.partition_key)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    sync_result = read_sweden_financial_archive_sync_manifest(
        object_store=object_store,
        sync_kind="backfill",
        load_partition_key=context.partition_key,
    )
    with duckdb_resource(duckdb_path).get_connection() as connection:
        record_sweden_financial_archive_sync(
            connection=connection,
            sync_result=sync_result,
            sync_kind="backfill",
            source_run_id=context.run_id,
            load_partition_key=context.partition_key,
        )
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            partition_year=context.partition_key,
            replace_scope="partition",
            log_info=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "bolagsverket", "xbrl"},
    description="Checks 2026 Sweden annual-report ZIP archives every 7 days.",
)
def sweden_financial_current_raw_archives_s3(
    context: dg.AssetExecutionContext,
    sweden_financial_reports: SwedenFinancialReportsResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _sync_raw_archives(
        context=context,
        sweden_financial_reports=sweden_financial_reports,
        object_store=object_store,
        sync_kind="current",
        archive_year=SWEDEN_FINANCIAL_CURRENT_YEAR,
        load_partition_key=SWEDEN_FINANCIAL_CURRENT_MANIFEST_KEY,
    )


@dg.asset(
    deps=["sweden_financial_current_raw_archives_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "xhtml", "xbrl"},
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description="Extracts changed 2026 Sweden report XHTML archives for current refreshes.",
)
def sweden_financial_current_report_xhtml_catalog_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(SWEDEN_FINANCIAL_CURRENT_YEAR)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    sync_result = read_sweden_financial_archive_sync_manifest(
        object_store=object_store,
        sync_kind="current",
        load_partition_key=SWEDEN_FINANCIAL_CURRENT_MANIFEST_KEY,
    )
    with duckdb_resource(duckdb_path).get_connection() as connection:
        record_sweden_financial_archive_sync(
            connection=connection,
            sync_result=sync_result,
            sync_kind="current",
            source_run_id=context.run_id,
            load_partition_key=SWEDEN_FINANCIAL_CURRENT_MANIFEST_KEY,
        )
        changed_keys = changed_sweden_financial_archive_keys_for_run(
            connection=connection,
            source_run_id=context.run_id,
        )
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            partition_year=SWEDEN_FINANCIAL_CURRENT_YEAR,
            source_archive_keys=changed_keys,
            replace_scope="archive",
            log_info=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "changed_source_archive_count": len(changed_keys),
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    deps=["sweden_financial_backfill_report_xhtml_catalog_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description=(
        "Parses Sweden backfill XHTML/iXBRL reports into structured report and "
        "fact tables in the year DuckDB file."
    ),
)
def sweden_financial_backfill_parsed_reports_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(context.partition_key)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb_resource(duckdb_path).get_connection() as connection:
        counts = parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            partition_year=context.partition_key,
            replace_scope="partition",
            log_info=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    deps=["sweden_financial_current_report_xhtml_catalog_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "xhtml", "xbrl"},
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description=(
        "Parses changed Sweden current-year XHTML/iXBRL reports into structured "
        "report and fact tables in the active-year DuckDB file."
    ),
)
def sweden_financial_current_parsed_reports_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(SWEDEN_FINANCIAL_CURRENT_YEAR)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb_resource(duckdb_path).get_connection() as connection:
        counts = parse_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            partition_year=SWEDEN_FINANCIAL_CURRENT_YEAR,
            replace_scope="archive",
            catalog_source_run_id=context.run_id,
            log_info=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_path": str(duckdb_path),
        }
    )


def _sweden_financial_facts_usd_result(
    *,
    context: dg.AssetExecutionContext,
    year: str,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(year)
    with sweden_financial_year_duckdb_write_connection(year) as connection:
        counts = apply_sweden_financial_facts_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_table": "sweden_financial.facts",
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    deps=[
        "sweden_financial_backfill_parsed_reports_duckdb",
        "exchange_rates_v2_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl", "currency", "fx"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description=(
        "Converts every currency-bearing Sweden backfill fact to USD in the "
        "year DuckDB file, using the filing report-period end date."
    ),
)
def sweden_financial_backfill_facts_usd_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    return _sweden_financial_facts_usd_result(
        context=context,
        year=context.partition_key,
    )


@dg.asset(
    deps=[
        "sweden_financial_current_parsed_reports_duckdb",
        "exchange_rates_v2_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl", "currency", "fx"},
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description=(
        "Converts every currency-bearing Sweden current-year fact to USD in "
        "the active-year DuckDB file, using the filing report-period end date."
    ),
)
def sweden_financial_current_facts_usd_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    return _sweden_financial_facts_usd_result(
        context=context,
        year=SWEDEN_FINANCIAL_CURRENT_YEAR,
    )


class SwedenFinancialClickhouseExportConfig(dg.Config):
    # Shrink-guard override (see clickhouse.py's
    # guard_against_clickhouse_table_shrink) -- MUST stay False by default.
    # Only set True via explicit run config for a confirmed-intentional
    # shrink of a populated se_financial_metrics/officers/audits table (e.g.
    # a deliberate upstream data retirement), never as a standing default.
    # The reports/facts exports no longer take it: they are partition-scoped
    # upserts that structurally cannot shrink the table beyond their own
    # partition's scope.
    allow_shrink: bool = False


def _upsert_reports_partition_result(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    year: str,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(year)
    with sweden_financial_year_duckdb_connection(year) as connection:
        metadata = upsert_sweden_financial_reports_partition(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            partition_key=context.partition_key,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_table": "sweden_financial.reports",
            "duckdb_path": str(duckdb_path),
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
        }
    )


def _upsert_facts_partition_result(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    year: str,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(year)
    with sweden_financial_year_duckdb_connection(year) as connection:
        metadata = upsert_sweden_financial_facts_partition(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            partition_key=context.partition_key,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_table": "sweden_financial.facts",
            "duckdb_path": str(duckdb_path),
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
        }
    )


@dg.asset(
    deps=["sweden_financial_backfill_parsed_reports_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    metadata={"table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE},
    description=(
        "Upserts one backfill year of parsed Sweden financial report "
        "documents into ClickHouse: deletes exactly its own partition's "
        "source_archive_key scope, then inserts -- never a full-table "
        "replace, so a host can never delete years it does not hold."
    ),
)
def sweden_financial_backfill_reports_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _upsert_reports_partition_result(
        context=context,
        clickhouse=clickhouse,
        year=context.partition_key,
    )


@dg.asset(
    deps=["sweden_financial_current_parsed_reports_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    metadata={"table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE},
    description=(
        "Reconciles the active-year Sweden financial reports into "
        "ClickHouse: diffs the local year DuckDB against the target per "
        "source_archive_key and upserts exactly the missing/mismatched "
        "archives. Stateless -- safe to run in any order relative to the "
        "yearly backfill."
    ),
)
def sweden_financial_current_reports_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(SWEDEN_FINANCIAL_CURRENT_YEAR)
    with sweden_financial_year_duckdb_connection(
        SWEDEN_FINANCIAL_CURRENT_YEAR
    ) as connection:
        metadata = reconcile_sweden_financial_reports_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_table": "sweden_financial.reports",
            "duckdb_path": str(duckdb_path),
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=["sweden_financial_backfill_facts_usd_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    metadata={"table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE},
    description=(
        "Upserts one backfill year of parsed Sweden financial inline-XBRL "
        "facts into ClickHouse, scoped by the partition's statement keys -- "
        "never a full-table replace."
    ),
)
def sweden_financial_backfill_facts_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _upsert_facts_partition_result(
        context=context,
        clickhouse=clickhouse,
        year=context.partition_key,
    )


@dg.asset(
    deps=["sweden_financial_current_facts_usd_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    metadata={"table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE},
    description=(
        "Reconciles the active-year Sweden financial inline-XBRL facts "
        "into ClickHouse: diffs facts counts per archive (via "
        "statement_key) and upserts exactly the missing/mismatched "
        "archives. Stateless -- safe to run in any order relative to the "
        "yearly backfill."
    ),
)
def sweden_financial_current_facts_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    duckdb_path = sweden_financial_source_duckdb_path(SWEDEN_FINANCIAL_CURRENT_YEAR)
    with sweden_financial_year_duckdb_connection(
        SWEDEN_FINANCIAL_CURRENT_YEAR
    ) as connection:
        metadata = reconcile_sweden_financial_facts_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_table": "sweden_financial.facts",
            "duckdb_path": str(duckdb_path),
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
        }
    )


def _all_partition_deps(*asset_keys: str) -> list[dg.AssetDep]:
    """Deps of an unpartitioned derived asset on partitioned exports."""
    return [
        dg.AssetDep(
            dg.AssetKey(asset_key),
            partition_mapping=dg.AllPartitionMapping(),
        )
        for asset_key in asset_keys
    ]


# Derived-asset deps: the backfill exports are year-partitioned (need
# AllPartitionMapping); the current exports are unpartitioned reconcilers
# (plain deps).
SWEDEN_FINANCIAL_EXPORT_DEPS = [
    *_all_partition_deps(
        "sweden_financial_backfill_reports_clickhouse",
        "sweden_financial_backfill_facts_clickhouse",
    ),
    "sweden_financial_current_reports_clickhouse",
    "sweden_financial_current_facts_clickhouse",
]
SWEDEN_FINANCIAL_FACTS_EXPORT_DEPS = [
    *_all_partition_deps("sweden_financial_backfill_facts_clickhouse"),
    "sweden_financial_current_facts_clickhouse",
]


@dg.asset(
    deps=[
        *SWEDEN_FINANCIAL_EXPORT_DEPS,
        dg.AssetDep(
            dg.AssetKey("exchange_rates_v2_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl", "fx"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_METRICS_TABLE},
    description=(
        "Builds canonical Sweden filing-level metrics from every published XBRL "
        "fact, converts SEK to USD, and preserves exact source-document links."
    ),
)
def sweden_financial_metrics_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenFinancialClickhouseExportConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts = replace_sweden_financial_metrics_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        *SWEDEN_FINANCIAL_EXPORT_DEPS,
        # Not a data dependency (the history build reads reports/facts/
        # exchange_rates directly, not se_financial_metrics) -- an ordering
        # dependency so history rebuilds land in the same wave as metrics
        # (both derive from the same reports+facts and are consumed
        # together downstream).
        "sweden_financial_metrics_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl", "fx"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_HISTORY_TABLE},
    description=(
        "Builds per-(company, fiscal_year) Sweden financial history rows -- "
        "each filing's own reported figures plus its flerarsoversikt "
        "comparative-year figures, trust-guarded on revenue overlap "
        "agreement -- from se_financial_reports and se_financial_facts."
    ),
)
def se_financial_history_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_se_financial_history_clickhouse(
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    deps=SWEDEN_FINANCIAL_FACTS_EXPORT_DEPS,
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_COMPANY_OFFICERS_TABLE},
    description=(
        "Extracts Swedish company officers (board members, CEO, auditors) "
        "from XBRL signature-block facts in se_financial_facts."
    ),
)
def se_company_officers_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenFinancialClickhouseExportConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_se_company_officers_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    deps=SWEDEN_FINANCIAL_FACTS_EXPORT_DEPS,
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_COMPANY_AUDITS_TABLE},
    description=(
        "Extracts Swedish company audit firm and audit-opinion form "
        "(standard/modified/unknown) from XBRL facts in se_financial_facts."
    ),
)
def se_company_audits_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenFinancialClickhouseExportConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_se_company_audits_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    return dg.MaterializeResult(metadata=metadata)


SWEDEN_FINANCIAL_ARCHIVE_INGEST_GAP_TOLERANCE = 6


def sweden_financial_processed_archive_counts_by_year(
    clickhouse: ClickhouseResource,
) -> dict[str, int]:
    """Count distinct processed source archives per year in ClickHouse."""
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT
                extract(source_archive_key, 'year=([^/]+)') AS archive_year,
                uniqExact(source_archive_key) AS processed_count
            FROM {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE}
            GROUP BY archive_year
            """
        )
    return {str(year): int(count) for year, count in rows}


def sweden_financial_archive_ingest_gap_result(
    *,
    upstream_counts: dict[str, int],
    processed_counts: dict[str, int],
    gap_tolerance: int = SWEDEN_FINANCIAL_ARCHIVE_INGEST_GAP_TOLERANCE,
) -> dg.AssetCheckResult:
    """Build the check result comparing upstream vs. processed archives per year.

    2026-07-18: the yearly backfill partitions ran once and the "current"
    weekly partitions only start 2026-07-04, so 216 of 241 upstream 2026
    archives were silently never ingested -- a seam with no guard. Fails
    when any year's processed count lags the upstream listing by more than
    ``gap_tolerance`` archives (tolerance for weekly cadence + in-flight
    syncs).
    """
    years = sorted(set(upstream_counts) | set(processed_counts))
    per_year = {
        year: {
            "upstream": upstream_counts.get(year, 0),
            "processed": processed_counts.get(year, 0),
            "gap": upstream_counts.get(year, 0) - processed_counts.get(year, 0),
        }
        for year in years
    }
    max_gap = max((entry["gap"] for entry in per_year.values()), default=0)
    return dg.AssetCheckResult(
        passed=max_gap <= gap_tolerance,
        metadata={
            "per_year": dg.MetadataValue.json(per_year),
            "max_gap": dg.MetadataValue.int(max_gap),
            "gap_tolerance": dg.MetadataValue.int(gap_tolerance),
        },
    )


@dg.asset_check(
    asset="sweden_financial_metrics_clickhouse",
    name="archive_ingest_complete",
)
def archive_ingest_complete(
    sweden_financial_reports: SwedenFinancialReportsResource,
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Guard against a silent upstream-archive ingest gap (see module docs).

    Lists upstream archives per year via
    ``SwedenFinancialReportsResource.count_archives_by_year`` (which reuses
    the existing paginated listing capability) and compares against distinct
    processed ``source_archive_key`` values per year in ClickHouse.

    Attached to ``sweden_financial_metrics_clickhouse`` (not the
    reports exports) because its semantics are whole-table completeness
    across ALL years: the reports exports are now partition-scoped, and a
    single-partition run must not be failed for years that simply have not
    been exported yet. The metrics rebuild is the unpartitioned derived
    asset that runs after every export wave, so the completeness question
    is well-posed there.
    """
    upstream_counts = sweden_financial_reports.count_archives_by_year()
    processed_counts = sweden_financial_processed_archive_counts_by_year(clickhouse)
    return sweden_financial_archive_ingest_gap_result(
        upstream_counts=upstream_counts,
        processed_counts=processed_counts,
    )


SWEDEN_FINANCIAL_BACKFILL_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_backfill_raw_archives_s3",
    "sweden_financial_backfill_report_xhtml_catalog_duckdb",
    "sweden_financial_backfill_parsed_reports_duckdb",
    "sweden_financial_backfill_facts_usd_duckdb",
)
# The weekly current selection is the FULL chain -- sync, catalog, parse,
# fact FX enrichment, and both ClickHouse exports -- as separate
# unpartitioned assets in one job/run. The exports are stateless reconcilers
# (diff local year file vs ClickHouse), so weekly and yearly materializations
# are order-independent by construction (the 2026-07-20 design closing the
# 2026-07-18 incident).
SWEDEN_FINANCIAL_CURRENT_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_current_raw_archives_s3",
    "sweden_financial_current_report_xhtml_catalog_duckdb",
    "sweden_financial_current_parsed_reports_duckdb",
    "sweden_financial_current_facts_usd_duckdb",
    "sweden_financial_current_reports_clickhouse",
    "sweden_financial_current_facts_clickhouse",
    "sweden_financial_company_source_records_clickhouse",
    "se_financial_facts_concepts",
    "se_financial_taxonomy_concepts",
    "se_financial_taxonomy_official_translations",
    "sweden_financial_taxonomy_translation_load",
)
# The ClickHouse layer is three jobs: the year-partitioned backfill FX/export
# chain, the unpartitioned current FX/reconciling export chain for manual
# re-runs, and the derived (unpartitioned) rebuild wave which keeps the
# historical job name.
SWEDEN_FINANCIAL_BACKFILL_CLICKHOUSE_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_backfill_facts_usd_duckdb",
    "sweden_financial_backfill_reports_clickhouse",
    "sweden_financial_backfill_facts_clickhouse",
    "sweden_financial_company_source_records_clickhouse",
)
SWEDEN_FINANCIAL_CURRENT_CLICKHOUSE_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_current_facts_usd_duckdb",
    "sweden_financial_current_reports_clickhouse",
    "sweden_financial_current_facts_clickhouse",
    "sweden_financial_company_source_records_clickhouse",
)
SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_metrics_clickhouse",
    "se_financial_history_clickhouse",
    "se_company_officers_clickhouse",
    "se_company_audits_clickhouse",
    "sweden_financial_company_source_records_clickhouse",
)
SWEDEN_FINANCIAL_CONCEPTS_SELECTION = dg.AssetSelection.assets(
    "se_financial_facts_concepts",
    "se_financial_taxonomy_concepts",
    "se_financial_taxonomy_official_translations",
    "sweden_financial_taxonomy_translation_load",
)


sweden_financial_backfill_job = dg.define_asset_job(
    "sweden_financial_backfill_job",
    selection=SWEDEN_FINANCIAL_BACKFILL_SELECTION,
)

sweden_financial_current_year_job = dg.define_asset_job(
    "sweden_financial_current_year_job",
    selection=SWEDEN_FINANCIAL_CURRENT_SELECTION,
)

sweden_financial_backfill_clickhouse_job = dg.define_asset_job(
    "sweden_financial_backfill_clickhouse_job",
    selection=SWEDEN_FINANCIAL_BACKFILL_CLICKHOUSE_SELECTION,
)

sweden_financial_current_clickhouse_job = dg.define_asset_job(
    "sweden_financial_current_clickhouse_job",
    selection=SWEDEN_FINANCIAL_CURRENT_CLICKHOUSE_SELECTION,
)

sweden_financial_clickhouse_job = dg.define_asset_job(
    "sweden_financial_clickhouse_job",
    selection=SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION,
)

sweden_financial_concepts_job = dg.define_asset_job(
    "sweden_financial_concepts_job",
    selection=SWEDEN_FINANCIAL_CONCEPTS_SELECTION,
)


sweden_financial_current_year_weekly = dg.ScheduleDefinition(
    name="sweden_financial_current_year_weekly",
    job=sweden_financial_current_year_job,
    cron_schedule="45 6 * * 6",
    execution_timezone=SWEDEN_FINANCIAL_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        sweden_financial_backfill_raw_archives_s3,
        sweden_financial_backfill_report_xhtml_catalog_duckdb,
        sweden_financial_backfill_parsed_reports_duckdb,
        sweden_financial_current_raw_archives_s3,
        sweden_financial_current_report_xhtml_catalog_duckdb,
        sweden_financial_current_parsed_reports_duckdb,
        sweden_financial_backfill_facts_usd_duckdb,
        sweden_financial_current_facts_usd_duckdb,
        sweden_financial_backfill_reports_clickhouse,
        sweden_financial_current_reports_clickhouse,
        sweden_financial_backfill_facts_clickhouse,
        sweden_financial_current_facts_clickhouse,
        sweden_financial_metrics_clickhouse,
        se_financial_history_clickhouse,
        se_company_officers_clickhouse,
        se_company_audits_clickhouse,
        se_financial_facts_concepts,
        se_financial_taxonomy_concepts,
        se_financial_taxonomy_official_translations,
        sweden_financial_taxonomy_translation_load,
    ],
    asset_checks=[
        archive_ingest_complete,
        sweden_financial_taxonomy_translation_coverage,
    ],
    jobs=[
        sweden_financial_backfill_job,
        sweden_financial_current_year_job,
        sweden_financial_backfill_clickhouse_job,
        sweden_financial_current_clickhouse_job,
        sweden_financial_clickhouse_job,
        sweden_financial_concepts_job,
    ],
    schedules=[sweden_financial_current_year_weekly],
    resources={
        "sweden_financial_reports": SwedenFinancialReportsResource(),
    },
)
