from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
from dagster_clickhouse import ClickhouseResource

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
    export_sweden_financial_facts_clickhouse,
    export_sweden_financial_reports_clickhouse,
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
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialReportsResource
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS
from dagster_v3.defs.sweden_financial.storage import (
    existing_sweden_financial_source_duckdb_paths,
    sweden_financial_read_only_partitioned_connection,
)

GROUP_NAME = "sweden_financial"
SWEDEN_FINANCIAL_CURRENT_DUCKDB_POOL = "sweden_financial_current_2026_duckdb"


SWEDEN_FINANCIAL_BACKFILL_YEARS = tuple(str(year) for year in range(2020, 2027))
SWEDEN_FINANCIAL_CURRENT_YEAR = "2026"
SWEDEN_FINANCIAL_CURRENT_START_DATE = date(2026, 7, 4)
SWEDEN_FINANCIAL_CURRENT_END_DATE = date(2027, 1, 1)
SWEDEN_FINANCIAL_TIMEZONE = "Europe/Belgrade"
SWEDEN_FINANCIAL_BACKFILL_PARTITIONS = dg.StaticPartitionsDefinition(
    list(SWEDEN_FINANCIAL_BACKFILL_YEARS)
)
SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS = tuple(
    (SWEDEN_FINANCIAL_CURRENT_START_DATE + timedelta(days=days)).isoformat()
    for days in range(
        0,
        (SWEDEN_FINANCIAL_CURRENT_END_DATE - SWEDEN_FINANCIAL_CURRENT_START_DATE).days,
        7,
    )
)
SWEDEN_FINANCIAL_CURRENT_PARTITIONS = dg.StaticPartitionsDefinition(
    list(SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS)
)


def _sync_raw_archives(
    *,
    context: dg.AssetExecutionContext,
    sweden_financial_reports: SwedenFinancialReportsResource,
    object_store: ObjectStoreResource,
    sync_kind: str,
    archive_year: str,
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
        load_partition_key=context.partition_key,
    )
    return dg.MaterializeResult(
        metadata={
            **sync_result.metadata,
            "archive_year": archive_year,
            "archive_sync_manifest_key": manifest_key,
            "sync_kind": sync_kind,
            "load_partition_key": context.partition_key,
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
    )


@dg.asset(
    deps=["sweden_financial_backfill_raw_archives_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
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
    partitions_def=SWEDEN_FINANCIAL_CURRENT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
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
    )


@dg.asset(
    deps=["sweden_financial_current_raw_archives_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_CURRENT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_CURRENT_DUCKDB_POOL,
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
        load_partition_key=context.partition_key,
    )
    with duckdb_resource(duckdb_path).get_connection() as connection:
        record_sweden_financial_archive_sync(
            connection=connection,
            sync_result=sync_result,
            sync_kind="current",
            source_run_id=context.run_id,
            load_partition_key=context.partition_key,
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
            "load_partition_key": context.partition_key,
            "duckdb_path": str(duckdb_path),
        }
    )


@dg.asset(
    deps=["sweden_financial_backfill_report_xhtml_catalog_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
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
    partitions_def=SWEDEN_FINANCIAL_CURRENT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=SWEDEN_FINANCIAL_CURRENT_DUCKDB_POOL,
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
            "load_partition_key": context.partition_key,
        }
    )


SWEDEN_FINANCIAL_CLICKHOUSE_DEPENDENCIES = [
    dg.AssetDep(
        dg.AssetKey("sweden_financial_backfill_parsed_reports_duckdb"),
        partition_mapping=dg.AllPartitionMapping(),
    ),
    dg.AssetDep(
        dg.AssetKey("sweden_financial_current_parsed_reports_duckdb"),
        partition_mapping=dg.AllPartitionMapping(),
    ),
]


@dg.asset(
    deps=SWEDEN_FINANCIAL_CLICKHOUSE_DEPENDENCIES,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE},
    description="Exports parsed Sweden financial report documents to ClickHouse.",
)
def sweden_financial_reports_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    years = SWEDEN_FINANCIAL_BACKFILL_PARTITIONS.get_partition_keys()
    duckdb_paths = existing_sweden_financial_source_duckdb_paths(years=years)
    with sweden_financial_read_only_partitioned_connection(
        years=years,
        table_names=("reports",),
    ) as connection:
        rows = export_sweden_financial_reports_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": rows,
            "se_financial_reports_row_count": rows,
            "duckdb_table": "sweden_financial.reports",
            "duckdb_paths": [str(path) for path in duckdb_paths],
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
        }
    )


@dg.asset(
    deps=SWEDEN_FINANCIAL_CLICKHOUSE_DEPENDENCIES,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE},
    description="Exports parsed Sweden financial inline-XBRL facts to ClickHouse.",
)
def sweden_financial_facts_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    years = SWEDEN_FINANCIAL_BACKFILL_PARTITIONS.get_partition_keys()
    duckdb_paths = existing_sweden_financial_source_duckdb_paths(years=years)
    with sweden_financial_read_only_partitioned_connection(
        years=years,
        table_names=("facts",),
    ) as connection:
        rows = export_sweden_financial_facts_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": rows,
            "se_financial_facts_row_count": rows,
            "duckdb_table": "sweden_financial.facts",
            "duckdb_paths": [str(path) for path in duckdb_paths],
            "clickhouse_table": QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
        }
    )


@dg.asset(
    deps=[
        "sweden_financial_reports_clickhouse",
        "sweden_financial_facts_clickhouse",
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
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts = replace_sweden_financial_metrics_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        "sweden_financial_reports_clickhouse",
        "sweden_financial_facts_clickhouse",
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
    asset="sweden_financial_reports_clickhouse",
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
)
SWEDEN_FINANCIAL_CURRENT_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_current_raw_archives_s3",
    "sweden_financial_current_report_xhtml_catalog_duckdb",
    "sweden_financial_current_parsed_reports_duckdb",
)
SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_reports_clickhouse",
    "sweden_financial_facts_clickhouse",
    "sweden_financial_metrics_clickhouse",
    "se_financial_history_clickhouse",
)


sweden_financial_backfill_job = dg.define_asset_job(
    "sweden_financial_backfill_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=SWEDEN_FINANCIAL_BACKFILL_SELECTION,
)

sweden_financial_current_year_job = dg.define_asset_job(
    "sweden_financial_current_year_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=SWEDEN_FINANCIAL_CURRENT_SELECTION,
)

sweden_financial_clickhouse_job = dg.define_asset_job(
    "sweden_financial_clickhouse_job",
    selection=SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION,
)


def _current_year_run_request(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    if context.scheduled_execution_time is None:
        partition_key = SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS[0]
    else:
        partition_date = context.scheduled_execution_time.astimezone(
            ZoneInfo(SWEDEN_FINANCIAL_TIMEZONE)
        ).date()
        partition_key = partition_date.isoformat()
    if partition_key not in SWEDEN_FINANCIAL_CURRENT_PARTITION_KEYS:
        return dg.SkipReason(
            f"No Sweden financial current partition for schedule date {partition_key}"
        )
    return dg.RunRequest(partition_key=partition_key)


sweden_financial_current_year_weekly = dg.ScheduleDefinition(
    name="sweden_financial_current_year_weekly",
    job=sweden_financial_current_year_job,
    cron_schedule="45 6 * * 6",
    execution_timezone=SWEDEN_FINANCIAL_TIMEZONE,
    execution_fn=_current_year_run_request,
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
        sweden_financial_reports_clickhouse,
        sweden_financial_facts_clickhouse,
        sweden_financial_metrics_clickhouse,
        se_financial_history_clickhouse,
    ],
    asset_checks=[archive_ingest_complete],
    jobs=[
        sweden_financial_backfill_job,
        sweden_financial_current_year_job,
        sweden_financial_clickhouse_job,
    ],
    schedules=[sweden_financial_current_year_weekly],
    resources={
        "sweden_financial_reports": SwedenFinancialReportsResource(),
    },
)
