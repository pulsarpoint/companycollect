from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.archive_state import (
    changed_sweden_financial_archive_keys_for_run,
    read_sweden_financial_archive_sync_manifest,
    record_sweden_financial_archive_sync,
    write_sweden_financial_archive_sync_manifest,
)
from dagster_v3.defs.sweden_financial.parsing import (
    extract_sweden_financial_report_xhtml_catalog,
    sweden_financial_source_duckdb_path,
)
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialReportsResource

GROUP_NAME = "sweden_financial"
SWEDEN_FINANCIAL_DUCKDB_POOL = "sweden_financial_duckdb"


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
    partitions_def=SWEDEN_FINANCIAL_CURRENT_PARTITIONS,
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


SWEDEN_FINANCIAL_BACKFILL_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_backfill_raw_archives_s3",
    "sweden_financial_backfill_report_xhtml_catalog_duckdb",
)
SWEDEN_FINANCIAL_CURRENT_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_current_raw_archives_s3",
    "sweden_financial_current_report_xhtml_catalog_duckdb",
)


sweden_financial_backfill_job = dg.define_asset_job(
    "sweden_financial_backfill_job",
    selection=SWEDEN_FINANCIAL_BACKFILL_SELECTION,
)

sweden_financial_current_year_job = dg.define_asset_job(
    "sweden_financial_current_year_job",
    selection=SWEDEN_FINANCIAL_CURRENT_SELECTION,
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
        sweden_financial_current_raw_archives_s3,
        sweden_financial_current_report_xhtml_catalog_duckdb,
    ],
    jobs=[
        sweden_financial_backfill_job,
        sweden_financial_current_year_job,
    ],
    schedules=[sweden_financial_current_year_weekly],
    resources={
        "sweden_financial_reports": SwedenFinancialReportsResource(),
    },
)
