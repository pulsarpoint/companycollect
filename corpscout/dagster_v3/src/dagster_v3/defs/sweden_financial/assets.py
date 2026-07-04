from datetime import UTC, datetime

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.parsing import (
    SWEDEN_FINANCIAL_DUCKDB_PATH,
    extract_sweden_financial_report_xhtml_catalog,
)
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialReportsResource

GROUP_NAME = "sweden_financial"
SWEDEN_FINANCIAL_DUCKDB_POOL = "sweden_financial_duckdb"
SWEDEN_FINANCIAL_BACKFILL_YEARS = tuple(str(year) for year in range(2020, 2026))
SWEDEN_FINANCIAL_YEAR_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        *SWEDEN_FINANCIAL_BACKFILL_YEARS,
        *(
            [str(datetime.now(UTC).year)]
            if str(datetime.now(UTC).year) not in SWEDEN_FINANCIAL_BACKFILL_YEARS
            else []
        ),
    ]
)


def current_sweden_financial_year() -> str:
    return str(datetime.now(UTC).year)


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "bolagsverket", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_YEAR_PARTITIONS,
    description=(
        "Downloads Sweden annual-report outer ZIP archives from Bolagsverket "
        "to object storage. This asset does not extract XHTML or parse reports."
    ),
)
def sweden_financial_raw_archives_s3(
    context: dg.AssetExecutionContext,
    sweden_financial_reports: SwedenFinancialReportsResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    started_at = datetime.now(UTC)
    partition_year = context.partition_key
    result = sweden_financial_reports.download_raw_archives(
        object_store=object_store,
        year=partition_year,
        log_info=context.log.info,
    )
    metadata = dict(result.metadata or {})
    metadata["partition_year"] = partition_year
    metadata["started_at"] = started_at.isoformat()
    metadata["finished_at"] = datetime.now(UTC).isoformat()
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    deps=["sweden_financial_raw_archives_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "xhtml", "xbrl"},
    partitions_def=SWEDEN_FINANCIAL_YEAR_PARTITIONS,
    pool=SWEDEN_FINANCIAL_DUCKDB_POOL,
    description=(
        "Extracts nested annual-report XHTML files from Sweden financial raw "
        "archives into object storage and writes a DuckDB catalog."
    ),
)
def sweden_financial_report_xhtml_catalog_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    sweden_financial_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    SWEDEN_FINANCIAL_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sweden_financial_duckdb.get_connection() as connection:
        counts = extract_sweden_financial_report_xhtml_catalog(
            connection=connection,
            object_store=object_store,
            source_run_id=context.run_id,
            partition_year=context.partition_key,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "duckdb_path": str(SWEDEN_FINANCIAL_DUCKDB_PATH),
        }
    )


SWEDEN_FINANCIAL_ASSET_SELECTION = dg.AssetSelection.assets(
    "sweden_financial_raw_archives_s3",
    "sweden_financial_report_xhtml_catalog_duckdb",
)


sweden_financial_backfill_job = dg.define_asset_job(
    "sweden_financial_backfill_job",
    selection=SWEDEN_FINANCIAL_ASSET_SELECTION,
)

sweden_financial_current_year_job = dg.define_asset_job(
    "sweden_financial_current_year_job",
    selection=SWEDEN_FINANCIAL_ASSET_SELECTION,
)


def _current_year_run_request(_: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    partition_key = current_sweden_financial_year()
    return dg.RunRequest(
        partition_key=partition_key,
    )


sweden_financial_current_year_weekly = dg.ScheduleDefinition(
    name="sweden_financial_current_year_weekly",
    job=sweden_financial_current_year_job,
    cron_schedule="45 6 * * 1",
    execution_timezone="Europe/Belgrade",
    execution_fn=_current_year_run_request,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        sweden_financial_raw_archives_s3,
        sweden_financial_report_xhtml_catalog_duckdb,
    ],
    jobs=[
        sweden_financial_backfill_job,
        sweden_financial_current_year_job,
    ],
    schedules=[sweden_financial_current_year_weekly],
    resources={
        "sweden_financial_duckdb": duckdb_resource(SWEDEN_FINANCIAL_DUCKDB_PATH),
        "sweden_financial_reports": SwedenFinancialReportsResource(),
    },
)
