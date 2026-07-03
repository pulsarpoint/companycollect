from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialReportsResource

GROUP_NAME = "sweden_financial"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "bolagsverket", "xbrl"},
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
    result = sweden_financial_reports.download_raw_archives(
        object_store=object_store,
        log_info=context.log.info,
    )
    metadata = dict(result.metadata or {})
    metadata["started_at"] = started_at.isoformat()
    metadata["finished_at"] = datetime.now(UTC).isoformat()
    return dg.MaterializeResult(metadata=metadata)


sweden_financial_raw_archives_refresh_job = dg.define_asset_job(
    "sweden_financial_raw_archives_refresh_job",
    selection=dg.AssetSelection.assets("sweden_financial_raw_archives_s3"),
)

sweden_financial_raw_archives_weekly = dg.ScheduleDefinition(
    name="sweden_financial_raw_archives_weekly",
    job=sweden_financial_raw_archives_refresh_job,
    cron_schedule="45 6 * * 1",
    execution_timezone="Europe/Belgrade",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


defs = dg.Definitions(
    assets=[sweden_financial_raw_archives_s3],
    jobs=[sweden_financial_raw_archives_refresh_job],
    schedules=[sweden_financial_raw_archives_weekly],
    resources={
        "sweden_financial_reports": SwedenFinancialReportsResource(),
    },
)
