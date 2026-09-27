"""provider-recon: trigger the service, then normalise its S3 documents in ClickHouse.

provider_recon_documents runs a collect on the provider-recon service
(companycollect) and reports it; the documents it writes are what the
S3-engine table provider_recon_documents_s3 maps. provider_recon_clickhouse
upserts those documents into the permanent range/rule timeline tables.
"""

from datetime import UTC, datetime

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE, assert_clickhouse_tables_exist
from dagster_v3.defs.provider_recon import sql, tables
from dagster_v3.defs.provider_recon.resource import ProviderReconResource

GROUP_NAME = "provider_recon"
FEEDS_OK_CHECK = "provider_recon_feeds_ok"


@dg.asset(
    name="provider_recon_documents",
    group_name=GROUP_NAME,
    kinds={"s3", "clickhouse"},
    check_specs=[dg.AssetCheckSpec(FEEDS_OK_CHECK, asset="provider_recon_documents",
                                   description="WARN when any provider feed ended stale or failed in the run.")],
    description=(
        "Runs a provider-recon collect on the companycollect service and waits for it. The "
        "per-provider documents land in the provider-recon bucket, mapped in ClickHouse as "
        "corpscout.provider_recon_documents_s3."
    ),
)
def provider_recon_documents(
    context: AssetExecutionContext,
    provider_recon: ProviderReconResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    started = provider_recon.start_collect()
    run_id = started["run_id"]
    context.log.info("provider-recon run %s started", run_id)
    run = provider_recon.wait_for_run(run_id)
    if run["status"] != "succeeded":
        raise dg.Failure(description=f"provider-recon run {run_id} failed: {run.get('error', '')}",
                         metadata={"run_id": run_id})
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=(tables.DOCUMENTS_S3_TABLE,))
    with clickhouse.get_connection() as client:
        documents = int(client.execute(sql.documents_count_sql(RESOLVED_DATABASE))[0][0])
    issues = run.get("issues") or []
    issue_lines = "\n".join(f"{i['slug']} {i['collector']} {i['status']}: {i.get('error', '')}" for i in issues)
    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "changed": len(run.get("changed") or []),
            "changed_providers": ", ".join(run.get("changed") or []) or "none",
            "unchanged": run.get("unchanged_count", 0),
            "issues": len(issues),
            "documents": documents,
        },
        check_results=[
            dg.AssetCheckResult(
                check_name=FEEDS_OK_CHECK,
                passed=not issues,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"issues": dg.MetadataValue.text(issue_lines or "none")},
            )
        ],
    )


@dg.asset(
    name="provider_recon_clickhouse",
    deps=[provider_recon_documents],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "Upserts provider-recon documents from provider_recon_documents_s3 into "
        "provider_services, provider_ip_ranges and provider_rules (ReplacingMergeTree by "
        "loaded_at; nothing is deleted, so ClickHouse keeps the full timeline)."
    ),
)
def provider_recon_clickhouse(context: AssetExecutionContext, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=tables.ALL_TABLES)
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    rows: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        documents = int(client.execute(sql.documents_count_sql(RESOLVED_DATABASE))[0][0])
        if documents == 0:
            raise ValueError(
                "provider_recon_documents_s3 returned no documents; refusing to load "
                "(check the provider_recon named collection and the bucket)"
            )
        for table, statement in sql.load_statements(RESOLVED_DATABASE):
            context.log.info("loading %s", table)
            client.execute(statement, {"loaded_at": loaded_at})
            result = client.execute(
                f"SELECT count() FROM `{RESOLVED_DATABASE}`.`{table}` WHERE loaded_at = %(loaded_at)s",
                {"loaded_at": loaded_at},
            )
            rows[table] = int(result[0][0]) if result else 0
    return dg.MaterializeResult(metadata={"documents": documents, **{f"{t}_rows": n for t, n in rows.items()}})


provider_recon_job = dg.define_asset_job(
    name="provider_recon_job",
    selection=dg.AssetSelection.assets(provider_recon_documents, provider_recon_clickhouse),
)

# Daily 03:12 UTC (a minute no other schedule uses). STOPPED by default per
# house pattern; start it on the instance after the first verified run.
provider_recon_daily = dg.ScheduleDefinition(
    name="provider_recon_daily",
    job=provider_recon_job,
    cron_schedule="12 3 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[provider_recon_documents, provider_recon_clickhouse],
    jobs=[provider_recon_job],
    schedules=[provider_recon_daily],
    resources={"provider_recon": ProviderReconResource()},
)
