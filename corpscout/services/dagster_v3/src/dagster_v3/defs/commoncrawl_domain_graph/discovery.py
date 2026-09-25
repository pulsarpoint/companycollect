from datetime import UTC, datetime

import dagster as dg
from dlt.sources.helpers import requests

from dagster_v3.defs.commoncrawl_domain_graph.assets import GROUP, PARTITIONS
from dagster_v3.defs.commoncrawl_domain_graph.catalog import discover_releases
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource


@dg.asset(
    group_name=GROUP, kinds={"http", "postgres"}, pool="commoncrawl_graph_discovery"
)
def commoncrawl_graph_release_catalog(
    context: dg.AssetExecutionContext,
    graph_catalog: GraphCatalogResource,
) -> dg.MaterializeResult:
    """Discover all official graph releases and download capabilities; no bulk imports."""
    started_at = datetime.now(UTC)
    with graph_catalog.get_store() as store:
        store.record_discovery_attempt(started_at)
        try:
            with requests.Session() as session:
                releases = discover_releases(session)
            store.save_catalog(releases, started_at)
            context.instance.add_dynamic_partitions(
                PARTITIONS.name, [release.graph_release for release in releases]
            )
        except Exception as error:
            store.record_discovery_failure(type(error).__name__)
            raise
    return dg.MaterializeResult(
        metadata={
            "release_count": len(releases),
            "available_files": sum(
                file.availability == "available"
                for release in releases
                for file in release.files
            ),
        }
    )


commoncrawl_graph_discovery_job = dg.define_asset_job(
    "commoncrawl_graph_discovery_job",
    selection=dg.AssetSelection.assets(commoncrawl_graph_release_catalog),
)
commoncrawl_graph_discovery_schedule = dg.ScheduleDefinition(
    job=commoncrawl_graph_discovery_job,
    cron_schedule="17 4 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
defs = dg.Definitions(
    assets=[commoncrawl_graph_release_catalog],
    jobs=[commoncrawl_graph_discovery_job],
    schedules=[commoncrawl_graph_discovery_schedule],
    resources={
        "graph_catalog": GraphCatalogResource(
            postgres_url=dg.EnvVar("COMMONCRAWL_GRAPH_PG_URL")
        )
    },
)
