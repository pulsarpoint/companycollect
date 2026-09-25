"""Materialize per-type result tables by processing saved crawl inputs."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.website_crawl.results import CrawlResultsConfig, process_crawls


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_crawl_input"],
    metadata={"dagster/table_name": "corpscout.website_full_crawl_results"},
    description="Process a crawl draft (task_id) or a bounded batch of explicit or due inputs, and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_full_crawl_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    crawler_queue_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return process_crawls(
        context, config, clickhouse, processing, "full", crawler_queue_store
    )


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_crawl_input"],
    metadata={"dagster/table_name": "corpscout.website_jobs_crawl_results"},
    description="Process a crawl draft (task_id) or a bounded batch of explicit or due inputs, and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_jobs_crawl_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    crawler_queue_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return process_crawls(
        context, config, clickhouse, processing, "jobs", crawler_queue_store
    )


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_crawl_input"],
    metadata={"dagster/table_name": "corpscout.website_site_info_results"},
    description="Process a crawl draft (task_id) or a bounded batch of explicit or due inputs, and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_site_info_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    crawler_queue_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return process_crawls(
        context, config, clickhouse, processing, "site_info", crawler_queue_store
    )


defs = dg.Definitions(
    assets=[
        website_full_crawl_results,
        website_jobs_crawl_results,
        website_site_info_results,
    ],
    jobs=[
        dg.define_asset_job(
            "website_full_crawl_results_job",
            selection=dg.AssetSelection.assets(website_full_crawl_results),
        ),
        dg.define_asset_job(
            "website_jobs_crawl_results_job",
            selection=dg.AssetSelection.assets(website_jobs_crawl_results),
        ),
        dg.define_asset_job(
            "website_site_info_results_job",
            selection=dg.AssetSelection.assets(website_site_info_results),
        ),
    ],
)
