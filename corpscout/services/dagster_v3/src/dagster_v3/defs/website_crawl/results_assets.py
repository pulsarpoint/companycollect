"""Materialize per-type result tables by processing saved crawl inputs."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.results import CrawlResultsConfig, process_crawls


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_full_crawl_requests"],
    metadata={"dagster/table_name": "corpscout.website_full_crawl_results"},
    description="Process enabled, due inputs in bounded batches and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_full_crawl_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    return process_crawls(context, config, clickhouse, processing, "full")


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_jobs_crawl_requests"],
    metadata={"dagster/table_name": "corpscout.website_jobs_crawl_results"},
    description="Process enabled, due inputs in bounded batches and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_jobs_crawl_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    return process_crawls(context, config, clickhouse, processing, "jobs")


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    deps=["website_site_info_requests"],
    metadata={"dagster/table_name": "corpscout.website_site_info_results"},
    description="Process enabled, due inputs in bounded batches and store completed crawler responses.",
    pool="website_crawl_results",
)
def website_site_info_results(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    return process_crawls(context, config, clickhouse, processing, "site_info")


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
