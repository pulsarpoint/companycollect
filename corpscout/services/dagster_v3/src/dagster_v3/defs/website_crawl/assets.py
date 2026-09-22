"""Inventory selection assets for full, jobs and basic website crawls."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.website_crawl.input import CrawlInputConfig, seed_crawl_inputs


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    pool="website_crawl_input",
    metadata={"dagster/table_name": "corpscout.website_full_crawl_requests"},
    description="Add full-crawl domains selected by source IDs or column filters. Existing settings are preserved.",
)
def website_full_crawl_requests(
    context: dg.AssetExecutionContext,
    config: CrawlInputConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return seed_crawl_inputs(
        context, config, clickhouse, "corpscout.website_full_crawl_requests"
    )


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    pool="website_crawl_input",
    metadata={"dagster/table_name": "corpscout.website_jobs_crawl_requests"},
    description="Add jobs-crawl domains selected by source IDs or column filters. Existing settings are preserved.",
)
def website_jobs_crawl_requests(
    context: dg.AssetExecutionContext,
    config: CrawlInputConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return seed_crawl_inputs(
        context, config, clickhouse, "corpscout.website_jobs_crawl_requests"
    )


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse"},
    pool="website_crawl_input",
    metadata={"dagster/table_name": "corpscout.website_site_info_requests"},
    description="Add basic-info domains selected by source IDs or column filters. Existing settings are preserved.",
)
def website_site_info_requests(
    context: dg.AssetExecutionContext,
    config: CrawlInputConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return seed_crawl_inputs(
        context, config, clickhouse, "corpscout.website_site_info_requests"
    )


website_full_crawl_input_job = dg.define_asset_job(
    "website_full_crawl_input_job",
    selection=dg.AssetSelection.assets(website_full_crawl_requests),
)
website_jobs_crawl_input_job = dg.define_asset_job(
    "website_jobs_crawl_input_job",
    selection=dg.AssetSelection.assets(website_jobs_crawl_requests),
)
website_site_info_input_job = dg.define_asset_job(
    "website_site_info_input_job",
    selection=dg.AssetSelection.assets(website_site_info_requests),
)

defs = dg.Definitions(
    assets=[
        website_full_crawl_requests,
        website_jobs_crawl_requests,
        website_site_info_requests,
    ],
    jobs=[
        website_full_crawl_input_job,
        website_jobs_crawl_input_job,
        website_site_info_input_job,
    ],
)
