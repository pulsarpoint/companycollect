"""Explorer cache asset for Finland PRH YTJ."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_ytj import spec
from dagster_corpscout.sources.finland.prh_ytj.assets.code_lists import code_lists
from dagster_corpscout.sources.finland.prh_ytj.assets.industry_mapping import (
    industry_nace_mappings,
)
from dagster_corpscout.sources.finland.prh_ytj.assets.normalized import normalized_tables
from dagster_corpscout.sources.finland.prh_ytj.explorer_cache import (
    refresh_company_explorer_cache,
)


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="company_explorer_cache",
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "serving"},
    deps=[normalized_tables, code_lists, industry_nace_mappings],
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def company_explorer_cache(
    context: dg.AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Refresh the final company explorer cache with an atomic ClickHouse table swap."""
    result = refresh_company_explorer_cache(clickhouse)
    context.log.info(
        "company explorer cache refreshed: %d rows in %s",
        result.rows,
        result.cache_table,
    )
    return dg.MaterializeResult(
        metadata={
            "table": result.cache_table,
            "rows": result.rows,
            "refreshed_at": result.refreshed_at.isoformat(),
        }
    )
