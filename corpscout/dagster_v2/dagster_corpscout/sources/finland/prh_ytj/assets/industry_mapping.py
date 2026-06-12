"""Industry mapping asset for Finland PRH YTJ."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_ytj import spec
from dagster_corpscout.sources.finland.prh_ytj.assets.normalized import normalized_tables
from dagster_corpscout.sources.finland.prh_ytj.industry_mapping import (
    refresh_industry_nace_mappings,
)


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="industry_nace_mappings",
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "mapping"},
    deps=[normalized_tables],
    automation_condition=dg.AutomationCondition.eager(),
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def industry_nace_mappings(
    context: dg.AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Refresh source industry codes mapped to reference NACE classes."""
    result = refresh_industry_nace_mappings(clickhouse)
    context.log.info(
        "industry NACE mappings refreshed: %d rows, %d mapped, %d unmapped",
        result.rows,
        result.mapped_rows,
        result.unmapped_rows,
    )
    return dg.MaterializeResult(
        metadata={
            "table": result.mapping_table,
            "rows": result.rows,
            "mapped_rows": result.mapped_rows,
            "unmapped_rows": result.unmapped_rows,
            "mapped_at": result.mapped_at.isoformat(),
        }
    )
