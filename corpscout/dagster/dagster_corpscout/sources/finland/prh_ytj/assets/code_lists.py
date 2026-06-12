"""Code-list asset for Finland PRH YTJ."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_ytj import spec
from dagster_corpscout.sources.finland.prh_ytj.assets.raw import raw_snapshot
from dagster_corpscout.sources.finland.prh_ytj.code_lists import (
    code_list_objects_from_manifest,
    import_code_lists,
)


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="code_lists",
    group_name=spec.GROUP_NAME,
    deps=[raw_snapshot],
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def code_lists(
    context: dg.AssetExecutionContext,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Parse the latest PRH YTJ code-list artifacts and import ClickHouse rows."""
    manifest = rustfs.latest_manifest(spec.BUCKET)
    objects = code_list_objects_from_manifest(manifest, rustfs, spec.BUCKET)
    imported = import_code_lists(
        clickhouse=clickhouse,
        objects=objects,
        run_id=manifest["run_id"],
    )
    return dg.MaterializeResult(metadata={"run_id": manifest["run_id"], "rows": imported})
