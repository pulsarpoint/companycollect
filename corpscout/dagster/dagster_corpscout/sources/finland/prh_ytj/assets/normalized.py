"""Normalized table asset for Finland PRH YTJ."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_ytj import spec
from dagster_corpscout.sources.finland.prh_ytj.assets.manifest import artifact_by_key
from dagster_corpscout.sources.finland.prh_ytj.assets.raw import raw_snapshot
from dagster_corpscout.sources.finland.prh_ytj.importer import import_normalized_snapshot


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="normalized_tables",
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "normalized"},
    deps=[raw_snapshot],
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def normalized_tables(
    context: dg.AssetExecutionContext,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Parse the latest PRH YTJ snapshot and import normalized ClickHouse rows."""
    manifest = rustfs.latest_manifest(spec.BUCKET)
    source = artifact_by_key(manifest, "source")
    with rustfs.open_object(spec.BUCKET, source["object_key"]) as stream:
        counts = import_normalized_snapshot(
            clickhouse=clickhouse,
            stream=stream,
            run_id=manifest["run_id"],
            progress_logger=context.log.info,
        )

    return dg.MaterializeResult(
        metadata={
            "run_id": manifest["run_id"],
            "tables": len(counts),
            "rows": sum(counts.values()),
            **{f"rows_{table}": count for table, count in counts.items()},
        }
    )
