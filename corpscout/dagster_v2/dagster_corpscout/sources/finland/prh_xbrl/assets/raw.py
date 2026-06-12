import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_xml_documents",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
    deps=[source_system],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_xml_documents(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    raise NotImplementedError("Download the partition window into RustFS before registering.")
