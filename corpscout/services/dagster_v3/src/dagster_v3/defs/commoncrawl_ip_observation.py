from collections.abc import Sequence

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_ADDRESSES_KEY,
    COMMONCRAWL_IP_PARTITIONS,
    commoncrawl_ip_bucket_index,
)


COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL = """
SELECT count() AS ip_count
FROM corpscout.commoncrawl_ip_addresses FINAL
WHERE bucket = %(bucket_index)s
"""
COMMONCRAWL_IP_MEMBERSHIP_VERSION_SCHEME = "unique-ip-count-v1"
_COMMONCRAWL_IP_ADDRESSES_SPEC = dg.AssetSpec(
    key=COMMONCRAWL_IP_ADDRESSES_KEY,
    description=(
        "Canonical unique A/AAAA addresses incrementally aggregated from retry-safe "
        "CommonCrawl DNS record observations. Each stable hash bucket can be observed "
        "manually to record whether its IP membership changed."
    ),
    group_name="commoncrawl_ip",
    kinds={"clickhouse", "dns"},
    partitions_def=COMMONCRAWL_IP_PARTITIONS,
)


def commoncrawl_ip_membership_data_version(ip_count: int) -> dg.DataVersion:
    """Version one bucket by its additive canonical-IP membership count."""
    if not isinstance(ip_count, int) or isinstance(ip_count, bool):
        raise TypeError(f"CommonCrawl IP count must be an integer: {ip_count!r}")
    if ip_count < 0:
        raise ValueError(f"CommonCrawl IP count cannot be negative: {ip_count}")
    return dg.DataVersion(f"{COMMONCRAWL_IP_MEMBERSHIP_VERSION_SCHEME}:{ip_count}")


def commoncrawl_ip_count_from_query(rows: Sequence[Sequence[object]]) -> int:
    """Validate the single aggregate row returned by the bucket count query."""
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(
            "CommonCrawl IP observation expected one row containing one count"
        )

    ip_count = rows[0][0]
    if not isinstance(ip_count, int) or isinstance(ip_count, bool):
        raise TypeError(
            f"ClickHouse returned a non-integer CommonCrawl IP count: {ip_count!r}"
        )
    if ip_count < 0:
        raise ValueError(f"ClickHouse returned a negative CommonCrawl IP count: {ip_count}")
    return ip_count


@dg.multi_observable_source_asset(
    specs=[_COMMONCRAWL_IP_ADDRESSES_SPEC],
    description=(
        "Manually records the selected bucket's canonical IP membership without "
        "materializing or launching either enrichment asset."
    ),
)
def commoncrawl_ip_addresses_observation(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.ObserveResult:
    """Observe one bucket; repeated sightings do not change its data version.

    The ClickHouse registry is additive. Its logical row count changes only when a
    canonical IP first enters this bucket, while ``last_seen`` changes whenever an
    existing IP is observed again. Keeping ``last_seen`` out of the data version
    prevents false downstream staleness.
    """
    bucket_index = commoncrawl_ip_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL,
            {"bucket_index": bucket_index},
        )
    ip_count = commoncrawl_ip_count_from_query(rows)

    return dg.ObserveResult(
        asset_key=COMMONCRAWL_IP_ADDRESSES_KEY,
        data_version=commoncrawl_ip_membership_data_version(ip_count),
        metadata={
            "partition_key": context.partition_key,
            "bucket_index": bucket_index,
            "ip_count": ip_count,
        },
    )


commoncrawl_ip_addresses_observation = (
    commoncrawl_ip_addresses_observation.with_attributes(
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )
)


commoncrawl_ip_addresses_observation_job = dg.define_asset_job(
    "observe_commoncrawl_ip_addresses",
    selection=dg.AssetSelection.assets(
        COMMONCRAWL_IP_ADDRESSES_KEY
    ).without_checks(),
    description=(
        "Manually observe one CommonCrawl IP bucket so Dagster can show whether "
        "the matching GeoIP and RDAP partitions are stale."
    ),
)


defs = dg.Definitions(
    assets=[commoncrawl_ip_addresses_observation],
    jobs=[commoncrawl_ip_addresses_observation_job],
)
