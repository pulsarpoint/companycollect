from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    COMMONCRAWL_DOMAINS_KEY,
    COMMONCRAWL_HOSTNAME_GROUP,
    COMMONCRAWL_HOSTNAME_SOURCE_KEYS,
    CTLOGS_HOSTNAMES_KEY,
    DNS_RECORD_OBSERVATIONS_KEY,
    EPOCH,
    HOSTNAME_SHARD_COUNT,
    HOSTNAME_SYNC_WATERMARKS_SQL,
    hostname_shard_index,
)


COMMONCRAWL_HOSTNAME_SOURCE_VERSION_SCHEME = "source-watermark-v1"


@dataclass(frozen=True)
class _HostnameSource:
    key: dg.AssetKey
    table: str
    watermark_column: str
    observation_scope: str
    description: str
    kinds: frozenset[str]


_HOSTNAME_SOURCES = (
    _HostnameSource(
        key=CTLOGS_HOSTNAMES_KEY,
        table="ctlogs.hostnames",
        watermark_column="last_ingested_at",
        observation_scope="all Certificate Transparency hostname updates in the shard",
        description=(
            "Certificate Transparency hostnames stored in ctlogs.hostnames. Manual "
            "observations record the selected shard's latest ingestion watermark so "
            "Dagster can show downstream hostname staleness."
        ),
        kinds=frozenset({"clickhouse", "ct"}),
    ),
    _HostnameSource(
        key=COMMONCRAWL_DOMAINS_KEY,
        table="corpscout.commoncrawl_domains",
        watermark_column="resolved_at",
        observation_scope="non-empty Common Crawl root domains in the shard",
        description=(
            "Root domains selected from Common Crawl for DNS scanning. Manual "
            "observations record the selected shard's latest resolution watermark so "
            "Dagster can show downstream hostname staleness."
        ),
        kinds=frozenset({"clickhouse", "commoncrawl", "dns"}),
    ),
    _HostnameSource(
        key=DNS_RECORD_OBSERVATIONS_KEY,
        table="corpscout.commoncrawl_domain_dns_record_observations",
        watermark_column="loaded_at",
        observation_scope="AXFR observations in the shard; non-AXFR rows are excluded",
        description=(
            "Retry-safe DNS record observations written by cc-dns-scan and cc-dns-axfr. "
            "Manual observations track only the AXFR rows consumed by the downstream "
            "hostname asset."
        ),
        kinds=frozenset({"clickhouse", "dns", "axfr"}),
    ),
)

# The defs-folder loader flattens module-level lists as standalone definitions.
# A tuple keeps these specs owned only by the multi-observable asset below.
_HOSTNAME_SOURCE_SPECS = tuple(
    dg.AssetSpec(
        key=source.key,
        description=source.description,
        group_name=COMMONCRAWL_HOSTNAME_GROUP,
        kinds=set(source.kinds),
        partitions_def=COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    )
    for source in _HOSTNAME_SOURCES
)


def normalize_hostname_source_watermark(
    watermark: object,
    *,
    watermark_column: str,
) -> datetime:
    """Normalize one ClickHouse watermark to UTC, using the epoch for an empty shard."""
    if watermark is None:
        return EPOCH
    if not isinstance(watermark, datetime):
        raise TypeError(
            f"ClickHouse returned a non-datetime {watermark_column} watermark: "
            f"{watermark!r}"
        )
    if watermark.tzinfo is None or watermark.utcoffset() is None:
        return watermark.replace(tzinfo=UTC)
    return watermark.astimezone(UTC)


def hostname_source_data_version(
    watermark_column: str,
    watermark: datetime,
) -> dg.DataVersion:
    """Build a deterministic source version from a normalized cursor watermark."""
    normalized = normalize_hostname_source_watermark(
        watermark,
        watermark_column=watermark_column,
    )
    timestamp = normalized.isoformat(timespec="milliseconds")
    return dg.DataVersion(
        f"{COMMONCRAWL_HOSTNAME_SOURCE_VERSION_SCHEME}:{watermark_column}:{timestamp}"
    )


def hostname_source_watermarks_from_query(
    rows: Sequence[Sequence[object]],
) -> tuple[datetime, datetime, datetime]:
    """Validate and normalize the three watermarks returned for one source shard."""
    if len(rows) != 1 or len(rows[0]) != len(_HOSTNAME_SOURCES):
        raise RuntimeError(
            "CommonCrawl hostname source observation expected one row containing "
            "three watermarks"
        )

    return tuple(
        normalize_hostname_source_watermark(
            watermark,
            watermark_column=source.watermark_column,
        )
        for source, watermark in zip(_HOSTNAME_SOURCES, rows[0], strict=True)
    )


@dg.multi_observable_source_asset(
    specs=_HOSTNAME_SOURCE_SPECS,
    can_subset=True,
    description=(
        "Manually records source-table watermarks for one hostname shard without "
        "materializing commoncrawl_domain_hostnames."
    ),
)
def commoncrawl_hostname_sources_observation(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.ObserveResult]:
    """Observe the selected source keys for one stable hostname shard."""
    shard_index = hostname_shard_index(context.partition_key)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            HOSTNAME_SYNC_WATERMARKS_SQL,
            {
                "shard_count": HOSTNAME_SHARD_COUNT,
                "shard_index": shard_index,
            },
        )
    watermarks = hostname_source_watermarks_from_query(rows)

    for source, watermark in zip(_HOSTNAME_SOURCES, watermarks, strict=True):
        if source.key not in context.selected_asset_keys:
            continue
        yield dg.ObserveResult(
            asset_key=source.key,
            data_version=hostname_source_data_version(
                source.watermark_column,
                watermark,
            ),
            metadata={
                "source_table": source.table,
                "watermark_column": source.watermark_column,
                "watermark_utc": watermark.isoformat(timespec="milliseconds"),
                "observation_scope": source.observation_scope,
                "partition_key": context.partition_key,
                "shard_index": shard_index,
                "shard_count": HOSTNAME_SHARD_COUNT,
            },
        )


commoncrawl_hostname_sources_observation = (
    commoncrawl_hostname_sources_observation.with_attributes(
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )
)


commoncrawl_hostname_sources_observation_job = dg.define_asset_job(
    "observe_commoncrawl_hostname_sources",
    selection=dg.AssetSelection.assets(
        *COMMONCRAWL_HOSTNAME_SOURCE_KEYS
    ).without_checks(),
    description=(
        "Manually observe one CommonCrawl hostname source shard so Dagster can show "
        "whether the matching hostname partition is stale."
    ),
)
