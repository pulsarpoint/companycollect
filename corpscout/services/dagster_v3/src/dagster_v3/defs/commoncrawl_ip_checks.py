from collections.abc import Sequence
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.commoncrawl_geoip.assets import GEOIP_MISSING_BY_BUCKET_SQL
from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_ADDRESSES_KEY,
    commoncrawl_ip_partition_key,
)
from dagster_v3.defs.commoncrawl_rdap.assets import RDAP_ACTIONABLE_BY_BUCKET_SQL


type GeoIPBacklogRow = tuple[int, int, datetime]
type RdapBacklogRow = tuple[int, int, int, int, datetime]

GEOIP_UPDATE_CHECK_KEY = dg.AssetCheckKey(
    COMMONCRAWL_IP_ADDRESSES_KEY,
    "commoncrawl_geoip_update_available",
)
RDAP_UPDATE_CHECK_KEY = dg.AssetCheckKey(
    COMMONCRAWL_IP_ADDRESSES_KEY,
    "commoncrawl_rdap_update_available",
)


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _geoip_check_result(
    rows: Sequence[GeoIPBacklogRow],
    *,
    checked_at: datetime,
) -> dg.AssetCheckResult:
    actionable_partitions: list[dict[str, str | int]] = []
    seen_partitions: set[str] = set()
    actionable_ip_count = 0
    oldest_uncovered_first_seen: datetime | None = None

    for bucket_index, pending_count, first_seen in sorted(rows, key=lambda row: row[0]):
        partition_key = commoncrawl_ip_partition_key(bucket_index)
        if partition_key in seen_partitions:
            raise ValueError(f"Duplicate GeoIP backlog partition: {partition_key}")
        if pending_count <= 0:
            raise ValueError(
                f"GeoIP backlog count must be positive for {partition_key}: "
                f"{pending_count}"
            )
        if not isinstance(first_seen, datetime):
            raise TypeError(
                f"GeoIP oldest first_seen must be a datetime for {partition_key}"
            )

        seen_partitions.add(partition_key)
        actionable_ip_count += pending_count
        actionable_partitions.append(
            {
                "partition_key": partition_key,
                "actionable_ip_count": pending_count,
            }
        )
        if (
            oldest_uncovered_first_seen is None
            or first_seen < oldest_uncovered_first_seen
        ):
            oldest_uncovered_first_seen = first_seen

    actionable_bucket_count = len(actionable_partitions)
    passed = actionable_ip_count == 0
    if passed:
        description = "GeoIP enrichment covers all current CommonCrawl IP addresses."
    else:
        bucket_label = "partition" if actionable_bucket_count == 1 else "partitions"
        description = (
            f"{actionable_ip_count} IP addresses in {actionable_bucket_count} "
            f"{bucket_label} need manual GeoIP enrichment."
        )

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=description,
        metadata={
            "actionable_ip_count": actionable_ip_count,
            "actionable_bucket_count": actionable_bucket_count,
            "actionable_partitions": dg.MetadataValue.json(actionable_partitions),
            "oldest_uncovered_first_seen": (
                dg.MetadataValue.text(_utc_isoformat(oldest_uncovered_first_seen))
                if oldest_uncovered_first_seen is not None
                else dg.MetadataValue.null()
            ),
            "checked_at_utc": _utc_isoformat(checked_at),
        },
    )


def _rdap_check_result(
    rows: Sequence[RdapBacklogRow],
    *,
    checked_at: datetime,
) -> dg.AssetCheckResult:
    actionable_partitions: list[dict[str, str | int]] = []
    seen_partitions: set[str] = set()
    actionable_ip_count = 0
    new_uncovered_ip_count = 0
    due_retry_ip_count = 0
    oldest_uncovered_first_seen: datetime | None = None

    for (
        bucket_index,
        pending_count,
        new_uncovered_count,
        due_retry_count,
        first_seen,
    ) in sorted(rows, key=lambda row: row[0]):
        partition_key = commoncrawl_ip_partition_key(bucket_index)
        if partition_key in seen_partitions:
            raise ValueError(f"Duplicate RDAP backlog partition: {partition_key}")
        if pending_count <= 0:
            raise ValueError(
                f"RDAP backlog count must be positive for {partition_key}: "
                f"{pending_count}"
            )
        if new_uncovered_count < 0 or due_retry_count < 0:
            raise ValueError(
                f"RDAP reason counts cannot be negative for {partition_key}"
            )
        if pending_count != new_uncovered_count + due_retry_count:
            raise ValueError(
                f"RDAP reason counts do not equal the backlog for {partition_key}"
            )
        if not isinstance(first_seen, datetime):
            raise TypeError(
                f"RDAP oldest first_seen must be a datetime for {partition_key}"
            )

        seen_partitions.add(partition_key)
        actionable_ip_count += pending_count
        new_uncovered_ip_count += new_uncovered_count
        due_retry_ip_count += due_retry_count
        actionable_partitions.append(
            {
                "partition_key": partition_key,
                "actionable_ip_count": pending_count,
                "new_uncovered_ip_count": new_uncovered_count,
                "due_retry_ip_count": due_retry_count,
            }
        )
        if (
            oldest_uncovered_first_seen is None
            or first_seen < oldest_uncovered_first_seen
        ):
            oldest_uncovered_first_seen = first_seen

    actionable_bucket_count = len(actionable_partitions)
    passed = actionable_ip_count == 0
    if passed:
        description = "RDAP state accounts for all current CommonCrawl IP addresses."
    else:
        bucket_label = "partition" if actionable_bucket_count == 1 else "partitions"
        description = (
            f"{actionable_ip_count} IP addresses in {actionable_bucket_count} "
            f"{bucket_label} need manual RDAP enrichment."
        )

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=description,
        metadata={
            "actionable_ip_count": actionable_ip_count,
            "new_uncovered_ip_count": new_uncovered_ip_count,
            "due_retry_ip_count": due_retry_ip_count,
            "actionable_bucket_count": actionable_bucket_count,
            "actionable_partitions": dg.MetadataValue.json(actionable_partitions),
            "oldest_uncovered_first_seen": (
                dg.MetadataValue.text(_utc_isoformat(oldest_uncovered_first_seen))
                if oldest_uncovered_first_seen is not None
                else dg.MetadataValue.null()
            ),
            "checked_at_utc": _utc_isoformat(checked_at),
        },
    )


@dg.asset_check(
    asset=COMMONCRAWL_IP_ADDRESSES_KEY,
    name=GEOIP_UPDATE_CHECK_KEY.name,
    description=(
        "Shows which stable IP buckets need a manually launched GeoIP update. "
        "The result changes only when this check is evaluated manually."
    ),
    blocking=False,
)
def commoncrawl_geoip_update_available(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        rows = client.execute(GEOIP_MISSING_BY_BUCKET_SQL)
    return _geoip_check_result(rows, checked_at=datetime.now(UTC))


@dg.asset_check(
    asset=COMMONCRAWL_IP_ADDRESSES_KEY,
    name=RDAP_UPDATE_CHECK_KEY.name,
    description=(
        "Shows which stable IP buckets need a manually launched RDAP update. "
        "The result changes only when this check is evaluated manually."
    ),
    blocking=False,
)
def commoncrawl_rdap_update_available(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        rows = client.execute(RDAP_ACTIONABLE_BY_BUCKET_SQL)
    return _rdap_check_result(rows, checked_at=datetime.now(UTC))


commoncrawl_ip_enrichment_backlog_checks = dg.define_asset_job(
    "commoncrawl_ip_enrichment_backlog_checks",
    selection=dg.AssetSelection.checks(
        GEOIP_UPDATE_CHECK_KEY,
        RDAP_UPDATE_CHECK_KEY,
    ),
)


defs = dg.Definitions(
    asset_checks=[
        commoncrawl_geoip_update_available,
        commoncrawl_rdap_update_available,
    ],
    jobs=[commoncrawl_ip_enrichment_backlog_checks],
)
