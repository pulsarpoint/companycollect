from collections import Counter
from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Any

import dagster as dg
import maxminddb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    GeoIPEnrichment,
    IPAddress,
    build_geoip_enrichment,
    classify_ip_scope,
    lookup_maxmind_record,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


GEOIP_BUCKET_COUNT = 256
COMMONCRAWL_GEOIP_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket_index:03d}" for bucket_index in range(GEOIP_BUCKET_COUNT)]
)
COMMONCRAWL_GEOIP_POOL = "commoncrawl_geoip"
GEOIP_COLUMNS = (
    "bucket",
    "ip",
    "ip_version",
    "ip_scope",
    "city_lookup_status",
    "asn_lookup_status",
    "continent_code",
    "continent_name",
    "country_iso_code",
    "country_name",
    "country_geoname_id",
    "registered_country_iso_code",
    "registered_country_name",
    "subdivision_iso_codes",
    "subdivision_names",
    "city_geoname_id",
    "city_name",
    "latitude",
    "longitude",
    "accuracy_radius_km",
    "timezone",
    "asn",
    "asn_organization",
    "city_network",
    "asn_network",
    "city_db_build_epoch",
    "asn_db_build_epoch",
    "enriched_at",
)
COMMONCRAWL_IP_ADDRESSES_ASSET = dg.AssetSpec(
    key="commoncrawl_ip_addresses",
    description=(
        "Canonical unique A/AAAA addresses refreshed in ClickHouse from the retry-safe "
        "CommonCrawl DNS record summary."
    ),
    group_name="commoncrawl_geoip",
    kinds={"clickhouse", "dns"},
)

GEOIP_CANDIDATES_SQL = """
SELECT
    addresses.ip,
    addresses.ip_version
FROM corpscout.commoncrawl_ip_addresses AS addresses
LEFT JOIN
(
    SELECT
        ip,
        toUInt8(1) AS matched,
        argMax(city_db_build_epoch, enriched_at) AS city_db_build_epoch,
        argMax(asn_db_build_epoch, enriched_at) AS asn_db_build_epoch
    FROM corpscout.commoncrawl_ip_geoip
    WHERE bucket = %(bucket_index)s
    GROUP BY ip
) AS current USING (ip)
WHERE addresses.bucket = %(bucket_index)s
  AND (
      ifNull(current.matched, 0) = 0
      OR current.city_db_build_epoch != %(city_db_build_epoch)s
      OR current.asn_db_build_epoch != %(asn_db_build_epoch)s
  )
ORDER BY addresses.ip
"""

GEOIP_INSERT_SQL = """
INSERT INTO corpscout.commoncrawl_ip_geoip
(
    bucket,
    ip,
    ip_version,
    ip_scope,
    city_lookup_status,
    asn_lookup_status,
    continent_code,
    continent_name,
    country_iso_code,
    country_name,
    country_geoname_id,
    registered_country_iso_code,
    registered_country_name,
    subdivision_iso_codes,
    subdivision_names,
    city_geoname_id,
    city_name,
    latitude,
    longitude,
    accuracy_radius_km,
    timezone,
    asn,
    asn_organization,
    city_network,
    asn_network,
    city_db_build_epoch,
    asn_db_build_epoch,
    enriched_at
)
VALUES
"""


class CommoncrawlGeoIPConfig(dg.Config):
    insert_batch_size: int = 50_000


@dg.asset(
    name="commoncrawl_ip_geoip",
    deps=[COMMONCRAWL_IP_ADDRESSES_ASSET.key],
    group_name="commoncrawl_geoip",
    kinds={"python", "clickhouse", "dns", "maxmind"},
    partitions_def=COMMONCRAWL_GEOIP_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=COMMONCRAWL_GEOIP_POOL,
    description=(
        "Enriches one stable hash bucket of unique CommonCrawl DNS IP addresses from "
        "the current MaxMind City and ASN databases."
    ),
)
def commoncrawl_ip_geoip(
    context: dg.AssetExecutionContext,
    config: CommoncrawlGeoIPConfig,
    clickhouse: ClickhouseResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    if config.insert_batch_size <= 0:
        raise ValueError("insert_batch_size must be greater than zero")

    bucket_index = geoip_bucket_index(context.partition_key)
    assert_clickhouse_tables_exist(
        clickhouse,
        database="corpscout",
        tables=("commoncrawl_ip_addresses", "commoncrawl_ip_geoip"),
    )
    city_database_path, asn_database_path = maxmind_geoip.database_paths()
    enriched_at = datetime.now(UTC)

    with (
        maxminddb.open_database(city_database_path) as city_reader,
        maxminddb.open_database(asn_database_path) as asn_reader,
    ):
        city_metadata = city_reader.metadata()
        asn_metadata = asn_reader.metadata()
        _assert_database_type(city_metadata.database_type, "City")
        _assert_database_type(asn_metadata.database_type, "ASN")
        city_build_epoch = datetime.fromtimestamp(city_metadata.build_epoch, UTC)
        asn_build_epoch = datetime.fromtimestamp(asn_metadata.build_epoch, UTC)

        context.log.info(
            "Starting CommonCrawl GeoIP bucket: partition=%s city_database=%s "
            "city_build_epoch=%s asn_database=%s asn_build_epoch=%s",
            context.partition_key,
            city_database_path,
            city_build_epoch.isoformat(),
            asn_database_path,
            asn_build_epoch.isoformat(),
        )
        result = _enrich_geoip_bucket(
            context=context,
            clickhouse=clickhouse,
            bucket_index=bucket_index,
            city_reader=city_reader,
            asn_reader=asn_reader,
            city_build_epoch=city_build_epoch,
            asn_build_epoch=asn_build_epoch,
            enriched_at=enriched_at,
            insert_batch_size=config.insert_batch_size,
        )

    metadata = {
        "partition": context.partition_key,
        "bucket_index": bucket_index,
        "table": "corpscout.commoncrawl_ip_geoip",
        "city_database": str(city_database_path),
        "city_db_build_epoch": city_build_epoch.isoformat(),
        "asn_database": str(asn_database_path),
        "asn_db_build_epoch": asn_build_epoch.isoformat(),
        **result,
    }
    context.log.info(
        "Completed CommonCrawl GeoIP bucket: partition=%s metadata=%s",
        context.partition_key,
        metadata,
    )
    return dg.MaterializeResult(metadata=metadata)


def geoip_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid GeoIP partition key: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < GEOIP_BUCKET_COUNT:
        raise ValueError(f"GeoIP bucket index out of range: {bucket_index}")
    return bucket_index


def _enrich_geoip_bucket(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    bucket_index: int,
    city_reader: Any,
    asn_reader: Any,
    city_build_epoch: datetime,
    asn_build_epoch: datetime,
    enriched_at: datetime,
    insert_batch_size: int,
) -> dict[str, Any]:
    counts: Counter[str] = Counter(
        {
            "candidate_count": 0,
            "inserted_count": 0,
            "city_found_count": 0,
            "city_not_found_count": 0,
            "city_not_global_count": 0,
            "asn_found_count": 0,
            "asn_not_found_count": 0,
            "asn_not_global_count": 0,
        }
    )
    scope_counts: Counter[str] = Counter()
    insert_rows: list[tuple[Any, ...]] = []

    with (
        clickhouse.get_connection() as read_client,
        clickhouse.get_connection() as write_client,
    ):
        candidates = read_client.execute_iter(
            GEOIP_CANDIDATES_SQL,
            {
                "bucket_index": bucket_index,
                "city_db_build_epoch": city_build_epoch,
                "asn_db_build_epoch": asn_build_epoch,
            },
            settings={"max_block_size": insert_batch_size},
        )
        for ip_text, ip_version in candidates:
            address = ip_address(str(ip_text))
            if address.version != int(ip_version):
                raise ValueError(
                    f"IP version mismatch in commoncrawl_ip_addresses: ip={address} "
                    f"stored_version={ip_version}"
                )
            enrichment = _enrich_address(
                bucket_index=bucket_index,
                address=address,
                city_reader=city_reader,
                asn_reader=asn_reader,
                city_build_epoch=city_build_epoch,
                asn_build_epoch=asn_build_epoch,
                enriched_at=enriched_at,
            )
            insert_rows.append(enrichment.clickhouse_values())
            _increment_counts(counts, scope_counts, enrichment)

            if len(insert_rows) >= insert_batch_size:
                counts["inserted_count"] += _insert_geoip_rows(
                    write_client, insert_rows
                )
                context.log.info(
                    "Inserted CommonCrawl GeoIP batch: bucket=%d inserted_total=%d",
                    bucket_index,
                    counts["inserted_count"],
                )
                insert_rows.clear()

        if insert_rows:
            counts["inserted_count"] += _insert_geoip_rows(write_client, insert_rows)

    return {
        **dict(counts),
        "scope_counts": dict(scope_counts),
    }


def _enrich_address(
    *,
    bucket_index: int,
    address: IPAddress,
    city_reader: Any,
    asn_reader: Any,
    city_build_epoch: datetime,
    asn_build_epoch: datetime,
    enriched_at: datetime,
) -> GeoIPEnrichment:
    if classify_ip_scope(address) == "global":
        city_lookup = lookup_maxmind_record(city_reader, address)
        asn_lookup = lookup_maxmind_record(asn_reader, address)
    else:
        city_lookup = None
        asn_lookup = None
    return build_geoip_enrichment(
        bucket=bucket_index,
        address=address,
        city_lookup=city_lookup,
        asn_lookup=asn_lookup,
        city_build_epoch=city_build_epoch,
        asn_build_epoch=asn_build_epoch,
        enriched_at=enriched_at,
    )


def _increment_counts(
    counts: Counter[str],
    scope_counts: Counter[str],
    enrichment: GeoIPEnrichment,
) -> None:
    counts["candidate_count"] += 1
    counts[f"city_{enrichment.city_lookup_status}_count"] += 1
    counts[f"asn_{enrichment.asn_lookup_status}_count"] += 1
    scope_counts[enrichment.ip_scope] += 1


def _insert_geoip_rows(client: Any, rows: list[tuple[Any, ...]]) -> int:
    client.execute(GEOIP_INSERT_SQL, rows)
    return len(rows)


def _assert_database_type(database_type: str, expected_suffix: str) -> None:
    if not database_type.endswith(expected_suffix):
        raise ValueError(
            f"Expected MaxMind {expected_suffix} database, got {database_type!r}"
        )
