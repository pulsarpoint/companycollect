import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from netaddr import IPAddress as NetaddrIPAddress
from netaddr import IPSet
from pydantic import field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_geoip.maxmind import classify_ip_scope
from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_ADDRESSES_ASSET,
    COMMONCRAWL_IP_PARTITIONS,
    commoncrawl_ip_bucket_index,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    NormalizedRdapNetwork,
    RdapIpLookupResult,
    normalize_rdap_network,
)


COMMONCRAWL_RDAP_POOL = "commoncrawl_rdap"
RDAP_USER_AGENT = "Corpscout RDAP enrichment/1.0"
BEST_KNOWN_SEMANTICS_WARNING = (
    "Trie matches are the most-specific RDAP registrations discovered so far; they do "
    "not prove that a more-specific undiscovered registration does not exist."
)
RDAP_NETWORK_COLUMNS = (
    "network_key",
    "rir",
    "handle",
    "ip_version",
    "start_address",
    "end_address",
    "name",
    "registration_type",
    "country_code",
    "status",
    "registrant_handles",
    "registrant_names",
    "parent_network_key",
    "parent_handle",
    "self_url",
    "up_url",
    "registration_date",
    "last_changed_at",
    "response_sha256",
    "raw_response",
    "fetched_at",
)
RDAP_SEGMENT_COLUMNS = (
    "network_key",
    "cidr",
    "ip_version",
    "prefix_length",
    "segment_role",
    "response_sha256",
    "derived_at",
)
RDAP_LOOKUP_COLUMNS = (
    "bucket",
    "ip",
    "ip_version",
    "lookup_status",
    "network_key",
    "error_code",
    "retry_after",
    "queried_at",
)

RDAP_CANDIDATES_SQL = """
SELECT
    ip,
    ip_version
FROM
(
    SELECT
        addresses.ip AS ip,
        addresses.ip_version AS ip_version
    FROM
    (
        SELECT ip, ip_version
        FROM corpscout.commoncrawl_ip_addresses FINAL
        WHERE bucket = %(bucket_index)s AND ip_version = 4
    ) AS addresses
    LEFT JOIN
    (
        SELECT
            ip,
            ip_version,
            toUInt8(1) AS matched,
            lookup_status,
            retry_after
        FROM corpscout.rdap_ip_lookup_results_current
        WHERE bucket = %(bucket_index)s
    ) AS current USING (ip, ip_version)
    WHERE dictHas(
              'corpscout.rdap_network_trie',
              tuple(toIPv4(addresses.ip))
          ) = 0
      AND (
          ifNull(current.matched, 0) = 0
          OR (
              current.lookup_status = 'retryable_error'
              AND (current.retry_after IS NULL OR current.retry_after <= now64(3))
          )
      )

    UNION ALL

    SELECT
        addresses.ip AS ip,
        addresses.ip_version AS ip_version
    FROM
    (
        SELECT ip, ip_version
        FROM corpscout.commoncrawl_ip_addresses FINAL
        WHERE bucket = %(bucket_index)s AND ip_version = 6
    ) AS addresses
    LEFT JOIN
    (
        SELECT
            ip,
            ip_version,
            toUInt8(1) AS matched,
            lookup_status,
            retry_after
        FROM corpscout.rdap_ip_lookup_results_current
        WHERE bucket = %(bucket_index)s
    ) AS current USING (ip, ip_version)
    WHERE dictHas(
              'corpscout.rdap_network_trie',
              tuple(toIPv6(addresses.ip))
          ) = 0
      AND (
          ifNull(current.matched, 0) = 0
          OR (
              current.lookup_status = 'retryable_error'
              AND (current.retry_after IS NULL OR current.retry_after <= now64(3))
          )
      )
)
ORDER BY ip_version, ip
LIMIT %(candidate_scan_limit)s
"""

RDAP_NETWORK_INSERT_SQL = """
INSERT INTO corpscout.rdap_networks
(
    network_key,
    rir,
    handle,
    ip_version,
    start_address,
    end_address,
    name,
    registration_type,
    country_code,
    status,
    registrant_handles,
    registrant_names,
    parent_network_key,
    parent_handle,
    self_url,
    up_url,
    registration_date,
    last_changed_at,
    response_sha256,
    raw_response,
    fetched_at
)
VALUES
"""

RDAP_SEGMENT_INSERT_SQL = """
INSERT INTO corpscout.rdap_network_segments
(
    network_key,
    cidr,
    ip_version,
    prefix_length,
    segment_role,
    response_sha256,
    derived_at
)
VALUES
"""

RDAP_LOOKUP_INSERT_SQL = """
INSERT INTO corpscout.rdap_ip_lookup_results
(
    bucket,
    ip,
    ip_version,
    lookup_status,
    network_key,
    error_code,
    retry_after,
    queried_at
)
VALUES
"""


class CommoncrawlRdapConfig(dg.Config):
    candidate_scan_limit: int = 10_000
    max_requests: int = 250
    insert_batch_size: int = 1_000
    request_delay_seconds: float = 1.0
    parent_depth: int = 1
    rate_limit_retry_seconds: int = 3_600
    transient_retry_seconds: int = 900

    @field_validator(
        "candidate_scan_limit",
        "max_requests",
        "insert_batch_size",
        "rate_limit_retry_seconds",
        "transient_retry_seconds",
    )
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_non_negative_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_delay_seconds must be zero or greater")
        return value

    @field_validator("parent_depth")
    @classmethod
    def validate_non_negative_depth(cls, value: int) -> int:
        if value < 0:
            raise ValueError("parent_depth must be zero or greater")
        return value


@dg.asset(
    name="commoncrawl_ip_rdap_networks",
    deps=[COMMONCRAWL_IP_ADDRESSES_ASSET.key],
    group_name="commoncrawl_rdap",
    kinds={"python", "clickhouse", "dns", "rdap"},
    partitions_def=COMMONCRAWL_IP_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=COMMONCRAWL_RDAP_POOL,
    description=(
        "Discovers reusable RDAP network registrations for one stable CommonCrawl IP "
        "bucket. Coverage is best-known and may not include undiscovered child assignments."
    ),
)
def commoncrawl_ip_rdap_networks(
    context: dg.AssetExecutionContext,
    config: CommoncrawlRdapConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    bucket_index = commoncrawl_ip_bucket_index(context.partition_key)
    _assert_rdap_storage_exists(clickhouse)
    _reload_rdap_dictionary(clickhouse)
    queried_at = datetime.now(UTC)
    rdap_client = RdapClient(user_agent=RDAP_USER_AGENT)
    try:
        result = _enrich_rdap_bucket(
            context=context,
            clickhouse=clickhouse,
            bucket_index=bucket_index,
            config=config,
            rdap_client=rdap_client,
            queried_at=queried_at,
            sleep=time.sleep,
        )
    finally:
        rdap_client.close()

    if result["direct_networks"] > 0:
        _reload_rdap_dictionary(clickhouse)
    metadata = {
        "partition": context.partition_key,
        "bucket_index": bucket_index,
        "network_table": "corpscout.rdap_networks",
        "segment_table": "corpscout.rdap_network_segments",
        "lookup_table": "corpscout.rdap_ip_lookup_results",
        **result,
    }
    context.log.info(
        "Completed CommonCrawl RDAP bucket: partition=%s metadata=%s",
        context.partition_key,
        metadata,
    )
    return dg.MaterializeResult(metadata=metadata)


def _enrich_rdap_bucket(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    bucket_index: int,
    config: CommoncrawlRdapConfig,
    rdap_client: RdapClient,
    queried_at: datetime,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    """Persist reusable coverage before writing an exact-IP completion marker."""
    counts: Counter[str] = Counter(
        {
            "candidates_scanned": 0,
            "rdap_requests": 0,
            "direct_networks": 0,
            "parent_networks": 0,
            "segments": 0,
            "in_run_trie_skips": 0,
            "non_global_ips": 0,
            "terminal_misses": 0,
            "retryable_errors": 0,
            "parent_lookup_failures": 0,
            "network_rows_written": 0,
            "segment_rows_written": 0,
            "lookup_rows_written": 0,
            "request_limit_reached": 0,
        }
    )
    rir_counts: Counter[str] = Counter()
    in_run_segments = IPSet()
    written_network_keys: set[str] = set()
    lookup_rows: list[tuple[Any, ...]] = []

    with (
        clickhouse.get_connection() as read_client,
        clickhouse.get_connection() as write_client,
    ):
        candidates = read_client.execute_iter(
            RDAP_CANDIDATES_SQL,
            {
                "bucket_index": bucket_index,
                "candidate_scan_limit": config.candidate_scan_limit,
            },
            settings={"max_block_size": config.insert_batch_size},
        )
        for ip_text, stored_ip_version in candidates:
            address = ip_address(str(ip_text))
            if address.version != int(stored_ip_version):
                raise ValueError(
                    f"IP version mismatch in commoncrawl_ip_addresses: ip={address} "
                    f"stored_version={stored_ip_version}"
                )
            counts["candidates_scanned"] += 1

            if classify_ip_scope(address) != "global":
                lookup_rows.append(
                    _lookup_result(
                        bucket_index=bucket_index,
                        address=str(address),
                        ip_version=address.version,
                        lookup_status="not_global",
                        network_key=None,
                        error_code=None,
                        retry_after=None,
                        queried_at=queried_at,
                    ).clickhouse_values()
                )
                counts["non_global_ips"] += 1
                counts["lookup_rows_written"] += _flush_full_lookup_batches(
                    write_client,
                    lookup_rows,
                    batch_size=config.insert_batch_size,
                )
                continue

            if NetaddrIPAddress(str(address)) in in_run_segments:
                counts["in_run_trie_skips"] += 1
                continue
            if counts["rdap_requests"] >= config.max_requests:
                counts["request_limit_reached"] = 1
                break

            _wait_between_requests(
                completed_requests=counts["rdap_requests"],
                delay_seconds=config.request_delay_seconds,
                sleep=sleep,
            )
            counts["rdap_requests"] += 1
            try:
                direct_response = rdap_client.lookup_ip(str(address))
            except RdapClientError as error:
                lookup_result = _direct_lookup_error_result(
                    error,
                    bucket_index=bucket_index,
                    address=str(address),
                    ip_version=address.version,
                    queried_at=queried_at,
                    config=config,
                )
                lookup_rows.append(lookup_result.clickhouse_values())
                if lookup_result.lookup_status == "retryable_error":
                    counts["retryable_errors"] += 1
                else:
                    counts["terminal_misses"] += 1
                counts["lookup_rows_written"] += _flush_full_lookup_batches(
                    write_client,
                    lookup_rows,
                    batch_size=config.insert_batch_size,
                )
                continue

            direct = normalize_rdap_network(
                direct_response,
                fetched_at=queried_at,
                segment_role="lookup_result",
            )
            normalized_networks = [direct]
            counts["direct_networks"] += 1
            rir_counts[direct.network.rir] += 1
            current = direct
            ancestor_keys = {direct.network.network_key}

            for _depth in range(config.parent_depth):
                if current.network.up_url is None:
                    break
                if counts["rdap_requests"] >= config.max_requests:
                    counts["request_limit_reached"] = 1
                    break
                _wait_between_requests(
                    completed_requests=counts["rdap_requests"],
                    delay_seconds=config.request_delay_seconds,
                    sleep=sleep,
                )
                counts["rdap_requests"] += 1
                try:
                    parent_response = rdap_client.lookup_up_url(
                        current.network.up_url,
                        rir=current.network.rir,
                    )
                except RdapClientError as error:
                    if error.code not in {"not_found", "unsupported"} and not (
                        error.retryable
                    ):
                        raise
                    counts["parent_lookup_failures"] += 1
                    if error.retryable:
                        counts["retryable_errors"] += 1
                    else:
                        counts["terminal_misses"] += 1
                    context.log.warning(
                        "RDAP parent lookup stopped: child_network=%s error_code=%s",
                        current.network.network_key,
                        error.code,
                    )
                    break

                parent = normalize_rdap_network(
                    parent_response,
                    fetched_at=queried_at,
                    segment_role="parent",
                )
                if parent.network.network_key in ancestor_keys:
                    context.log.warning(
                        "RDAP parent cycle stopped: network_key=%s",
                        parent.network.network_key,
                    )
                    break
                ancestor_keys.add(parent.network.network_key)
                normalized_networks.append(parent)
                current = parent
                counts["parent_networks"] += 1
                rir_counts[parent.network.rir] += 1

            network_rows, segment_rows = _insert_normalized_networks(
                write_client,
                normalized_networks,
                written_network_keys=written_network_keys,
                batch_size=config.insert_batch_size,
            )
            counts["network_rows_written"] += network_rows
            counts["segment_rows_written"] += segment_rows
            counts["segments"] += segment_rows
            for segment in direct.segments:
                in_run_segments.add(segment.cidr)

            lookup_rows.append(
                _lookup_result(
                    bucket_index=bucket_index,
                    address=str(address),
                    ip_version=address.version,
                    lookup_status="found",
                    network_key=direct.network.network_key,
                    error_code=None,
                    retry_after=None,
                    queried_at=queried_at,
                ).clickhouse_values()
            )
            counts["lookup_rows_written"] += _flush_full_lookup_batches(
                write_client,
                lookup_rows,
                batch_size=config.insert_batch_size,
            )

        if lookup_rows:
            write_client.execute(RDAP_LOOKUP_INSERT_SQL, lookup_rows)
            counts["lookup_rows_written"] += len(lookup_rows)
            lookup_rows.clear()

    result: dict[str, Any] = dict(counts)
    result["request_limit_reached"] = bool(counts["request_limit_reached"])
    result["rows_written"] = (
        counts["network_rows_written"]
        + counts["segment_rows_written"]
        + counts["lookup_rows_written"]
    )
    result["rir_counts"] = dict(rir_counts)
    result["best_known_semantics_warning"] = BEST_KNOWN_SEMANTICS_WARNING
    return result


def _insert_normalized_networks(
    client: Any,
    normalized_networks: list[NormalizedRdapNetwork],
    *,
    written_network_keys: set[str],
    batch_size: int,
) -> tuple[int, int]:
    new_networks = [
        normalized
        for normalized in normalized_networks
        if normalized.network.network_key not in written_network_keys
    ]
    if not new_networks:
        return 0, 0
    network_rows = [
        normalized.network.clickhouse_values() for normalized in new_networks
    ]
    segment_rows = [
        segment.clickhouse_values()
        for normalized in new_networks
        for segment in normalized.segments
    ]
    _insert_rows_in_chunks(
        client,
        RDAP_NETWORK_INSERT_SQL,
        network_rows,
        batch_size=batch_size,
    )
    _insert_rows_in_chunks(
        client,
        RDAP_SEGMENT_INSERT_SQL,
        segment_rows,
        batch_size=batch_size,
    )
    written_network_keys.update(
        normalized.network.network_key for normalized in new_networks
    )
    return len(network_rows), len(segment_rows)


def _insert_rows_in_chunks(
    client: Any,
    insert_sql: str,
    rows: list[tuple[Any, ...]],
    *,
    batch_size: int,
) -> None:
    for offset in range(0, len(rows), batch_size):
        client.execute(insert_sql, rows[offset : offset + batch_size])


def _flush_full_lookup_batches(
    client: Any,
    rows: list[tuple[Any, ...]],
    *,
    batch_size: int,
) -> int:
    inserted = 0
    while len(rows) >= batch_size:
        client.execute(RDAP_LOOKUP_INSERT_SQL, rows[:batch_size])
        del rows[:batch_size]
        inserted += batch_size
    return inserted


def _direct_lookup_error_result(
    error: RdapClientError,
    *,
    bucket_index: int,
    address: str,
    ip_version: int,
    queried_at: datetime,
    config: CommoncrawlRdapConfig,
) -> RdapIpLookupResult:
    if error.code in {"not_found", "unsupported"}:
        return _lookup_result(
            bucket_index=bucket_index,
            address=address,
            ip_version=ip_version,
            lookup_status=error.code,
            network_key=None,
            error_code=error.code,
            retry_after=None,
            queried_at=queried_at,
        )
    if not error.retryable:
        raise error
    retry_seconds = (
        config.rate_limit_retry_seconds
        if error.code == "rate_limited"
        else config.transient_retry_seconds
    )
    return _lookup_result(
        bucket_index=bucket_index,
        address=address,
        ip_version=ip_version,
        lookup_status="retryable_error",
        network_key=None,
        error_code=error.code,
        retry_after=queried_at + timedelta(seconds=retry_seconds),
        queried_at=queried_at,
    )


def _lookup_result(
    *,
    bucket_index: int,
    address: str,
    ip_version: int,
    lookup_status: str,
    network_key: str | None,
    error_code: str | None,
    retry_after: datetime | None,
    queried_at: datetime,
) -> RdapIpLookupResult:
    return RdapIpLookupResult(
        bucket=bucket_index,
        ip=address,
        ip_version=ip_version,
        lookup_status=lookup_status,
        network_key=network_key,
        error_code=error_code,
        retry_after=retry_after,
        queried_at=queried_at,
    )


def _wait_between_requests(
    *,
    completed_requests: int,
    delay_seconds: float,
    sleep: Callable[[float], None],
) -> None:
    if completed_requests > 0 and delay_seconds > 0:
        sleep(delay_seconds)


def _assert_rdap_storage_exists(clickhouse: ClickhouseResource) -> None:
    assert_clickhouse_tables_exist(
        clickhouse,
        database="corpscout",
        tables=(
            "commoncrawl_ip_addresses",
            "rdap_networks",
            "rdap_networks_current",
            "rdap_network_segments",
            "rdap_network_segments_current",
            "rdap_ip_lookup_results",
            "rdap_ip_lookup_results_current",
        ),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            """
            SELECT name
            FROM system.dictionaries
            WHERE database = 'corpscout' AND name = 'rdap_network_trie'
            """
        )
        if not rows:
            raise ValueError(
                "Missing ClickHouse dictionary corpscout.rdap_network_trie; apply migrations "
                "000124 and 000126 before materializing this asset"
            )
        create_rows = client.execute(
            "SHOW CREATE DICTIONARY corpscout.rdap_network_trie"
        )
    if not create_rows or "USER 'corpscout_rdap_dictionary'" not in str(
        create_rows[0][0]
    ):
        raise ValueError(
            "ClickHouse dictionary corpscout.rdap_network_trie still uses the original "
            "default-user source; apply migration 000126_corpscout_rdap_dictionary_reader "
            "before materializing this asset"
        )


def _reload_rdap_dictionary(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
