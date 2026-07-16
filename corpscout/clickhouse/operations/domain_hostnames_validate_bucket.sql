-- Compare one finalized state bucket with the authoritative source-table derivation. Count plus two
-- independent order-insensitive fingerprints make this check inexpensive enough to run after every
-- bucket while covering every public column. A mismatch raises a ClickHouse exception so the Make
-- target fails before the public view can be cut over.
SELECT
    throwIf(
        source_count != state_count
        OR source_hash_sum != state_hash_sum
        OR source_hash_xor != state_hash_xor,
        'domain_hostnames_state does not match the source observations for this bucket'
    ) AS validation_error,
    source_count,
    state_count,
    source_hash_sum = state_hash_sum AS hash_sum_matches,
    source_hash_xor = state_hash_xor AS hash_xor_matches
FROM
(
    SELECT
        count() AS source_count,
        sum(row_hash) AS source_hash_sum,
        groupBitXor(row_hash) AS source_hash_xor
    FROM
    (
        SELECT cityHash64(
            root_domain,
            hostname,
            has_ipv4,
            has_ipv6,
            has_cname,
            discovery_source,
            first_seen,
            last_seen,
            last_loaded_at
        ) AS row_hash
        FROM
        (
            SELECT
                root_domain,
                name AS hostname,
                max(record_type = 'A') AS has_ipv4,
                max(record_type = 'AAAA') AS has_ipv6,
                max(record_type = 'CNAME') AS has_cname,
                multiIf(
                    max(discovery = 'axfr') = 1, 'axfr',
                    max(discovery = 'ct') = 1, 'ct',
                    max(discovery = 'static') = 1, 'static',
                    'unknown'
                ) AS discovery_source,
                min(observed_at) AS first_seen,
                max(observed_at) AS last_seen,
                max(loaded_at) AS last_loaded_at
            FROM corpscout.commoncrawl_domain_dns_record_observations
            WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
              AND record_type IN ('A', 'AAAA', 'CNAME')
              AND root_domain != ''
              AND name != ''
              AND position(name, '*') = 0
              AND
              (
                  name = root_domain
                  OR endsWith(name, concat('.', root_domain))
              )
            GROUP BY
                root_domain,
                hostname
        )
    )
) AS source
CROSS JOIN
(
    SELECT
        count() AS state_count,
        sum(row_hash) AS state_hash_sum,
        groupBitXor(row_hash) AS state_hash_xor
    FROM
    (
        SELECT cityHash64(
            root_domain,
            hostname,
            has_ipv4,
            has_ipv6,
            has_cname,
            discovery_source,
            first_seen,
            last_seen,
            last_loaded_at
        ) AS row_hash
        FROM
        (
            SELECT
                root_domain,
                hostname,
                toUInt8(max(has_ipv4)) AS has_ipv4,
                toUInt8(max(has_ipv6)) AS has_ipv6,
                toUInt8(max(has_cname)) AS has_cname,
                multiIf(
                    toUInt8(max(discovery_rank)) = 3, 'axfr',
                    toUInt8(max(discovery_rank)) = 2, 'ct',
                    toUInt8(max(discovery_rank)) = 1, 'static',
                    'unknown'
                ) AS discovery_source,
                toDateTime64(min(first_seen), 3, 'UTC') AS first_seen,
                toDateTime64(max(last_seen), 3, 'UTC') AS last_seen,
                toDateTime64(max(last_loaded_at), 3, 'UTC') AS last_loaded_at
            FROM corpscout.domain_hostnames_state
            WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
            GROUP BY
                root_domain,
                hostname
        )
    )
) AS state;
