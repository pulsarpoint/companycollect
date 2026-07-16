CREATE DATABASE IF NOT EXISTS corpscout;

-- AggregatingMergeTree stores rows ordered by the hostname identity, so FINAL can merge outstanding
-- partial rows as an ordered stream without constructing the large hash table required by GROUP BY.
-- Every identity is confined to its root-domain hash partition, making independent partition merges
-- both correct and substantially faster for an exact full-inventory count.
CREATE OR REPLACE VIEW corpscout.domain_hostnames AS
SELECT
    root_domain,
    hostname,
    substring(
        hostname,
        1,
        greatest(
            toInt64(length(hostname)) - toInt64(length(root_domain)) - 1,
            0
        )
    ) AS label,
    toUInt8(has_ipv4) AS has_ipv4,
    toUInt8(has_ipv6) AS has_ipv6,
    toUInt8(has_cname) AS has_cname,
    multiIf(
        toUInt8(discovery_rank) = 3, 'axfr',
        toUInt8(discovery_rank) = 2, 'ct',
        toUInt8(discovery_rank) = 1, 'static',
        'unknown'
    ) AS discovery_source,
    toDateTime64(first_seen, 3, 'UTC') AS first_seen,
    toDateTime64(last_seen, 3, 'UTC') AS last_seen,
    toDateTime64(last_loaded_at, 3, 'UTC') AS last_loaded_at
FROM corpscout.domain_hostnames_state FINAL
SETTINGS do_not_merge_across_partitions_select_final = 1;
