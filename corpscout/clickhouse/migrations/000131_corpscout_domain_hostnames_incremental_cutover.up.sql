CREATE DATABASE IF NOT EXISTS corpscout;

-- Migration 130's state table must be historically backfilled and validated before this cutover.
-- AggregatingMergeTree merges equal keys asynchronously, so the public view still groups by the
-- hostname identity to finalize any states that remain in separate physical parts. Explicit casts
-- preserve the exact public column types exposed by the former source-backed view.
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
GROUP BY
    root_domain,
    hostname;
