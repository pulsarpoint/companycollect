CREATE DATABASE IF NOT EXISTS corpscout;

-- Compact, monotonic hostname state maintained from newly inserted DNS observations. Every
-- aggregate is min or max, so SimpleAggregateFunction stores the scalar state directly and makes
-- replayed observation inserts idempotent. The root-domain hash matches the source partitioning so
-- every state row for one hostname remains eligible for background merging.
--
-- This migration deliberately leaves the public corpscout.domain_hostnames view unchanged. Existing
-- observations are backfilled through the bucketed operation in clickhouse/operations before the
-- later cutover migration points readers at this table.
CREATE TABLE IF NOT EXISTS corpscout.domain_hostnames_state
(
    root_domain String,
    hostname    String,

    has_ipv4 SimpleAggregateFunction(max, UInt8),
    has_ipv6 SimpleAggregateFunction(max, UInt8),
    has_cname SimpleAggregateFunction(max, UInt8),

    -- Numeric rank makes the intended precedence explicit instead of relying on string ordering.
    discovery_rank SimpleAggregateFunction(max, UInt8),

    first_seen     SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen      SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_loaded_at SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
PARTITION BY cityHash64(root_domain) % 16
ORDER BY (root_domain, hostname);

-- ClickHouse incremental materialized views are synchronous insert triggers. This view sees only
-- each newly inserted block, reduces it to one state row per hostname in that block, and sends the
-- result to domain_hostnames_state. It does not rescan historical observations and stores no rows
-- itself.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.domain_hostnames_ingest_mv
TO corpscout.domain_hostnames_state
AS
SELECT
    root_domain,
    name AS hostname,
    max(toUInt8(record_type = 'A')) AS has_ipv4,
    max(toUInt8(record_type = 'AAAA')) AS has_ipv6,
    max(toUInt8(record_type = 'CNAME')) AS has_cname,
    max(
        toUInt8(
            multiIf(
                discovery = 'axfr', 3,
                discovery = 'ct', 2,
                discovery = 'static', 1,
                0
            )
        )
    ) AS discovery_rank,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM corpscout.commoncrawl_domain_dns_record_observations
WHERE record_type IN ('A', 'AAAA', 'CNAME')
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
    hostname;
