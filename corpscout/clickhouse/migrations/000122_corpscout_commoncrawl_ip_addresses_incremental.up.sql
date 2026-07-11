CREATE DATABASE IF NOT EXISTS corpscout;

-- Production conversion from migration 115's empty refreshable summary consumer to the retry-safe
-- incremental registry. The old table is currently empty, and dropping it removes an incompatible
-- plain MergeTree schema before the aggregate-state columns are created.
DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_mv;
DROP TABLE IF EXISTS corpscout.commoncrawl_ip_addresses;

CREATE TABLE corpscout.commoncrawl_ip_addresses
(
    bucket     UInt16,
    ip         String,
    ip_version UInt8,
    first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (bucket, ip_version, ip);

CREATE MATERIALIZED VIEW corpscout.commoncrawl_ip_addresses_mv
TO corpscout.commoncrawl_ip_addresses
AS
SELECT
    toUInt16(cityHash64(ip) % 256) AS bucket,
    ip,
    ip_version,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen
FROM
(
    SELECT
        assumeNotNull(
            if(
                record_type = 'A',
                toString(toIPv4OrNull(value)),
                toString(toIPv6OrNull(value))
            )
        ) AS ip,
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
        observed_at
    FROM corpscout.commoncrawl_domain_dns_record_observations
    WHERE record_type IN ('A', 'AAAA')
      AND if(
          record_type = 'A',
          isNotNull(toIPv4OrNull(value)),
          isNotNull(toIPv6OrNull(value))
      )
)
GROUP BY bucket, ip, ip_version;
