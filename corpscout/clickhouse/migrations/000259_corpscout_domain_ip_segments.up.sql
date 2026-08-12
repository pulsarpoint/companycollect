CREATE DATABASE IF NOT EXISTS corpscout;

-- Stable routing neighborhoods used by the domain-IP graph. A /24 is specific enough to describe
-- a useful IPv4 hosting neighborhood without treating a whole provider allocation as one segment.
-- IPv6 /48 is the corresponding site-prefix boundary. These identities never depend on mutable
-- GeoIP, ASN, or RDAP enrichment.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_network_segments
(
    segment_bucket  UInt8,
    segment_cidr    String,
    ip_version      UInt8,
    segment_start   IPv6,
    segment_end     IPv6,
    first_seen      SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen       SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_loaded_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
PARTITION BY segment_bucket
ORDER BY
(
    segment_bucket,
    segment_cidr,
    ip_version
);

-- One address/domain relationship with hostname and observation history embedded in the row. The
-- bounded partition bucket avoids one physical partition per CIDR while the ORDER BY retains the
-- complete segment identity for index pruning. Exact-IP reads use the same prefix.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_ip_connections
(
    segment_bucket  UInt8,
    segment_cidr    String,
    ip_version      UInt8,
    address         IPv6,
    ip              SimpleAggregateFunction(any, String),
    root_domain     String,
    hostnames       SimpleAggregateFunction(groupUniqArrayArray, Array(String)),
    sources         SimpleAggregateFunction(groupUniqArrayArray, Array(String)),
    discoveries     SimpleAggregateFunction(groupUniqArrayArray, Array(String)),
    seen_dates      SimpleAggregateFunction(groupUniqArrayArray, Array(Date)),
    first_seen      SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen       SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_loaded_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
PARTITION BY segment_bucket
ORDER BY
(
    segment_bucket,
    segment_cidr,
    ip_version,
    address,
    root_domain
);

-- Both DNS scanners acknowledge inserts only after this Null-engine fan-out succeeds. The segment
-- row and denormalized domain-IP row therefore advance together under durable outbox retries.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_ip_network_segments_ingest_mv
TO corpscout.commoncrawl_ip_network_segments
AS
SELECT
    segment_bucket,
    segment_cidr,
    ip_version,
    any(segment_start) AS segment_start,
    any(segment_end) AS segment_end,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM
(
    SELECT
        toUInt8(cityHash64(segment_cidr) % 64) AS segment_bucket,
        segment_cidr,
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
        if(
            record_type = 'A',
            toIPv6(toString(tupleElement(
                IPv4CIDRToRange(assumeNotNull(toIPv4OrNull(value)), 24),
                1
            ))),
            tupleElement(
                IPv6CIDRToRange(assumeNotNull(toIPv6OrNull(value)), 48),
                1
            )
        ) AS segment_start,
        if(
            record_type = 'A',
            toIPv6(toString(tupleElement(
                IPv4CIDRToRange(assumeNotNull(toIPv4OrNull(value)), 24),
                2
            ))),
            tupleElement(
                IPv6CIDRToRange(assumeNotNull(toIPv6OrNull(value)), 48),
                2
            )
        ) AS segment_end,
        observed_at,
        loaded_at
    FROM
    (
        SELECT
            *,
            if(
                record_type = 'A',
                concat(toString(tupleElement(
                    IPv4CIDRToRange(assumeNotNull(toIPv4OrNull(value)), 24),
                    1
                )), '/24'),
                concat(toString(tupleElement(
                    IPv6CIDRToRange(assumeNotNull(toIPv6OrNull(value)), 48),
                    1
                )), '/48')
            ) AS segment_cidr
        FROM corpscout.commoncrawl_domain_dns_record_ingest
        WHERE record_type IN ('A', 'AAAA')
          AND if(
              record_type = 'A',
              isNotNull(toIPv4OrNull(value)),
              isNotNull(toIPv6OrNull(value))
          )
    )
)
GROUP BY segment_bucket, segment_cidr, ip_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_ip_connections_ingest_mv
TO corpscout.commoncrawl_domain_ip_connections
AS
SELECT
    segment_bucket,
    segment_cidr,
    ip_version,
    address,
    any(ip) AS ip,
    root_domain,
    groupUniqArray(hostname) AS hostnames,
    groupUniqArray(source) AS sources,
    groupUniqArray(discovery) AS discoveries,
    groupUniqArray(toDate(observed_at)) AS seen_dates,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM
(
    SELECT
        toUInt8(cityHash64(segment_cidr) % 64) AS segment_bucket,
        segment_cidr,
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
        if(
            record_type = 'A',
            toIPv6(toString(assumeNotNull(toIPv4OrNull(value)))),
            assumeNotNull(toIPv6OrNull(value))
        ) AS address,
        if(
            record_type = 'A',
            toString(assumeNotNull(toIPv4OrNull(value))),
            toString(assumeNotNull(toIPv6OrNull(value)))
        ) AS ip,
        root_domain,
        name AS hostname,
        source,
        discovery,
        observed_at,
        loaded_at
    FROM
    (
        SELECT
            *,
            if(
                record_type = 'A',
                concat(toString(tupleElement(
                    IPv4CIDRToRange(assumeNotNull(toIPv4OrNull(value)), 24),
                    1
                )), '/24'),
                concat(toString(tupleElement(
                    IPv6CIDRToRange(assumeNotNull(toIPv6OrNull(value)), 48),
                    1
                )), '/48')
            ) AS segment_cidr
        FROM corpscout.commoncrawl_domain_dns_record_ingest
        WHERE record_type IN ('A', 'AAAA')
          AND root_domain != ''
          AND name != ''
          AND if(
              record_type = 'A',
              isNotNull(toIPv4OrNull(value)),
              isNotNull(toIPv6OrNull(value))
          )
    )
)
GROUP BY
    segment_bucket,
    segment_cidr,
    ip_version,
    address,
    root_domain;
