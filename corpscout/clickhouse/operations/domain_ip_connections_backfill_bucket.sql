-- Replays one existing domain-hash partition into both segment-first tables from migration 259.
-- Every target aggregate is idempotent, so interrupted or repeated bucket runs are safe.
INSERT INTO corpscout.commoncrawl_ip_network_segments
(
    segment_bucket,
    segment_cidr,
    ip_version,
    segment_start,
    segment_end,
    first_seen,
    last_seen,
    last_loaded_at
)
SELECT
    toUInt8(cityHash64(segment_cidr) % 64) AS segment_bucket,
    segment_cidr,
    ip_version,
    any(segment_start) AS segment_start,
    any(segment_end) AS segment_end,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    max(last_loaded_at) AS last_loaded_at
FROM
(
    SELECT
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
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
        ) AS segment_cidr,
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
        first_seen,
        last_seen,
        last_loaded_at
    FROM corpscout.commoncrawl_domain_dns_records
    WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
      AND record_type IN ('A', 'AAAA')
      AND if(
          record_type = 'A',
          isNotNull(toIPv4OrNull(value)),
          isNotNull(toIPv6OrNull(value))
      )
)
GROUP BY segment_bucket, segment_cidr, ip_version;

INSERT INTO corpscout.commoncrawl_domain_ip_connections
(
    segment_bucket,
    segment_cidr,
    ip_version,
    address,
    ip,
    root_domain,
    hostnames,
    sources,
    discoveries,
    seen_dates,
    first_seen,
    last_seen,
    last_loaded_at
)
SELECT
    toUInt8(cityHash64(segment_cidr) % 64) AS segment_bucket,
    segment_cidr,
    ip_version,
    address,
    any(ip) AS ip,
    root_domain,
    arraySort(groupUniqArray(hostname)) AS hostnames,
    arraySort(arrayDistinct(arrayFlatten(groupArray(sources)))) AS sources,
    arraySort(arrayDistinct(arrayFlatten(groupArray(discoveries)))) AS discoveries,
    arraySort(arrayDistinct(arrayFlatten(groupArray(seen_dates)))) AS seen_dates,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    max(last_loaded_at) AS last_loaded_at
FROM
(
    SELECT
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
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
        ) AS segment_cidr,
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
        sources,
        discoveries,
        seen_dates,
        first_seen,
        last_seen,
        last_loaded_at
    FROM corpscout.commoncrawl_domain_dns_records
    WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
      AND record_type IN ('A', 'AAAA')
      AND root_domain != ''
      AND name != ''
      AND if(
          record_type = 'A',
          isNotNull(toIPv4OrNull(value)),
          isNotNull(toIPv6OrNull(value))
      )
)
GROUP BY
    segment_bucket,
    segment_cidr,
    ip_version,
    address,
    root_domain;
