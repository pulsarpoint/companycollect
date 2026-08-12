WITH
source_connections AS
(
    SELECT count() AS rows
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
            root_domain
        FROM corpscout.commoncrawl_domain_dns_records FINAL
        WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
          AND record_type IN ('A', 'AAAA')
          AND root_domain != ''
          AND name != ''
          AND if(
              record_type = 'A',
              isNotNull(toIPv4OrNull(value)),
              isNotNull(toIPv6OrNull(value))
          )
        GROUP BY ip_version, segment_cidr, address, root_domain
    )
),
target_connections AS
(
    SELECT count() AS rows
    FROM corpscout.commoncrawl_domain_ip_connections FINAL
    WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
),
missing_segments AS
(
    SELECT count() AS rows
    FROM
    (
        SELECT
            segment_bucket,
            segment_cidr,
            ip_version
        FROM corpscout.commoncrawl_domain_ip_connections FINAL
        WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
        GROUP BY segment_bucket, segment_cidr, ip_version
    )
    AS connections
    LEFT JOIN
    (
        SELECT
            segment_bucket,
            segment_cidr,
            ip_version,
            toUInt8(1) AS found
        FROM corpscout.commoncrawl_ip_network_segments FINAL
    )
    AS segments
    USING (segment_bucket, segment_cidr, ip_version)
    WHERE segments.found = 0
)
SELECT
    source_connections.rows AS source_connection_rows,
    target_connections.rows AS target_connection_rows,
    missing_segments.rows AS missing_segment_rows,
    throwIf(
        source_connections.rows != target_connections.rows,
        'Domain-IP connection bucket mismatch'
    ) AS connection_validation,
    throwIf(
        missing_segments.rows != 0,
        'Domain-IP connections reference missing network segments'
    ) AS segment_validation
FROM source_connections
CROSS JOIN target_connections
CROSS JOIN missing_segments;

INSERT INTO corpscout.commoncrawl_domain_ip_backfill_status
(
    bucket,
    completed_at
)
VALUES
(
    {bucket:UInt8},
    now64(3, 'UTC')
);
