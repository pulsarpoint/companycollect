CREATE DATABASE IF NOT EXISTS corpscout;

-- Dropping the legacy table deletes its data permanently. This rollback restores only the schema
-- and original insert triggers so an older DNS binary can write again. Restore a backup separately
-- if the deleted historical rows are required.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_observations
(
    root_domain String,
    name        String,
    record_type LowCardinality(String),
    record_type_code UInt16,
    record_class_code UInt16,
    slot        LowCardinality(String),
    value       String,
    rdata_wire  String,
    source      LowCardinality(String),
    discovery   LowCardinality(String),
    name_server String,
    name_server_ip String,
    scan_id     String,
    ttl         UInt32,
    priority    UInt16,
    rcode       String,
    observed_at DateTime64(3, 'UTC'),
    loaded_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY cityHash64(root_domain) % 16
ORDER BY
(
    root_domain,
    name,
    record_type,
    slot,
    value,
    source,
    discovery,
    scan_id,
    record_type_code,
    record_class_code,
    name_server,
    name_server_ip
);

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_ip_addresses_mv
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
