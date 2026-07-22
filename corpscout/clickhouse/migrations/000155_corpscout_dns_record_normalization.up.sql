CREATE DATABASE IF NOT EXISTS corpscout;

-- One immutable definition per canonical DNS resource record. The identity deliberately excludes
-- scan and query provenance: an RR is identified by its root-domain scope, owner, numeric class and
-- type, and exact uncompressed RDATA. Repeated sightings insert the same deterministic record_id and
-- ReplacingMergeTree eventually retains one compact definition.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records
(
    record_id        FixedString(16),
    root_domain      String,
    name             String,
    record_type      LowCardinality(String),
    record_type_code UInt16,
    record_class_code UInt16,
    value            String,
    rdata_wire       String,
    priority         UInt16,
    loaded_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY cityHash64(root_domain) % 16
ORDER BY
(
    root_domain,
    name,
    record_type_code,
    record_class_code,
    record_id
);

-- This is the logical unique-record surface. Background replacement is asynchronous, so callers
-- that require uniqueness use this view rather than depending on merge timing in the storage table.
CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_current AS
SELECT
    record_id,
    root_domain,
    name,
    record_type,
    record_type_code,
    record_class_code,
    value,
    rdata_wire,
    priority,
    loaded_at
FROM corpscout.commoncrawl_domain_dns_records FINAL
SETTINGS do_not_merge_across_partitions_select_final = 1;

-- Narrow temporal evidence that a canonical RR was seen. root_domain is intentionally retained in
-- the fact so the dominant domain-history query can prune by both month and ORDER BY without first
-- joining the complete record dimension. observed_at is stable across an outbox retry, so retry
-- copies remain in the same calendar partition and ReplacingMergeTree can collapse them.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_sightings
(
    record_id     FixedString(16),
    root_domain   String,
    scan_id       LowCardinality(String),
    slot          LowCardinality(String),
    source        LowCardinality(String),
    discovery     LowCardinality(String),
    name_server   String,
    name_server_ip String,
    ttl           UInt32,
    rcode         LowCardinality(String),
    observed_at   DateTime64(3, 'UTC'),
    loaded_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY
(
    root_domain,
    record_id,
    scan_id,
    source,
    discovery,
    slot,
    name_server,
    name_server_ip
);

-- Both DNS binaries write their existing full retry-safe row to this zero-storage routing table.
-- Its materialized views synchronously fan the inserted block into the two normalized tables and
-- the existing hostname and IP aggregate states. A failed target fails the source insert, allowing
-- the durable SQLite outboxes to retry the complete fan-out.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_ingest
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
ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_ingest_mv
TO corpscout.commoncrawl_domain_dns_records
AS
SELECT
    sipHash128(root_domain, name, record_type_code, record_class_code, rdata_wire) AS record_id,
    root_domain,
    name,
    record_type,
    record_type_code,
    record_class_code,
    value,
    rdata_wire,
    priority,
    loaded_at
FROM corpscout.commoncrawl_domain_dns_record_ingest;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv
TO corpscout.commoncrawl_domain_dns_record_sightings
AS
SELECT
    sipHash128(root_domain, name, record_type_code, record_class_code, rdata_wire) AS record_id,
    root_domain,
    scan_id,
    slot,
    source,
    discovery,
    name_server,
    name_server_ip,
    ttl,
    rcode,
    observed_at,
    loaded_at
FROM corpscout.commoncrawl_domain_dns_record_ingest;

-- These v2 triggers preserve the current derived-table behavior after the writers move from the
-- legacy wide history table to the routing table. The original triggers remain attached to the
-- legacy table so migration 155 is safe to apply before deploying the updated binaries.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_ip_addresses_ingest_v2_mv
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
    FROM corpscout.commoncrawl_domain_dns_record_ingest
    WHERE record_type IN ('A', 'AAAA')
      AND if(
          record_type = 'A',
          isNotNull(toIPv4OrNull(value)),
          isNotNull(toIPv6OrNull(value))
      )
)
GROUP BY bucket, ip, ip_version;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.domain_hostnames_ingest_v2_mv
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
FROM corpscout.commoncrawl_domain_dns_record_ingest
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
