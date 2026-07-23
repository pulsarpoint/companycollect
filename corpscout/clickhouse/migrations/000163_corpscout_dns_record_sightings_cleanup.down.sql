CREATE DATABASE IF NOT EXISTS corpscout;

-- Recreates only the empty schemas of the tables dropped by the up migration. The deleted rows
-- cannot be restored here and recovery requires the pre-cleanup backup. No triggers are recreated:
-- after 000162 the only active write path is the v2 seen-window trigger, and reattaching the
-- sightings trigger belongs to the 000162 down migration.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_legacy
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
