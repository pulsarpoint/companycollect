-- Restore the full-history records table from 000101.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;

CREATE TABLE corpscout.commoncrawl_domain_dns_records
(
    scan_id       LowCardinality(String),
    root_domain   String,
    name          String,
    record_type   LowCardinality(String),
    slot          LowCardinality(String),
    value         String,
    ttl           UInt32,
    priority      UInt16,
    rcode         LowCardinality(String),
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id, record_type, name, value);
