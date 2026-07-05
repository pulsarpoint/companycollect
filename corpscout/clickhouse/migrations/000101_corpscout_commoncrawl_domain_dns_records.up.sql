CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records
(
    scan_id       LowCardinality(String),
    root_domain   String,
    name          String,
    record_type   LowCardinality(String),
    slot          LowCardinality(String),
    value         String,                     -- rdata verbatim; MX value is "<pref> <host>" so the
                                              -- sort key below can't collapse two MX at different prefs
    ttl           UInt32,
    priority      UInt16,                     -- MX preference (convenience; also embedded in value)
    rcode         LowCardinality(String),
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id, record_type, name, value);
