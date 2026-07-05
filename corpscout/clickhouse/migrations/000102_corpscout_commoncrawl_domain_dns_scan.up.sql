CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_scan
(
    scan_id       LowCardinality(String),
    root_domain   String,
    etld          LowCardinality(String),
    nameservers   Array(String),
    ns_ips        Array(String),
    dnssec_signed UInt8,
    ds_present    UInt8,
    status        LowCardinality(String),
    error         String,
    queries_total UInt16,
    queries_ok    UInt16,
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id);
