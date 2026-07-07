CREATE DATABASE IF NOT EXISTS corpscout;

-- Latest-good-state-per-domain summary (supersedes 000102, which is empty). The loader inserts ONLY
-- successful (status='done') scans, so a transient re-scan failure never clobbers a domain's last-good
-- nameservers / DNSSEC data; ReplacingMergeTree(resolved_at) keeps the most recent successful summary
-- per domain. Domains that never resolve simply never appear here (they have no DNS state); the
-- distinct records table's last_seen already tells you when a domain last answered.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_scan;

CREATE TABLE corpscout.commoncrawl_domain_dns_scan
(
    root_domain   String,
    etld          LowCardinality(String),
    nameservers   Array(String),
    ns_ips        Array(String),
    dnssec_signed UInt8,
    ds_present    UInt8,
    status        LowCardinality(String),
    queries_total UInt16,
    queries_ok    UInt16,
    last_run_id   String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain);
