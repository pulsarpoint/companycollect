CREATE DATABASE IF NOT EXISTS corpscout;

-- Operational coverage for the 16 domain-hash partitions in the historical DNS source. Live DNS
-- observations update both segment-first tables regardless of historical replay status.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_ip_backfill_status
(
    bucket       UInt8,
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY bucket;
