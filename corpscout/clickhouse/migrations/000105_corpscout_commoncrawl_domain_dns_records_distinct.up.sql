CREATE DATABASE IF NOT EXISTS corpscout;

-- Distinct-record model (supersedes the full-history table from 000101, which is empty). One row per
-- (root_domain, record_type, slot, name, value) carrying its lifespan. Each 2-3 day re-scan inserts
-- the same rows with first_seen = last_seen = scan time and scans = 1; AggregatingMergeTree merges to
-- min(first_seen) / max(last_seen) / sum(scans), so storage grows only when a genuinely new record
-- appears, and a scan that misses a record (NS did not answer) simply fails to advance last_seen
-- instead of recording a false "removed".
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;

CREATE TABLE corpscout.commoncrawl_domain_dns_records
(
    root_domain  String,
    record_type  LowCardinality(String),
    slot         LowCardinality(String),
    name         String,
    value        String,                        -- rdata verbatim; MX value is "<pref> <host>"
    ttl          SimpleAggregateFunction(anyLast, UInt32),
    priority     SimpleAggregateFunction(anyLast, UInt16),
    rcode        SimpleAggregateFunction(anyLast, String),
    last_run_id  SimpleAggregateFunction(anyLast, String),   -- source_run_id of the last scan to see it
    first_seen   SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen    SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    scans        SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, record_type, slot, name, value);
