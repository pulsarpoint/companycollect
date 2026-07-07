CREATE DATABASE IF NOT EXISTS corpscout;

-- Distinct-record model superseding the empty full-history table from 000101. One row per
-- (root_domain, record_type, slot, name, value) with min(first_seen), max(last_seen), sum(scans),
-- so re-scans dedup and a missed scan just fails to advance last_seen instead of a false removal.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;

CREATE TABLE corpscout.commoncrawl_domain_dns_records
(
    root_domain  String,
    record_type  LowCardinality(String),
    slot         LowCardinality(String),
    name         String,
    value        String,
    ttl          SimpleAggregateFunction(anyLast, UInt32),
    priority     SimpleAggregateFunction(anyLast, UInt16),
    rcode        SimpleAggregateFunction(anyLast, String),
    last_run_id  SimpleAggregateFunction(anyLast, String),
    first_seen   SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen    SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    scans        SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, record_type, slot, name, value);
