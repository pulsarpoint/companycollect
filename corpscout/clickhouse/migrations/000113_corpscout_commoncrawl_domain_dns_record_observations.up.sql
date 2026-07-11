CREATE DATABASE IF NOT EXISTS corpscout;

-- Retry-safe raw DNS record observations. This table stores one immutable row per observation
-- (root_domain, name, record_type, slot, value, source, discovery,
-- scan_id): replaying the identical rows for the same scan_id always produces byte-identical rows,
-- which ReplacingMergeTree(loaded_at) collapses back to one on merge (or FINAL), however many times
-- the load is retried.
--
-- Partition strategy: PARTITION BY a hash of root_domain ONLY -- never by loaded_at, scan_id, or any
-- time-derived expression. root_domain is the one identity field that stays IDENTICAL across every
-- retry of the same observation: loaded_at changes on every retry by definition, scan_id partitioning
-- would still scatter one domain's history across many small partitions as scans accumulate, and a
-- calendar/time partition would put a delayed retry in a different partition than its original attempt
-- entirely. ClickHouse only background-merges parts WITHIN one partition, so if a retry's row ever
-- landed in a different partition than the original, the two copies would never physically collapse via
-- background merges -- they would only appear deduplicated when a reader happens to use FINAL, and disk
-- usage would grow unbounded across retries. Hashing the stable root_domain into a fixed 16-way bucket
-- keeps every historical and retried row for one domain in the same partition, so background merges
-- eventually and permanently collapse retries, while still bounding any single partition's size as the
-- domain corpus grows.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_observations
(
    root_domain String,
    name        String,
    record_type LowCardinality(String),
    slot        LowCardinality(String),
    value       String,                     -- rdata verbatim (MX value embeds "<pref> <host>")
    source      LowCardinality(String),      -- "query" | "axfr" -- how the record was obtained
    discovery   LowCardinality(String),      -- "static" | "ct" | "axfr" -- how the hostname was found
    scan_id     String,                      -- the scan (cycle) this observation belongs to
    ttl         UInt32,
    priority    UInt16,                      -- MX preference, convenience also embedded in value
    rcode       String,                      -- query rcode for the query that produced this record
    observed_at DateTime64(3, 'UTC'),        -- event time: when the record was actually resolved
    loaded_at   DateTime64(3, 'UTC')         -- ReplacingMergeTree version: when this row was inserted
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY cityHash64(root_domain) % 16
ORDER BY (root_domain, name, record_type, slot, value, source, discovery, scan_id);
