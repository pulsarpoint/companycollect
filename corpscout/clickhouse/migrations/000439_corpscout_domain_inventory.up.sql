CREATE DATABASE IF NOT EXISTS corpscout;

-- Published inventory, one row per root. The publisher builds a complete staging
-- table and exchanges it atomically. Source systems remain authoritative.
CREATE TABLE IF NOT EXISTS corpscout.domain_inventory
(
    root_domain String,
    sources Array(LowCardinality(String)),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    source_run_id String
)
ENGINE = MergeTree
ORDER BY root_domain;
