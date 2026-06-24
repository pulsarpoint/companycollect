CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_company_identifiers
(
    crawl_id LowCardinality(String),
    root_domain String,
    url String,
    subdomain String,
    id_type LowCardinality(String),
    id_value String,
    valid UInt8,
    source LowCardinality(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, id_type, id_value, url, crawl_id);
