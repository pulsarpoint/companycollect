CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_technologies
(
    crawl_id LowCardinality(String),
    url String,
    root_domain String,
    subdomain String,
    technology LowCardinality(String),
    category LowCardinality(String),
    version String,
    confidence UInt8,
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, url, technology, crawl_id);
