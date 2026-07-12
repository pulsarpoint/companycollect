CREATE DATABASE IF NOT EXISTS corpscout;

-- Rollback restores the previous schemas, not the intentionally discarded data.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_metadata
(
    crawl_id LowCardinality(String),
    root_domain String,
    subdomain String,
    name String,
    description String,
    logo String,
    country LowCardinality(String),
    founding_year UInt16,
    employee_count UInt32,
    source LowCardinality(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);

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

DROP TABLE IF EXISTS corpscout.commoncrawl_page_metadata;
DROP TABLE IF EXISTS corpscout.commoncrawl_page_technologies;
