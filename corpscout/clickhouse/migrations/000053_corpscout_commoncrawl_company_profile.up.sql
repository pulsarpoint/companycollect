CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_company_profile
(
    crawl_id LowCardinality(String),
    root_domain String,
    url String,
    subdomain String,
    name String,
    description String,
    logo String,
    country LowCardinality(String),
    email String,
    phone String,
    founding_year UInt16,
    employee_count UInt32,
    same_as Array(String),
    source LowCardinality(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, url, crawl_id);
