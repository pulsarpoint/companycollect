CREATE DATABASE IF NOT EXISTS corpscout;

-- What a domain's pages say about themselves: schema.org/JSON-LD Organization plus meta tags scraped
-- by the tech pass. Self-reported, not verified. One row per domain. Contacts live in
-- commoncrawl_domain_contact_info, authoritative company facts in the external company master.
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
