CREATE DATABASE IF NOT EXISTS corpscout;

-- Rollback restores the former schema, not data discarded by the up migration.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_metadata
(
    crawl_id LowCardinality(String),
    root_domain String,
    page_url String,
    subdomain String,
    warc_index UInt32,
    warc_filename String,
    warc_record_offset UInt64,
    warc_record_length UInt64,
    name String,
    description String,
    logo String,
    country LowCardinality(String),
    founding_year UInt16,
    employee_count UInt32,
    source LowCardinality(String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY crawl_id
ORDER BY (root_domain, crawl_id, warc_index, warc_record_offset);

DROP TABLE IF EXISTS corpscout.commoncrawl_page_jsonld;
