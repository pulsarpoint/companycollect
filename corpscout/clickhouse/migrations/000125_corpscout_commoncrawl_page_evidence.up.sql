CREATE DATABASE IF NOT EXISTS corpscout;

-- Lossless page-level evidence. Domain summaries are derived later in ClickHouse.
-- The enrichment worker does not merge pages before writing these tables.
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

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_technologies
(
    crawl_id LowCardinality(String),
    root_domain String,
    page_url String,
    subdomain String,
    warc_index UInt32,
    warc_filename String,
    warc_record_offset UInt64,
    warc_record_length UInt64,
    technology LowCardinality(String),
    category LowCardinality(String),
    version String,
    confidence UInt8,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),
    PROJECTION by_technology_version
    (
        SELECT technology, version, root_domain, _part_offset
        ORDER BY (technology, version, root_domain)
    )
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY crawl_id
ORDER BY (root_domain, crawl_id, warc_index, warc_record_offset, technology)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- The existing crawl is incomplete and will be reprocessed from the WARC catalog.
-- Keeping the aggregated tables would require compatibility and dual-write paths.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_metadata;
DROP TABLE IF EXISTS corpscout.commoncrawl_technologies;
