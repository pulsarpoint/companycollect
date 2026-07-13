CREATE DATABASE IF NOT EXISTS corpscout;

-- Lossless page-level JSON-LD evidence. Each typed or identified node is a separate row and no
-- organization/profile is selected by the worker. The source record plus script index and JSON
-- pointer identify the same entity when a page is processed again.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_jsonld
(
    crawl_id LowCardinality(String),
    root_domain String,
    page_url String,
    subdomain String,
    warc_index UInt32,
    warc_filename String,
    warc_record_offset UInt64,
    warc_record_length UInt64,
    script_index UInt32,
    entity_path String,
    entity_id String,
    entity_types Array(LowCardinality(String)),
    is_organization UInt8,
    name String,
    legal_name String,
    description String,
    entity_url String,
    logo String,
    email String,
    telephone String,
    same_as Array(String),
    country LowCardinality(String),
    founding_year UInt16,
    employee_count UInt32,
    entity_json String CODEC(ZSTD(3)),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY crawl_id
ORDER BY
(
    root_domain,
    crawl_id,
    warc_index,
    warc_record_offset,
    script_index,
    entity_path
);

-- Migration 000125 introduced a single distilled profile per page. It cannot represent sibling
-- JSON-LD entities and is intentionally replaced rather than dual-written.
DROP TABLE IF EXISTS corpscout.commoncrawl_page_metadata;
