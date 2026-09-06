CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_wikipedia_articles
(
    wikidata_id String,
    site_id LowCardinality(String),
    language_code LowCardinality(String),
    wikipedia_page_id UInt64,
    wikipedia_revision_id UInt64,
    wikipedia_revision_at DateTime64(3, 'UTC'),
    article_title String,
    article_url String,
    article_revision_url String,
    article_lead_text String CODEC(ZSTD(3)),
    article_text String CODEC(ZSTD(3)),
    content_format LowCardinality(String),
    license_name String,
    license_url String,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (wikidata_id, site_id);
