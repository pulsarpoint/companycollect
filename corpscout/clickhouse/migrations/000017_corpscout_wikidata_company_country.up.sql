CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.wikidata_companies
(
    wikidata_id String,
    wikidata_url String,
    name String,
    name_normalized String,
    company_description Nullable(String),
    official_name Nullable(String),
    headquarters_wikidata_id Nullable(String),
    headquarters_label Nullable(String),
    headquarters_country_wikidata_id Nullable(String),
    headquarters_country_label Nullable(String),
    headquarters_country_iso2 Nullable(String),
    country_resolution_method Nullable(String),
    country_resolution_confidence Nullable(String),
    industry_label Nullable(String),
    has_current_listing UInt8,
    listing_count UInt64,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (wikidata_id);

ALTER TABLE corpscout.wikidata_companies
    ADD COLUMN IF NOT EXISTS company_description Nullable(String) AFTER name_normalized,
    ADD COLUMN IF NOT EXISTS headquarters_wikidata_id Nullable(String) AFTER official_name,
    ADD COLUMN IF NOT EXISTS headquarters_country_wikidata_id Nullable(String) AFTER headquarters_label,
    ADD COLUMN IF NOT EXISTS headquarters_country_label Nullable(String) AFTER headquarters_country_wikidata_id,
    ADD COLUMN IF NOT EXISTS headquarters_country_iso2 Nullable(String) AFTER headquarters_country_label,
    ADD COLUMN IF NOT EXISTS country_resolution_method Nullable(String) AFTER headquarters_country_iso2,
    ADD COLUMN IF NOT EXISTS country_resolution_confidence Nullable(String) AFTER country_resolution_method;
