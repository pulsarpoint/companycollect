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
    inception_date Nullable(Date),
    legal_form_wikidata_id Nullable(String),
    legal_form_label Nullable(String),
    employee_count Nullable(UInt64),
    employee_count_point_in_time Nullable(Date),
    logo_image Nullable(String),
    logo_image_url Nullable(String),
    industry_wikidata_id Nullable(String),
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

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_identifiers
(
    wikidata_id String,
    identifier_type LowCardinality(String),
    wikidata_property_id LowCardinality(String),
    identifier_value String,
    identifier_scope Nullable(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (identifier_type, identifier_value, wikidata_id);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_relationships
(
    subject_wikidata_id String,
    object_wikidata_id String,
    relationship_type LowCardinality(String),
    wikidata_property_id LowCardinality(String),
    relationship_statement_id String,
    object_name Nullable(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    is_current UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (subject_wikidata_id, relationship_type, object_wikidata_id);

ALTER TABLE corpscout.wikidata_companies
    ADD COLUMN IF NOT EXISTS inception_date Nullable(Date) AFTER country_resolution_confidence,
    ADD COLUMN IF NOT EXISTS legal_form_wikidata_id Nullable(String) AFTER inception_date,
    ADD COLUMN IF NOT EXISTS legal_form_label Nullable(String) AFTER legal_form_wikidata_id,
    ADD COLUMN IF NOT EXISTS employee_count Nullable(UInt64) AFTER legal_form_label,
    ADD COLUMN IF NOT EXISTS employee_count_point_in_time Nullable(Date) AFTER employee_count,
    ADD COLUMN IF NOT EXISTS logo_image Nullable(String) AFTER employee_count_point_in_time,
    ADD COLUMN IF NOT EXISTS logo_image_url Nullable(String) AFTER logo_image,
    ADD COLUMN IF NOT EXISTS industry_wikidata_id Nullable(String) AFTER logo_image_url;

ALTER TABLE corpscout.wikidata_company_identifiers
    ADD COLUMN IF NOT EXISTS wikidata_property_id LowCardinality(String) AFTER identifier_type;

ALTER TABLE corpscout.wikidata_company_relationships
    ADD COLUMN IF NOT EXISTS wikidata_property_id LowCardinality(String) AFTER relationship_type;
