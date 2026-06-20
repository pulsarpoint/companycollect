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

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_listings
(
    wikidata_id String,
    listing_statement_id String,
    exchange_wikidata_id String,
    exchange_name String,
    ticker Nullable(String),
    isin Nullable(String),
    is_current UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (exchange_wikidata_id, ifNull(ticker, ''), wikidata_id, listing_statement_id);

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

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_websites
(
    wikidata_id String,
    website_url String,
    website_normalized_url String,
    website_host String,
    root_domain String,
    website_path Nullable(String),
    website_kind LowCardinality(String),
    confidence LowCardinality(String),
    validation_status LowCardinality(String),
    is_primary_candidate UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (wikidata_id, root_domain, website_normalized_url);

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

CREATE TABLE IF NOT EXISTS corpscout.wikidata_seed_extraction_runs
(
    source_run_id String,
    query_mode LowCardinality(String),
    query_exchange_id Nullable(String),
    query_hash FixedString(64),
    row_count UInt64,
    distinct_company_count UInt64,
    distinct_listing_count UInt64,
    companies_with_website_count UInt64,
    companies_with_cik_count UInt64,
    companies_with_lei_count UInt64,
    started_at DateTime64(3, 'UTC'),
    completed_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String)
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (source_run_id, query_mode, ifNull(query_exchange_id, ''));
