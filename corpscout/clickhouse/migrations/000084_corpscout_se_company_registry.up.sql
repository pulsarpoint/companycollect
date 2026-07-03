CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_companies
(
    company_id String,
    registration_number String,
    bolagsverket_company_id_raw Nullable(String),
    scb_company_id_raw Nullable(String),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    legal_form_code Nullable(String),
    status LowCardinality(String),
    status_reason Nullable(String),
    incorporation_date Nullable(Date32),
    dissolution_date Nullable(Date32),
    activity_description Nullable(String),
    source_run_id String,
    bolagsverket_source_record_id Nullable(String),
    scb_source_record_id Nullable(String),
    bolagsverket_source_payload_hash Nullable(String),
    scb_source_payload_hash Nullable(String),
    updated_from_raw_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_from_raw_at)
ORDER BY (company_id);

CREATE TABLE IF NOT EXISTS corpscout.se_company_addresses
(
    company_id String,
    address_type LowCardinality(String),
    source LowCardinality(String),
    raw_address Nullable(String),
    street_address Nullable(String),
    care_of Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    country_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    updated_from_raw_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_from_raw_at)
ORDER BY (company_id, address_type, source, source_record_id);

CREATE TABLE IF NOT EXISTS corpscout.se_industries
(
    company_id String,
    sequence UInt8,
    is_primary UInt8,
    sni_code String,
    nace_rev2_class_code String,
    source_field LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    updated_from_raw_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_from_raw_at)
ORDER BY (company_id, sequence, sni_code);
