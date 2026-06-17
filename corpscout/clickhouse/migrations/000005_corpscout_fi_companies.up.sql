CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_companies
(
    business_id String,
    country_iso2 LowCardinality(String),
    name String,
    name_normalized String,
    registration_date Nullable(Date),
    end_date Nullable(Date),
    lifecycle_status String,
    is_active UInt8,
    legal_form_code Nullable(String),
    legal_form_description_original Nullable(String),
    legal_form_description_language Nullable(String),
    legal_form_description_en Nullable(String),
    legal_form_description_translated_at Nullable(DateTime64(3, 'UTC')),
    legal_form_description_translation_provider Nullable(String),
    legal_form_description_translation_model Nullable(String),
    primary_website_url Nullable(String),
    primary_website_host Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (business_id);
