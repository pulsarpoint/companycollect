CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_line_number UInt64,
    source_record_id String,
    source_payload_hash FixedString(64),
    regcode String,
    vat_id String,
    sepa String,
    legal_name String,
    name_in_quotes String,
    legal_form_code LowCardinality(String),
    legal_form_text String,
    legal_form_description_en String,
    regtype_code LowCardinality(String),
    regtype_text String,
    registered_date String,
    terminated_date String,
    closed_flag LowCardinality(String),
    status LowCardinality(String),
    is_active UInt8,
    address String,
    postal_code String,
    address_id String,
    region_code LowCardinality(String),
    city_code LowCardinality(String),
    atvk_code LowCardinality(String),
    reregistration_term String,
    source_url String,
    raw_entity String
)
ENGINE = ReplacingMergeTree
ORDER BY (regcode);
