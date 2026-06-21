CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ee_company_contacts
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    reg_code String,
    contact_type LowCardinality(String),
    contact_type_en LowCardinality(String),
    contact_value String,
    is_current UInt8,
    end_date Nullable(Date),
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (reg_code, contact_type, contact_value);
