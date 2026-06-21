CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ee_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_line_number UInt64,
    source_record_id String,
    reg_code String,
    name String,
    vat_id String,
    legal_form_original String,
    legal_form_en String,
    legal_form_subtype_original String,
    legal_form_subtype_en String,
    status_code LowCardinality(String),
    status_original String,
    status_en String,
    is_active UInt8,
    first_entry_date Nullable(Date),
    address String,
    ehak_code LowCardinality(String),
    location String,
    postal_code String,
    address_id String,
    company_url String,
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (reg_code);
