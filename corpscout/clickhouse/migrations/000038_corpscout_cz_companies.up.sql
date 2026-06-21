CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.cz_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    ico String,
    name String,
    legal_form_code LowCardinality(String),
    legal_form_en LowCardinality(String),
    is_active UInt8,
    established_date Nullable(Date),
    terminated_date Nullable(Date),
    address String,
    postal_code LowCardinality(String),
    city LowCardinality(String),
    city_part String,
    street String,
    district_code LowCardinality(String),
    size_category LowCardinality(String),
    institutional_sector LowCardinality(String),
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (ico);
