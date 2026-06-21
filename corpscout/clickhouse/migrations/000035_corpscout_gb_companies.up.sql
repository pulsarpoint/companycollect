CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.gb_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    company_number String,
    name String,
    company_category LowCardinality(String),
    company_status LowCardinality(String),
    is_active UInt8,
    incorporation_date Nullable(Date),
    dissolution_date Nullable(Date),
    address String,
    address_line_2 String,
    postal_code LowCardinality(String),
    city LowCardinality(String),
    county LowCardinality(String),
    country LowCardinality(String),
    country_of_origin LowCardinality(String),
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (company_number);
