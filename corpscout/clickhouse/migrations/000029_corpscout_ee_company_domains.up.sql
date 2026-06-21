CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ee_company_domains
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    reg_code String,
    domain String,
    domain_source LowCardinality(String),
    website_url String,
    website_normalized_url String,
    website_host String,
    is_current UInt8,
    is_primary UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (reg_code, domain);
