CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.companies_all
(
    country_code LowCardinality(String),
    company_id String,
    name String,
    name_normalized String,
    is_active UInt8,
    status String,
    legal_form String,
    place String,
    size String,
    industry_code String,
    industry_label String,
    revenue_usd Nullable(Float64),
    fiscal_year Nullable(Int32),
    employees Nullable(Float64),
    has_financials UInt8,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_name_ngram name_normalized TYPE ngrambf_v1(3, 262144, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (country_code, company_id);
