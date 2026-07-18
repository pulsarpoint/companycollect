CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_financial_history
(
    company_id String,
    fiscal_year Int32,
    observation LowCardinality(String),
    source_statement_key String,
    source_fiscal_year Int32,
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    result_after_financial_items_amount_original Nullable(Float64),
    result_after_financial_items_amount_usd Nullable(Float64),
    solidity_pct Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year);
