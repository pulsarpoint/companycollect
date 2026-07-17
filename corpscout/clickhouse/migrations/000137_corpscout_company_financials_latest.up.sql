CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.no_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.fi_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.se_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.ee_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.lv_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.gb_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.br_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS corpscout.sk_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;
