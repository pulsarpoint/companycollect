CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.gb_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    company_number String,
    period_end_date Nullable(Date),
    fiscal_year Nullable(Int32),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 2)),
    revenue_amount_usd Nullable(Decimal(38, 2)),
    gross_profit_amount_original Nullable(Decimal(38, 2)),
    gross_profit_amount_usd Nullable(Decimal(38, 2)),
    operating_profit_amount_original Nullable(Decimal(38, 2)),
    operating_profit_amount_usd Nullable(Decimal(38, 2)),
    pretax_result_amount_original Nullable(Decimal(38, 2)),
    pretax_result_amount_usd Nullable(Decimal(38, 2)),
    net_result_amount_original Nullable(Decimal(38, 2)),
    net_result_amount_usd Nullable(Decimal(38, 2)),
    total_assets_amount_original Nullable(Decimal(38, 2)),
    total_assets_amount_usd Nullable(Decimal(38, 2)),
    fixed_assets_amount_original Nullable(Decimal(38, 2)),
    fixed_assets_amount_usd Nullable(Decimal(38, 2)),
    current_assets_amount_original Nullable(Decimal(38, 2)),
    current_assets_amount_usd Nullable(Decimal(38, 2)),
    cash_amount_original Nullable(Decimal(38, 2)),
    cash_amount_usd Nullable(Decimal(38, 2)),
    net_assets_amount_original Nullable(Decimal(38, 2)),
    net_assets_amount_usd Nullable(Decimal(38, 2)),
    equity_amount_original Nullable(Decimal(38, 2)),
    equity_amount_usd Nullable(Decimal(38, 2)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_number);
