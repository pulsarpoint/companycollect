CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    statement_id String,
    regcode String,
    fiscal_year Nullable(Int32),
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    employees Nullable(Int64),
    currency LowCardinality(String),
    rounded_to_nearest LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 2)),
    revenue_amount_usd Nullable(Decimal(38, 2)),
    gross_profit_amount_original Nullable(Decimal(38, 2)),
    gross_profit_amount_usd Nullable(Decimal(38, 2)),
    pretax_result_amount_original Nullable(Decimal(38, 2)),
    pretax_result_amount_usd Nullable(Decimal(38, 2)),
    net_result_amount_original Nullable(Decimal(38, 2)),
    net_result_amount_usd Nullable(Decimal(38, 2)),
    total_assets_amount_original Nullable(Decimal(38, 2)),
    total_assets_amount_usd Nullable(Decimal(38, 2)),
    current_assets_amount_original Nullable(Decimal(38, 2)),
    current_assets_amount_usd Nullable(Decimal(38, 2)),
    non_current_assets_amount_original Nullable(Decimal(38, 2)),
    non_current_assets_amount_usd Nullable(Decimal(38, 2)),
    equity_amount_original Nullable(Decimal(38, 2)),
    equity_amount_usd Nullable(Decimal(38, 2)),
    current_liabilities_amount_original Nullable(Decimal(38, 2)),
    current_liabilities_amount_usd Nullable(Decimal(38, 2)),
    non_current_liabilities_amount_original Nullable(Decimal(38, 2)),
    non_current_liabilities_amount_usd Nullable(Decimal(38, 2)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (regcode, statement_id);
