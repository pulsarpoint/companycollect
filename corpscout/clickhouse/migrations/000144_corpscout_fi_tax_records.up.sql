CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_tax_records
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    business_id String,
    taxpayer_name String,
    municipality_code LowCardinality(String),
    municipality_name LowCardinality(String),
    tax_year Int32,
    period_end_date Date,
    currency LowCardinality(String),
    taxable_income_amount_original Nullable(Decimal(38, 2)),
    taxable_income_amount_usd Nullable(Decimal(38, 2)),
    taxes_total_amount_original Nullable(Decimal(38, 2)),
    taxes_total_amount_usd Nullable(Decimal(38, 2)),
    prepayments_total_amount_original Nullable(Decimal(38, 2)),
    prepayments_total_amount_usd Nullable(Decimal(38, 2)),
    tax_refund_amount_original Nullable(Decimal(38, 2)),
    tax_refund_amount_usd Nullable(Decimal(38, 2)),
    residual_tax_amount_original Nullable(Decimal(38, 2)),
    residual_tax_amount_usd Nullable(Decimal(38, 2)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (business_id, tax_year);
