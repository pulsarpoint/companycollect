CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.sk_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    ico String,
    ruz_entity_id String,
    statement_id String,
    template_name LowCardinality(String),
    statement_type LowCardinality(String),
    fiscal_year Nullable(Int32),
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    filed_date Nullable(Date),
    approved_date Nullable(Date),
    currency_original LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 2)),
    revenue_amount_usd Nullable(Decimal(38, 2)),
    total_assets_amount_original Nullable(Decimal(38, 2)),
    total_assets_amount_usd Nullable(Decimal(38, 2)),
    equity_amount_original Nullable(Decimal(38, 2)),
    equity_amount_usd Nullable(Decimal(38, 2)),
    liabilities_amount_original Nullable(Decimal(38, 2)),
    liabilities_amount_usd Nullable(Decimal(38, 2)),
    pretax_result_amount_original Nullable(Decimal(38, 2)),
    pretax_result_amount_usd Nullable(Decimal(38, 2)),
    net_result_amount_original Nullable(Decimal(38, 2)),
    net_result_amount_usd Nullable(Decimal(38, 2)),
    mapped_metric_count UInt8,
    template_mapped UInt8,
    mapping_version LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_converted_at Nullable(DateTime64(3, 'UTC')),
    source_url String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (ico, statement_id);
