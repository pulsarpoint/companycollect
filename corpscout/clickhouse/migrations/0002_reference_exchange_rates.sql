CREATE DATABASE IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.exchange_rates
(
    rate_date Date,
    base_currency LowCardinality(String),
    quote_currency LowCardinality(String),
    rate Decimal(38, 12),
    source LowCardinality(String),
    source_url String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC'),
    _dlt_load_id String,
    _dlt_id String
)
ENGINE = ReplacingMergeTree(pulled_at)
ORDER BY (quote_currency, base_currency, rate_date, source);
