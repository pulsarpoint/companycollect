CREATE DATABASE IF NOT EXISTS corpscout;

-- EODHD exchange codes are provider identifiers such as US, LSE, or XETRA.
-- operating_mic_raw preserves the source field, while eodhd_exchange_mics exposes
-- its individual ISO 10383 MIC values for joins.
CREATE TABLE IF NOT EXISTS corpscout.eodhd_exchanges
(
    exchange_code String,
    exchange_name String,
    country_name Nullable(String),
    country_iso2 LowCardinality(Nullable(String)),
    country_iso3 LowCardinality(Nullable(String)),
    currency LowCardinality(Nullable(String)),
    operating_mic_raw Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (exchange_code);

-- One row per EODHD exchange-code/MIC relationship. mic_position preserves
-- source order without asserting that the first MIC is the primary venue.
CREATE TABLE IF NOT EXISTS corpscout.eodhd_exchange_mics
(
    exchange_code String,
    mic String,
    mic_position UInt16,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (exchange_code, mic);

-- One row per EODHD-listed instrument. eodhd_symbol_key is the provider's
-- fully-qualified symbol identity (for example AAPL.US), not a company ID.
CREATE TABLE IF NOT EXISTS corpscout.eodhd_symbols
(
    eodhd_symbol_key String,
    exchange_code String,
    reported_exchange_code Nullable(String),
    ticker String,
    symbol_name String,
    country_name Nullable(String),
    currency LowCardinality(Nullable(String)),
    instrument_type LowCardinality(String),
    isin Nullable(String),
    is_delisted UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (eodhd_symbol_key);

-- Resolved listing-to-venue relationships. This is intentionally separate
-- from eodhd_exchange_mics because an umbrella exchange code may expose more
-- than one MIC and each listed instrument needs its own resolution evidence.
CREATE TABLE IF NOT EXISTS corpscout.eodhd_symbol_mics
(
    eodhd_symbol_key String,
    mic String,
    is_primary UInt8,
    resolution_method LowCardinality(String),
    resolution_confidence LowCardinality(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (eodhd_symbol_key, mic);

-- One daily OHLCV observation per EODHD listing. OHLC values are unadjusted,
-- adjusted_close is the provider-adjusted close. No TTL is set so the table
-- can retain the planned five-to-six-year history and grow beyond it.
CREATE TABLE IF NOT EXISTS corpscout.eodhd_eod_prices
(
    eodhd_symbol_key String,
    exchange_code LowCardinality(String),
    ticker String,
    price_date Date,
    open Nullable(Decimal(20, 8)),
    high Nullable(Decimal(20, 8)),
    low Nullable(Decimal(20, 8)),
    close Nullable(Decimal(20, 8)),
    adjusted_close Nullable(Decimal(20, 8)),
    volume Nullable(UInt64),
    currency LowCardinality(Nullable(String)),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    source_object_key String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(price_date)
ORDER BY (eodhd_symbol_key, price_date);
