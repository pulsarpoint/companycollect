EXCHANGE_RATES_DATABASE = "reference"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_EXCHANGE_RATES_TABLE = f"{EXCHANGE_RATES_DATABASE}.{EXCHANGE_RATES_TABLE}"

EXCHANGE_RATES_COLUMNS = (
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
    "_dlt_load_id",
    "_dlt_id",
)

EXCHANGE_RATES_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_EXCHANGE_RATES_TABLE}
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
ENGINE = MergeTree
ORDER BY (quote_currency, base_currency, rate_date, source)
"""
