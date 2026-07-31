CREATE DATABASE IF NOT EXISTS corpscout;

-- Back to the all-time shape. Derived data, refilled by the assets.
DROP TABLE IF EXISTS corpscout.company_market_summary;

CREATE TABLE IF NOT EXISTS corpscout.company_market_summary
(
    country_code LowCardinality(String),
    company_id String,
    tickers Array(String),
    venues UInt16,
    lead_venue LowCardinality(String),
    lead_currency LowCardinality(String),
    last_close Nullable(Decimal(38, 8)),
    last_day Nullable(Date),
    traded_usd Decimal(38, 2),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, company_id);
