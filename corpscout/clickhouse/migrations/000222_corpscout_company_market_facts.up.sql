CREATE DATABASE IF NOT EXISTS corpscout;

-- Precomputed market facts, so a country page does not recompute them per request.
--
-- The natural source, company_listings, joins instrument_venues (15.0M) to
-- instrument_issuer (9.1M) to company_identifier and takes 57 seconds for one
-- country, while each underlying table answers in under half a second. Even the
-- cheaper identity path (company_identifier -> instrument_issuer ->
-- eodhd_symbols) costs 11 seconds. Neither belongs in a page load, and both
-- change only when a register, FIRDS or the daily price load advances.
--
-- Filled by the company_markets Dagster assets, which own the data.

-- Which EODHD symbol belongs to which company. The expensive resolve, done once.
CREATE TABLE IF NOT EXISTS corpscout.company_traded_symbols
(
    country_code LowCardinality(String),
    company_id String,
    isin String,
    eodhd_symbol_key String,
    ticker String,
    exchange_code LowCardinality(String),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, company_id, eodhd_symbol_key);

-- The country chart: one row per (country, month).
--
-- traded_usd is TURNOVER -- price x volume -- not market capitalisation, which
-- would need shares outstanding. This warehouse holds those for Brazil alone,
-- so the column is named for what it is.
CREATE TABLE IF NOT EXISTS corpscout.company_market_monthly
(
    country_code LowCardinality(String),
    month Date,
    companies UInt32,
    symbols UInt32,
    traded_usd Decimal(38, 2),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, month);

-- The country table: one row per traded company, already folded across venues.
CREATE TABLE IF NOT EXISTS corpscout.company_market_summary
(
    country_code LowCardinality(String),
    company_id String,
    tickers Array(String),
    venues UInt16,
    -- The venue the quoted price comes from, chosen by traded value rather than
    -- by whichever venue reported last. Taking the latest close across venues
    -- quoted Ericsson in EUR and Volvo in USD instead of their home prices.
    lead_venue LowCardinality(String),
    lead_currency LowCardinality(String),
    last_close Nullable(Decimal(38, 8)),
    last_day Nullable(Date),
    traded_usd Decimal(38, 2),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, company_id);
