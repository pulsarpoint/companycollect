CREATE DATABASE IF NOT EXISTS corpscout;

-- Give the per-company market summary a YEAR dimension.
--
-- It held one all-time row per company, which cannot answer "who traded most in
-- 2022" -- the question the country overview's year selector asks of every
-- other card. EODHD history is being extended to five years, so the shape has
-- to carry the year before that data lands rather than after.
--
-- Dropped and recreated because year belongs in the sort key, which ALTER
-- cannot change. Safe because this table is DERIVED: the company_markets
-- assets rebuild it in full on every run, so nothing here is a source of
-- record and the next materialisation restores it within minutes.
--
-- The all-time view the Markets tab shows becomes a sum across years, with the
-- quoted price taken from the most recent one.
DROP TABLE IF EXISTS corpscout.company_market_summary;

CREATE TABLE IF NOT EXISTS corpscout.company_market_summary
(
    country_code LowCardinality(String),
    year UInt16,
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
ORDER BY (country_code, year, company_id);
