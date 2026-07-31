"""The SQL behind the precomputed market facts.

Kept apart from the assets so each statement can be read — and tested — on its
own. Every one is an `INSERT INTO ... SELECT` into a staging table which the
asset then swaps in, so a reader never sees a half-filled table.
"""

from __future__ import annotations

from dagster_v3.defs.company_markets import tables

# The identity resolve, one branch per identity SYSTEM rather than per country.
#
# company_traded_symbols is the seam the rest of this module and both pages sit
# on: whatever chain a market uses, it lands here as (country, company, symbol)
# and everything downstream is identical.
#
# Branch 1 -- LEI. ESMA FIRDS gives ISIN -> issuer LEI, GLEIF gives LEI ->
# national registration number, company_identifier verifies that against the
# national register. Deliberately NOT company_listings: that view adds
# instrument_venues (15.0M rows) to answer "where is it admitted", which this
# does not need, and costs 57s per country instead of 11s.
#
# Branch 2 -- CNPJ. The EU chain cannot reach Brazil at all: there is no LEI
# mandate (4,259 Brazilian LEIs against 119,035 Swedish), and FIRDS records only
# EU/EEA admissions, so a B3-listed share is absent from it entirely. B3's own
# register publishes CNPJ and the trading-code root together, which closes both
# gaps, and the INNER JOIN to br_companies keeps it register-verified in the
# same sense branch 1 is -- no name matching anywhere.
TRADED_SYMBOLS_SELECT = """
SELECT
    ci.country_code AS country_code,
    ci.company_id AS company_id,
    ii.isin AS isin,
    s.eodhd_symbol_key AS eodhd_symbol_key,
    any(s.ticker) AS ticker,
    any(s.exchange_code) AS exchange_code,
    %(resolved_at)s AS resolved_at
FROM corpscout.company_identifier AS ci
INNER JOIN corpscout.instrument_issuer AS ii
    ON ii.issuer_scheme = ci.issuer_scheme AND ii.issuer_id = ci.issuer_id
INNER JOIN corpscout.eodhd_symbols AS s ON s.isin = ii.isin
WHERE ci.is_current = 1
GROUP BY country_code, company_id, isin, eodhd_symbol_key

UNION ALL

SELECT
    'BR' AS country_code,
    b.cnpj_basico AS company_id,
    ifNull(any(s.isin), '') AS isin,
    s.eodhd_symbol_key AS eodhd_symbol_key,
    any(s.ticker) AS ticker,
    any(s.exchange_code) AS exchange_code,
    %(resolved_at)s AS resolved_at
FROM corpscout.br_b3_listings AS b
-- The ticker root is what a Brazilian ISIN encodes and what EODHD's symbol
-- starts with: PETR4 and BRPETRACNPR6 both carry PETR.
INNER JOIN corpscout.eodhd_symbols AS s
    ON s.exchange_code = 'SA' AND substring(s.ticker, 1, 4) = b.ticker_root
-- The register is the proof, exactly as company_identifier is for branch 1.
INNER JOIN (SELECT DISTINCT cnpj_basico FROM corpscout.br_companies) AS c
    ON c.cnpj_basico = b.cnpj_basico
WHERE b.cnpj_basico != '' AND b.ticker_root != ''
GROUP BY country_code, company_id, eodhd_symbol_key
"""

# Daily traded value per (country, company, symbol), in USD.
#
# FX runs through a small calendar rather than a row-by-row join: there are 496
# distinct price dates and six currencies, so the ASOF covers a few thousand
# rows instead of millions. ASOF rather than equality because ECB rates land on
# business days and trail the last price by up to three weeks — an equality
# join would silently drop the most recent days. Carrying the last published
# rate forward is what a missing business day means.
_TRADED_CTE = """
WITH
-- No aggregate in here. ClickHouse inlines a CTE textually, so aggregating
-- both here and in per_symbol below nests one inside the other and fails with
-- ILLEGAL_AGGREGATION. The DISTINCT does the same de-duplication -- a symbol
-- reachable through two ISINs collapses to one row -- without aggregating.
px AS (
    SELECT
        t.country_code AS country_code,
        t.company_id AS company_id,
        t.eodhd_symbol_key AS symbol_key,
        t.ticker AS ticker,
        t.exchange_code AS exchange_code,
        p.price_date AS price_date,
        toString(p.currency) AS ccy,
        toFloat64(p.close) AS close_native,
        toFloat64(p.close) * p.volume AS traded_native
    FROM (
        SELECT DISTINCT country_code, company_id, eodhd_symbol_key,
                        ticker, exchange_code
        FROM corpscout.company_traded_symbols
    ) AS t
    INNER JOIN corpscout.eodhd_eod_prices AS p
        ON p.eodhd_symbol_key = t.eodhd_symbol_key
    WHERE p.volume > 0 AND p.close IS NOT NULL
),
fx AS (
    SELECT
        r.rate_date AS rate_date,
        toString(r.quote_currency) AS ccy,
        toFloat64(u.rate) / toFloat64(r.rate) AS to_usd
    FROM corpscout.exchange_rates AS r
    INNER JOIN (
        SELECT rate_date, rate FROM corpscout.exchange_rates
        WHERE base_currency = 'EUR' AND quote_currency = 'USD'
    ) AS u ON u.rate_date = r.rate_date
    WHERE r.base_currency = 'EUR'
),
factor AS (
    SELECT cal.price_date AS price_date, cal.ccy AS ccy, fx.to_usd AS to_usd
    FROM (SELECT DISTINCT price_date, ccy FROM px) AS cal
    ASOF LEFT JOIN fx ON fx.ccy = cal.ccy AND fx.rate_date <= cal.price_date
)
"""

MARKET_MONTHLY_SELECT = (
    _TRADED_CTE
    + """
SELECT
    px.country_code AS country_code,
    toStartOfMonth(px.price_date) AS month,
    toUInt32(uniqExact(px.company_id)) AS companies,
    toUInt32(uniqExact(px.symbol_key)) AS symbols,
    toDecimal64(sum(px.traded_native * factor.to_usd), 2) AS traded_usd,
    %(resolved_at)s AS resolved_at
FROM px
INNER JOIN factor
    ON factor.price_date = px.price_date AND factor.ccy = px.ccy
GROUP BY country_code, month
"""
)

# One row per traded company PER YEAR, folded across venues.
#
# Per year because the country overview lets a reader pick one, and an
# all-time total cannot answer "who traded most in 2022". The Markets tab
# sums across years for its all-time view.
#
# The quoted price comes from the symbol with the most traded value, not from
# whichever venue reported last: that put Ericsson at EUR 8.67 and Volvo at USD
# 37.59 rather than their home prices.
MARKET_SUMMARY_SELECT = (
    _TRADED_CTE
    + """
SELECT
    country_code,
    year,
    company_id,
    groupUniqArray(symbol_ticker) AS tickers,
    toUInt16(uniqExact(symbol_exchange)) AS venues,
    argMax(symbol_exchange, symbol_traded_usd) AS lead_venue,
    argMax(symbol_ccy, symbol_traded_usd) AS lead_currency,
    toDecimal64(argMax(symbol_last_close, symbol_traded_usd), 8) AS last_close,
    argMax(symbol_last_day, symbol_traded_usd) AS last_day,
    toDecimal64(sum(symbol_traded_usd), 2) AS traded_usd,
    %(resolved_at)s AS resolved_at
FROM (
    -- Inner aliases are prefixed so they cannot collide with the outer output
    -- names. Naming both levels `traded_usd` made ClickHouse resolve the outer
    -- sum(traded_usd) as sum(sum(...)) and fail with ILLEGAL_AGGREGATION --
    -- alias shadowing, not scoping.
    SELECT
        px.country_code AS country_code,
        toYear(px.price_date) AS year,
        px.company_id AS company_id,
        px.symbol_key AS symbol_key,
        any(px.ticker) AS symbol_ticker,
        any(px.exchange_code) AS symbol_exchange,
        argMax(px.close_native, px.price_date) AS symbol_last_close,
        argMax(px.ccy, px.price_date) AS symbol_ccy,
        max(px.price_date) AS symbol_last_day,
        sum(px.traded_native * factor.to_usd) AS symbol_traded_usd
    FROM px
    INNER JOIN factor
        ON factor.price_date = px.price_date AND factor.ccy = px.ccy
    GROUP BY country_code, year, company_id, symbol_key
) AS per_symbol
GROUP BY country_code, year, company_id
"""
)

SELECT_BY_TABLE = {
    tables.TRADED_SYMBOLS_TABLE: TRADED_SYMBOLS_SELECT,
    tables.MARKET_MONTHLY_TABLE: MARKET_MONTHLY_SELECT,
    tables.MARKET_SUMMARY_TABLE: MARKET_SUMMARY_SELECT,
}
