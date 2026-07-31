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
# Branch 1 -- LEI. FIRDS and GLEIF both give ISIN -> issuer LEI, GLEIF gives LEI ->
# national registration number, company_identifier verifies that against the
# national register. Deliberately NOT company_listings: that view adds
# instrument_venues (15.0M rows) to answer "where is it admitted", which this
# does not need, and costs 57s per country instead of 11s.
#
# Branch 2 -- CNPJ + B3 instruments. The EU chain cannot reach Brazil at all: there is no LEI
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
INNER JOIN (
    -- ESMA FIRDS and GLEIF answer the same question with different reach.
    -- FIRDS records EU/EEA admissions only; GLEIF's ISIN-to-LEI file is
    -- published worldwide with ANNA and knows 98,390 issuers against FIRDS's
    -- 39,675 -- 77,648 of them absent from FIRDS entirely, in Delaware,
    -- Turkey, the Caymans and elsewhere. Unioned rather than swapped: FIRDS
    -- carries EU instruments GLEIF has not been given, and the GROUP BY below
    -- collapses the overlap.
    SELECT issuer_scheme, issuer_id, isin FROM corpscout.instrument_issuer
    UNION DISTINCT
    SELECT 'lei' AS issuer_scheme, lei AS issuer_id, isin
    FROM corpscout.gleif_isin_lei
) AS ii
    ON ii.issuer_scheme = ci.issuer_scheme AND ii.issuer_id = ci.issuer_id
INNER JOIN corpscout.eodhd_symbols AS s ON s.isin = ii.isin
WHERE ci.is_current = 1
GROUP BY country_code, company_id, isin, eodhd_symbol_key

UNION ALL

SELECT
    'BR' AS country_code,
    i.cnpj_basico AS company_id,
    any(i.isin) AS isin,
    s.eodhd_symbol_key AS eodhd_symbol_key,
    any(s.ticker) AS ticker,
    any(s.exchange_code) AS exchange_code,
    %(resolved_at)s AS resolved_at
FROM corpscout.br_b3_instruments AS i
INNER JOIN corpscout.eodhd_symbols AS s
    ON s.exchange_code = 'SA'
    -- ISIN first, because B3 publishes the pairing and a match on it is a
    -- fact. The exact trading code is the fallback for symbols EODHD carries
    -- without an ISIN, and the root prefix catches the fractional-lot codes
    -- (PETR4F) that B3 does not list separately but that trade all the same.
    AND (
        (i.isin != '' AND s.isin = i.isin)
        OR s.ticker = i.ticker
        OR (i.ticker_root != '' AND substring(s.ticker, 1, 4) = i.ticker_root)
    )
-- The register is the proof, exactly as company_identifier is for branch 1.
INNER JOIN (SELECT DISTINCT cnpj_basico FROM corpscout.br_companies) AS c
    ON c.cnpj_basico = i.cnpj_basico
WHERE i.cnpj_basico != ''
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
-- The turnover plausibility guard.
--
-- EODHD adjusts price for a reverse split and leaves volume on the pre-split
-- share count: across 46 B3 jump events, price multiplies by a median 20x
-- while volume moves 0.80x. close x volume then mixes two share bases. Azul's
-- December 2025 split produced four days worth 538bn BRL and put a distressed
-- airline above Petrobras.
--
-- Judged per SYMBOL against its OWN median day, so a volatile small cap is
-- compared to itself rather than to Petrobras. The threshold is measured, not
-- guessed: a normal busy day is 32x the median (p99) and p99.9 is 253x, while
-- 200x removes a tenth of Brazil's 2025 value against three thousandths of
-- Sweden's. Cutting lower, at 50x, would discard a fiftieth of Sweden's
-- genuine trading to gain nothing in Brazil, whose inflation is all far out
-- in the tail. Figures in docs/market-turnover-plausibility.md.
--
-- Never write a literal per-cent sign in this file, comments included. The
-- ClickHouse driver binds parameters by that character, so one inside a
-- COMMENT is still read as a format spec and the whole statement dies with
-- "an integer is required, not dict". Spell the figures out in words instead.
--
-- 30 days of history minimum: below that there is no basis to judge a symbol,
-- and it passes through unguarded rather than being cut on a thin median.
symbol_median AS (
    SELECT symbol_key, median(traded_native) AS median_traded, count() AS days
    FROM px
    GROUP BY symbol_key
    HAVING days >= 30 AND median_traded > 0
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
),
-- Flagged, not filtered, so the amount set aside can be reported.
pxg AS (
    SELECT px.*,
           toUInt8(
               m.median_traded IS NOT NULL
               AND px.traded_native > m.median_traded * 200
           ) AS implausible
    FROM px
    LEFT JOIN symbol_median AS m ON m.symbol_key = px.symbol_key
)
"""

MARKET_MONTHLY_SELECT = (
    _TRADED_CTE
    + """
SELECT
    pxg.country_code AS country_code,
    toStartOfMonth(pxg.price_date) AS month,
    toUInt32(uniqExactIf(pxg.company_id, pxg.implausible = 0)) AS companies,
    toUInt32(uniqExactIf(pxg.symbol_key, pxg.implausible = 0)) AS symbols,
    toDecimal64(sumIf(pxg.traded_native * factor.to_usd, pxg.implausible = 0), 2)
        AS traded_usd,
    %(resolved_at)s AS resolved_at,
    toUInt32(countIf(pxg.implausible = 1)) AS excluded_days,
    toDecimal64(sumIf(pxg.traded_native * factor.to_usd, pxg.implausible = 1), 2)
        AS excluded_usd
FROM pxg
INNER JOIN factor
    ON factor.price_date = pxg.price_date AND factor.ccy = pxg.ccy
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
        pxg.country_code AS country_code,
        toYear(pxg.price_date) AS year,
        pxg.company_id AS company_id,
        pxg.symbol_key AS symbol_key,
        any(pxg.ticker) AS symbol_ticker,
        any(pxg.exchange_code) AS symbol_exchange,
        argMax(pxg.close_native, pxg.price_date) AS symbol_last_close,
        argMax(pxg.ccy, pxg.price_date) AS symbol_ccy,
        max(pxg.price_date) AS symbol_last_day,
        sum(pxg.traded_native * factor.to_usd) AS symbol_traded_usd
    FROM pxg
    INNER JOIN factor
        ON factor.price_date = pxg.price_date AND factor.ccy = pxg.ccy
    -- The same guard the monthly totals apply. Reading raw px here left Azul
    -- at 99.4bn USD in the company ranking while the country total had already
    -- set it aside -- two published numbers disagreeing about the same days.
    WHERE pxg.implausible = 0
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
