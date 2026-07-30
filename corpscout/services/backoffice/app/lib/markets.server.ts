import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

/**
 * A country's traded companies, read from precomputed market facts.
 *
 * The identity behind these numbers is deterministic end to end: ESMA FIRDS
 * gives ISIN -> issuer LEI, GLEIF gives LEI -> national registration number,
 * the national register turns that into our company_id, and EODHD's ISIN gives
 * the price series. Nothing matches on a name.
 *
 * None of that runs here. `company_listings` joins instrument_venues (15.0M) to
 * instrument_issuer (9.1M) to company_identifier and takes 57 seconds for one
 * country; even the leaner identity path costs 11. The company_markets Dagster
 * assets do that work once a day into three small tables, so these queries read
 * 723 rows in ~30ms instead of recomputing a warehouse join per request.
 *
 * TURNOVER, NOT MARKET CAPITALISATION. Market cap needs shares outstanding,
 * which this warehouse holds for Brazil alone. `traded_usd` is price x volume —
 * how much money changed hands — which is a real market figure and a different
 * question from what the companies are worth. Every label says so.
 */

export type MarketMonth = { month: string; companies: number; tradedUsd: number };

export type MarketOverview = {
  companies: number;
  symbols: number;
  firstDay: string;
  lastDay: string;
  tradedUsd: number;
  perMonth: MarketMonth[];
};

export type TradedCompanyRow = {
  company_id: string;
  name: string;
  tickers: string[];
  venues: number;
  /** The venue the quoted price comes from — shown, because traded value can
   * put it on a foreign listing (Ericsson's US ADR outtrades Stockholm). */
  leadVenue: string;
  currency: string;
  lastClose: number | null;
  lastDay: string;
  tradedUsd: number;
};

/** Whether this country has traded companies, for the nav tab. */
export async function hasMarkets(country: CountryConfig): Promise<boolean> {
  const rows = await chQuery<{ n: string }>(
    `SELECT count() AS n FROM company_market_summary
     WHERE country_code = {country:String}`,
    { country: country.code.toUpperCase() },
  );
  return Number(rows[0]?.n ?? 0) > 0;
}

export async function getMarketOverview(
  country: CountryConfig,
): Promise<MarketOverview | null> {
  const code = country.code.toUpperCase();
  const [months, totals] = await Promise.all([
    chQuery<{ month: string; companies: string; symbols: string; traded: string }>(
      `SELECT toString(month) AS month,
              toString(companies) AS companies,
              toString(symbols) AS symbols,
              toString(traded_usd) AS traded
       FROM company_market_monthly
       WHERE country_code = {country:String}
       ORDER BY month`,
      { country: code },
    ),
    chQuery<{ companies: string }>(
      `SELECT toString(count()) AS companies
       FROM company_market_summary
       WHERE country_code = {country:String}`,
      { country: code },
    ),
  ]);
  if (months.length === 0) return null;

  const perMonth = months.map((m) => ({
    month: m.month,
    companies: Number(m.companies),
    tradedUsd: Number(m.traded),
  }));

  return {
    companies: Number(totals[0]?.companies ?? 0),
    // The busiest month's symbol count, so the figure means "series we hold"
    // rather than summing the same symbol once per month.
    symbols: Math.max(...months.map((m) => Number(m.symbols))),
    firstDay: perMonth[0].month,
    lastDay: perMonth[perMonth.length - 1].month,
    tradedUsd: perMonth.reduce((sum, m) => sum + m.tradedUsd, 0),
    perMonth,
  };
}

/**
 * The traded companies themselves, ranked by traded value.
 *
 * Ranked by turnover rather than by last price, because a share price is not a
 * size — a company quoted at 2,000 is not bigger than one quoted at 20.
 */
export async function getTradedCompanies(
  country: CountryConfig,
  limit = 100,
): Promise<TradedCompanyRow[]> {
  const code = country.code.toUpperCase();
  const rows = await chQuery<{
    company_id: string;
    name: string;
    tickers: string[];
    venues: string;
    lead_venue: string;
    lead_currency: string;
    last_close: string | null;
    last_day: string | null;
    traded: string;
  }>(
    // Names come from the country's OWN register, named by CountryConfig. There
    // is no companies_all: registers differ by design, so each country declares
    // its table and which columns hold the id and the display name.
    `SELECT m.company_id AS company_id,
            ifNull(any(c.name), '') AS name,
            any(m.tickers) AS tickers,
            toString(any(m.venues)) AS venues,
            any(m.lead_venue) AS lead_venue,
            any(m.lead_currency) AS lead_currency,
            toString(any(m.last_close)) AS last_close,
            toString(any(m.last_day)) AS last_day,
            toString(any(m.traded_usd)) AS traded
     FROM company_market_summary AS m
     LEFT JOIN (
       SELECT ${country.idColumn} AS company_id, any(${country.nameColumn}) AS name
       FROM ${country.companiesTable}
       GROUP BY company_id
     ) AS c ON c.company_id = m.company_id
     WHERE m.country_code = {country:String}
     GROUP BY m.company_id
     ORDER BY any(m.traded_usd) DESC
     LIMIT {limit:UInt32}`,
    { country: code, limit },
  );

  return rows.map((r) => ({
    company_id: r.company_id,
    name: r.name,
    tickers: r.tickers ?? [],
    venues: Number(r.venues),
    leadVenue: r.lead_venue,
    currency: r.lead_currency,
    lastClose: r.last_close === null ? null : Number(r.last_close),
    lastDay: r.last_day ?? "",
    tradedUsd: Number(r.traded),
  }));
}
