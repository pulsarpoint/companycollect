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
  /** The year every headline figure describes. Never an unbounded total. */
  year: number;
  availableYears: number[];
  /** Whether `year` is still running, so the page can say so. */
  partial: boolean;
  companies: number;
  /** Companies with meaningful turnover, as opposed to a nominal listing. */
  activeCompanies: number;
  tradedUsd: number;
  /** Turnover set aside by the plausibility guard, and the days it came from.
   * Reported rather than silently netted off: EODHD adjusts price for a
   * reverse split and leaves volume on the pre-split share count, so those
   * days are an artefact — but a guard that quietly changes a published
   * number is its own kind of bug. See
   * dagster_v3/docs/market-turnover-plausibility.md. */
  excludedUsd: number;
  excludedDays: number;
  /** Every month held, not just the selected year — the trend is the point. */
  perMonth: MarketMonth[];
};

/**
 * Turnover below which a listing is nominal rather than traded.
 *
 * 67 Swedish companies have Frankfurt as their busiest venue and essentially
 * zero traded value: admitted somewhere, traded nowhere. Counting them as
 * "traded companies" overstates the market, so they are counted separately
 * rather than dropped — they ARE listed, which is a fact worth keeping.
 */
const NOMINAL_TURNOVER_USD = 1_000_000;

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

/**
 * The default period: the latest year that has fully elapsed.
 *
 * Not the latest year with data, and never a total across everything held.
 * "Traded value over 31 months" was a number that grew because the backfill
 * advanced, not because anything happened in Sweden — it answered no question.
 * A completed calendar year is a period a reader can reason about, and the
 * running year stays selectable, marked as incomplete.
 */
function defaultMarketYear(years: number[], now = new Date()): number | null {
  if (years.length === 0) return null;
  const sorted = [...years].sort((a, b) => a - b);
  const complete = sorted.filter((y) => y < now.getUTCFullYear());
  return complete.length > 0 ? complete[complete.length - 1] : sorted[sorted.length - 1];
}

export async function getMarketOverview(
  country: CountryConfig,
  requestedYear?: number | null,
): Promise<MarketOverview | null> {
  const code = country.code.toUpperCase();
  const yearRows = await chQuery<{ year: string }>(
    `SELECT DISTINCT toString(year) AS year FROM company_market_summary
     WHERE country_code = {country:String} ORDER BY year`,
    { country: code },
  );
  const availableYears = yearRows.map((r) => Number(r.year));
  if (availableYears.length === 0) return null;
  const year =
    requestedYear != null && availableYears.includes(requestedYear)
      ? requestedYear
      : defaultMarketYear(availableYears)!;

  const [months, totals] = await Promise.all([
    chQuery<{
      month: string;
      companies: string;
      symbols: string;
      traded: string;
      excluded: string;
      excluded_days: string;
    }>(
      `SELECT toString(month) AS month,
              toString(companies) AS companies,
              toString(symbols) AS symbols,
              toString(traded_usd) AS traded,
              toString(excluded_usd) AS excluded,
              toString(excluded_days) AS excluded_days
       FROM company_market_monthly
       WHERE country_code = {country:String}
       ORDER BY month`,
      { country: code },
    ),
    chQuery<{ companies: string; active: string; traded: string }>(
      `SELECT toString(uniqExact(company_id)) AS companies,
              toString(uniqExactIf(company_id, traded_usd >= ${NOMINAL_TURNOVER_USD})) AS active,
              toString(sum(traded_usd)) AS traded
       FROM company_market_summary
       WHERE country_code = {country:String} AND year = {year:UInt16}`,
      { country: code, year },
    ),
  ]);
  if (months.length === 0) return null;

  const perMonth = months.map((m) => ({
    month: m.month,
    companies: Number(m.companies),
    tradedUsd: Number(m.traded),
  }));

  return {
    year,
    availableYears,
    partial: year >= new Date().getUTCFullYear(),
    companies: Number(totals[0]?.companies ?? 0),
    activeCompanies: Number(totals[0]?.active ?? 0),
    tradedUsd: Number(totals[0]?.traded ?? 0),
    // Scoped to the selected year, like every other headline figure.
    excludedUsd: months
      .filter((m) => m.month.startsWith(String(year)))
      .reduce((sum, m) => sum + Number(m.excluded), 0),
    excludedDays: months
      .filter((m) => m.month.startsWith(String(year)))
      .reduce((sum, m) => sum + Number(m.excluded_days), 0),
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
  year?: number | null,
): Promise<TradedCompanyRow[]> {
  const code = country.code.toUpperCase();
  // company_market_summary is keyed per (country, year, company). Without a
  // year the rows are folded to an all-time view: traded value sums, and the
  // quoted price comes from the most recent year rather than an arbitrary one.
  const yearFilter = year == null ? "" : " AND m.year = {year:UInt16}";
  const rows = await chQuery<{
    company_id: string;
    tickers: string[];
    venues: string;
    lead_venue: string;
    lead_currency: string;
    last_close: string | null;
    last_day: string | null;
    traded: string;
  }>(
    // Ranked WITHOUT the register join. Joining a grouped se_companies (1.9M
    // rows) inline cost 1.28s against 0.03s for the ranking alone: the whole
    // register was being scanned to name a hundred rows.
    `SELECT m.company_id AS company_id,
            arrayDistinct(arrayFlatten(groupArray(m.tickers))) AS tickers,
            toString(max(m.venues)) AS venues,
            argMax(m.lead_venue, m.year) AS lead_venue,
            argMax(m.lead_currency, m.year) AS lead_currency,
            toString(argMax(m.last_close, m.year)) AS last_close,
            toString(argMax(m.last_day, m.year)) AS last_day,
            toString(sum(m.traded_usd)) AS traded
     FROM company_market_summary AS m
     WHERE m.country_code = {country:String}${yearFilter}
     GROUP BY m.company_id
     ORDER BY sum(m.traded_usd) DESC
     LIMIT {limit:UInt32}`,
    year == null ? { country: code, limit } : { country: code, limit, year },
  );

  // Names for the rows actually shown — an indexed lookup of at most `limit`
  // ids. Registers differ by design, so each country declares its table and
  // which columns hold the id and the display name.
  const nameById = new Map<string, string>();
  if (rows.length > 0) {
    const names = await chQuery<{ company_id: string; name: string }>(
      `SELECT toString(${country.idColumn}) AS company_id,
              any(${country.nameColumn}) AS name
       FROM ${country.companiesTable}
       WHERE ${country.idColumn} IN {ids:Array(String)}
       GROUP BY company_id`,
      { ids: rows.map((r) => r.company_id) },
    );
    for (const n of names) nameById.set(n.company_id, n.name);
  }

  return rows.map((r) => ({
    company_id: r.company_id,
    name: nameById.get(r.company_id) ?? "",
    tickers: r.tickers ?? [],
    venues: Number(r.venues),
    leadVenue: r.lead_venue,
    currency: r.lead_currency,
    lastClose: r.last_close === null ? null : Number(r.last_close),
    lastDay: r.last_day ?? "",
    tradedUsd: Number(r.traded),
  }));
}
