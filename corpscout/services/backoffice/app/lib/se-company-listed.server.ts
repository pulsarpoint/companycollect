import { chQuery } from "~/lib/clickhouse.server";

/** One current LEI linked to the company in corpscout.company_identifier. */
export interface SeCompanyLeiRow {
  lei: string;
  entity_status: string;
  registration_status: string;
}

/** One listed line/venue from corpscout.company_traded_symbols. Cross-listings
 * are real rows: Handelsbanken has an SHB-A line on ST plus LSE lines. */
export interface SeCompanyTradedSymbolRow {
  isin: string;
  eodhd_symbol_key: string;
  ticker: string;
  exchange_code: string;
}

/** The company's latest-year market summary from company_market_summary. */
export interface SeCompanyMarketSummary {
  year: number;
  venues: number;
  lead_venue: string;
  lead_currency: string;
  last_close: number | null;
  last_day: string;
  /** TURNOVER (price x volume) in USD, never market capitalisation — the
   * warehouse holds shares outstanding for Brazil alone (migration 000222). */
  traded_usd: number;
}

/** One daily close for the lead symbol, oldest first. */
export interface SeCompanyPricePoint {
  price_date: string;
  close: number;
}

/**
 * company_identifier is a plain MergeTree snapshot, but succession can leave
 * several versions of one link, so the statuses are argMax'd by resolved_at
 * per LEI instead of trusting the physical rows to be unique.
 */
export const COMPANY_LEI_SQL = `SELECT
  i.issuer_id AS lei,
  argMax(toString(i.entity_status), i.resolved_at) AS entity_status,
  argMax(toString(i.registration_status), i.resolved_at) AS registration_status
FROM corpscout.company_identifier AS i
WHERE i.issuer_scheme = 'lei'
  AND i.country_code = 'SE'
  AND i.is_current = 1
  AND i.company_id = {companyId:String}
GROUP BY lei
ORDER BY lei
LIMIT 50`;

/**
 * The company's listed lines, one per venue. company_traded_symbols is a
 * derived MergeTree the company_markets Dagster assets rebuild in full, so no
 * FINAL and no dedup — the physical rows are the listing list.
 */
export const COMPANY_TRADED_SYMBOLS_SQL = `SELECT
  s.isin AS isin,
  s.eodhd_symbol_key AS eodhd_symbol_key,
  s.ticker AS ticker,
  toString(s.exchange_code) AS exchange_code
FROM corpscout.company_traded_symbols AS s
WHERE s.country_code = 'SE'
  AND s.company_id = {companyId:String}
ORDER BY s.eodhd_symbol_key
LIMIT 100`;

/**
 * company_market_summary is keyed per (country, year, company) since migration
 * 000223, so the tab reads the most recent year's row: quoted price, its venue
 * and currency (chosen by traded value — see migration 000222's comments), and
 * that year's turnover. traded_usd is TURNOVER, never market cap.
 */
export const COMPANY_MARKET_SUMMARY_SQL = `SELECT
  toString(m.year) AS year,
  toString(m.venues) AS venues,
  toString(m.lead_venue) AS lead_venue,
  toString(m.lead_currency) AS lead_currency,
  toString(m.last_close) AS last_close,
  toString(m.last_day) AS last_day,
  toString(m.traded_usd) AS traded_usd
FROM corpscout.company_market_summary AS m
WHERE m.country_code = 'SE'
  AND m.company_id = {companyId:String}
ORDER BY m.year DESC
LIMIT 1`;

/**
 * One year of daily closes for ONE symbol — a keyed read on the table's own
 * primary key (eodhd_symbol_key, price_date), so it never scans the 15M-row
 * price history. eodhd_eod_prices is a ReplacingMergeTree on retrieved_at, so
 * FINAL: a re-fetched day must show once, in its newest state. A trading year
 * has ~260 sessions; LIMIT 400 bounds a malformed backfill without ever
 * clipping a real year.
 */
export const COMPANY_LEAD_PRICES_SQL = `SELECT
  toString(p.price_date) AS price_date,
  toFloat64(p.close) AS close
FROM corpscout.eodhd_eod_prices AS p FINAL
WHERE p.eodhd_symbol_key = {symbolKey:String}
  AND p.price_date >= today() - INTERVAL 1 YEAR
  AND p.close IS NOT NULL
ORDER BY p.price_date
LIMIT 400`;

export interface SeCompanyListed {
  leis: SeCompanyLeiRow[];
  symbols: SeCompanyTradedSymbolRow[];
  summary: SeCompanyMarketSummary | null;
  /** The symbol the chart and quote describe: the traded_symbols row on the
   * summary's lead venue, or the first row when the venues disagree. Empty
   * string when the company is not traded. */
  leadSymbolKey: string;
  prices: SeCompanyPricePoint[];
}

interface RawSummaryRow {
  year: string;
  venues: string;
  lead_venue: string;
  lead_currency: string;
  last_close: string | null;
  last_day: string | null;
  traded_usd: string;
}

/** The lead line: the row quoted prices come from. The summary names its
 * venue; when no row sits on that venue (or there is no summary at all) the
 * first row stands in, so a traded company always has a chartable symbol. */
export function pickLeadSymbol(
  symbols: SeCompanyTradedSymbolRow[],
  summary: SeCompanyMarketSummary | null,
): SeCompanyTradedSymbolRow | null {
  if (symbols.length === 0) return null;
  if (summary !== null) {
    const onLeadVenue = symbols.find(
      (s) => s.exchange_code === summary.lead_venue,
    );
    if (onLeadVenue !== undefined) return onLeadVenue;
  }
  return symbols[0];
}

/**
 * The company's public-market state, from the EODHD market facts: its listed
 * lines (company_traded_symbols), the latest-year quote and turnover
 * (company_market_summary), a year of daily closes for the lead line
 * (eodhd_eod_prices), and its current LEI(s) as identity context.
 *
 * "Publicly traded" here means an EODHD symbol resolved to this company
 * through the deterministic ISIN -> LEI -> register identity chain — NOT that
 * an ESEF filing exists. The summary can lag the symbol resolve (it is a
 * separate asset), so a symbols-without-summary state is real and rendered.
 */
export async function loadSeCompanyListed(
  companyId: string,
): Promise<SeCompanyListed> {
  const [leis, symbols, summaryRows] = await Promise.all([
    chQuery<SeCompanyLeiRow>(COMPANY_LEI_SQL, { companyId }),
    chQuery<SeCompanyTradedSymbolRow>(COMPANY_TRADED_SYMBOLS_SQL, {
      companyId,
    }),
    chQuery<RawSummaryRow>(COMPANY_MARKET_SUMMARY_SQL, { companyId }),
  ]);

  const raw = summaryRows[0];
  const summary: SeCompanyMarketSummary | null =
    raw === undefined
      ? null
      : {
          year: Number(raw.year),
          venues: Number(raw.venues),
          lead_venue: raw.lead_venue,
          lead_currency: raw.lead_currency,
          last_close: raw.last_close === null ? null : Number(raw.last_close),
          last_day: raw.last_day ?? "",
          traded_usd: Number(raw.traded_usd),
        };

  const lead = pickLeadSymbol(symbols, summary);
  const prices =
    lead === null
      ? []
      : await chQuery<SeCompanyPricePoint>(COMPANY_LEAD_PRICES_SQL, {
          symbolKey: lead.eodhd_symbol_key,
        });

  return {
    leis,
    symbols,
    summary,
    leadSymbolKey: lead === null ? "" : lead.eodhd_symbol_key,
    prices,
  };
}
