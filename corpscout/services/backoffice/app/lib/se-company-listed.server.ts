import { chQuery } from "~/lib/clickhouse.server";

/** One current LEI linked to the company in corpscout.company_identifier. */
export interface SeCompanyLeiRow {
  lei: string;
  entity_status: string;
  registration_status: string;
}

/** One listed line/venue from corpscout.company_traded_symbols, enriched with
 * the EODHD symbol dimension (official name, instrument type, quote currency,
 * delisting flag). Cross-listings are real rows: Handelsbanken has an SHB-A
 * line on ST plus LSE lines. */
export interface SeCompanyTradedSymbolRow {
  isin: string;
  eodhd_symbol_key: string;
  ticker: string;
  exchange_code: string;
  /** EODHD's official name for the line, '' when the dimension has no row. */
  symbol_name: string;
  /** e.g. 'Common Stock', 'Preferred Stock'; '' on a dimension miss. */
  instrument_type: string;
  /** The currency THIS line quotes in — cross-listings differ from the lead. */
  quote_currency: string;
  /** 1 when EODHD marks the line delisted. */
  is_delisted: number;
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

/** One daily session for the lead symbol, oldest first. `close` is always
 * present (the SQL filters null closes); the OHLC extras and volume are
 * nullable per EODHD's own gaps and pass through as null. */
export interface SeCompanyPricePoint {
  price_date: string;
  close: number;
  high: number | null;
  low: number | null;
  adjusted_close: number | null;
  volume: number | null;
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
 * FINAL and no dedup on the LEFT side — the physical rows are the listing
 * list. The joined eodhd_symbols dimension IS a ReplacingMergeTree
 * (retrieved_at), so that side takes FINAL: a re-fetched symbol must
 * contribute its newest name/type/delisting state once. Every joined column
 * is ifNull-folded so the row shape survives both join_use_nulls settings —
 * a join miss is '' / 0, never NULL and never a type default surprise.
 */
export const COMPANY_TRADED_SYMBOLS_SQL = `SELECT
  s.isin AS isin,
  s.eodhd_symbol_key AS eodhd_symbol_key,
  s.ticker AS ticker,
  toString(s.exchange_code) AS exchange_code,
  ifNull(es.symbol_name, '') AS symbol_name,
  ifNull(toString(es.instrument_type), '') AS instrument_type,
  ifNull(toString(es.currency), '') AS quote_currency,
  toUInt8(ifNull(es.is_delisted, 0)) AS is_delisted
FROM corpscout.company_traded_symbols AS s
LEFT JOIN corpscout.eodhd_symbols AS es FINAL
  ON es.eodhd_symbol_key = s.eodhd_symbol_key
WHERE s.country_code = 'SE'
  AND s.company_id = {companyId:String}
ORDER BY s.eodhd_symbol_key
LIMIT 100`;

/**
 * company_market_summary is keyed per (country, year, company) since migration
 * 000223: one row per YEAR the company traded — quoted price at that year's
 * end, its venue and currency (chosen by traded value — see migration
 * 000222's comments), and that year's turnover. The tab reads EVERY year
 * (owner 2026-08-28: per-year AND cumulative, not one unexplained figure);
 * the newest row doubles as the headline quote. traded_usd is TURNOVER,
 * never market cap. LIMIT 50 is a backstop, not paging — the price history
 * starts in 2020.
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
LIMIT 50`;

/**
 * Five years of daily sessions (close, true high/low, adjusted close, volume)
 * for ONE symbol — a keyed read on the table's own
 * primary key (eodhd_symbol_key, price_date), so it never scans the 15M-row
 * price history. eodhd_eod_prices is a ReplacingMergeTree on retrieved_at, so
 * FINAL: a re-fetched day must show once, in its newest state. Five years of
 * ~260 sessions each is ~1,300 rows; LIMIT 1500 bounds a malformed backfill
 * without ever clipping the real window (owner 2026-08-28: one year was too
 * short -- the price history goes back to 2020).
 */
export const COMPANY_LEAD_PRICES_SQL = `SELECT
  toString(p.price_date) AS price_date,
  toFloat64(p.close) AS close,
  toFloat64(p.high) AS high,
  toFloat64(p.low) AS low,
  toFloat64(p.adjusted_close) AS adjusted_close,
  toFloat64(p.volume) AS volume
FROM corpscout.eodhd_eod_prices AS p FINAL
WHERE p.eodhd_symbol_key = {symbolKey:String}
  AND p.price_date >= today() - INTERVAL 5 YEAR
  AND p.close IS NOT NULL
ORDER BY p.price_date
LIMIT 1500`;

/** One headline return figure. `value` is fractional (0.05 = +5%) and null
 * when the series does not reach back to the window's baseline. */
export interface SeCompanyMarketStatReturn {
  /** "1M" | "YTD" | "1Y" | "5Y" | "Since {year}". */
  label: string;
  value: number | null;
}

/** Key stats the stat strip renders, all derived from the loaded price
 * series — no extra query. */
export interface SeCompanyMarketStats {
  /** True intraday high/low over the trailing 365 days (a day missing its
   * high/low falls back to that day's close). Null when the series has no
   * session inside the window. */
  high52w: number | null;
  low52w: number | null;
  /** Mean daily volume over the trailing 365 days, ignoring null-volume
   * days; null when no day in the window reports volume. */
  avgVolume: number | null;
  /** Always four entries: 1M, YTD, 1Y, then 5Y or "Since {year}". */
  returns: SeCompanyMarketStatReturn[];
}

export interface SeCompanyListed {
  leis: SeCompanyLeiRow[];
  symbols: SeCompanyTradedSymbolRow[];
  summary: SeCompanyMarketSummary | null;
  /** Every traded year, newest first — `summary` is summaries[0]. */
  summaries: SeCompanyMarketSummary[];
  /** The symbol the chart and quote describe: the traded_symbols row on the
   * summary's lead venue, or the first row when the venues disagree. Empty
   * string when the company is not traded. */
  leadSymbolKey: string;
  prices: SeCompanyPricePoint[];
  /** Derived from `prices` at load time; null when the series is empty. */
  stats: SeCompanyMarketStats | null;
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

/** `date` shifted back by `days`, as YYYY-MM-DD. */
function daysBefore(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

/** The return endpoint for a session: adjusted close (splits/dividends
 * folded in) with the raw close standing in when EODHD has no adjusted
 * figure for that day. */
function endpointValue(point: SeCompanyPricePoint): number {
  return point.adjusted_close ?? point.close;
}

/** last / (latest session on or before `cutoff`) - 1, or null when the
 * series starts after the cutoff. */
function windowReturn(
  prices: SeCompanyPricePoint[],
  cutoff: string,
): number | null {
  const last = prices[prices.length - 1];
  let baseline: SeCompanyPricePoint | null = null;
  for (const point of prices) {
    if (point.price_date > cutoff) break;
    baseline = point;
  }
  if (baseline === null) return null;
  return endpointValue(last) / endpointValue(baseline) - 1;
}

/** A span of at least ~4.75 years earns the plain "5Y" label; anything
 * shorter is honestly "Since {year}". */
const FIVE_YEAR_LABEL_MIN_DAYS = 1735;

/**
 * Key stats from an already-loaded daily series, oldest first. Pure: `today`
 * is a parameter (YYYY-MM-DD) so tests pin the reference date. Returns null
 * for an empty series. The 52-week figures use TRUE intraday high/low (close
 * as a per-day fallback); returns use adjusted close with a raw-close
 * fallback per endpoint.
 */
export function computeMarketStats(
  prices: SeCompanyPricePoint[],
  today: string,
): SeCompanyMarketStats | null {
  if (prices.length === 0) return null;

  const yearAgo = daysBefore(today, 365);
  let high52w: number | null = null;
  let low52w: number | null = null;
  let volumeSum = 0;
  let volumeDays = 0;
  for (const point of prices) {
    if (point.price_date < yearAgo) continue;
    const high = point.high ?? point.close;
    const low = point.low ?? point.close;
    if (high52w === null || high > high52w) high52w = high;
    if (low52w === null || low < low52w) low52w = low;
    if (point.volume != null) {
      volumeSum += point.volume;
      volumeDays += 1;
    }
  }

  const last = prices[prices.length - 1];
  const earliest = prices[0];

  // YTD is measured from the first session of the CURRENT calendar year, so
  // it is 0 on that first session rather than null.
  const jan1 = `${today.slice(0, 4)}-01-01`;
  const firstOfYear = prices.find((point) => point.price_date >= jan1) ?? null;
  const ytd =
    firstOfYear === null
      ? null
      : endpointValue(last) / endpointValue(firstOfYear) - 1;

  // The SQL caps the series at 5 years, so the earliest session is the 5Y
  // baseline; the label only claims "5Y" when the span really is ~5 years.
  const spansFiveYears =
    earliest.price_date <= daysBefore(today, FIVE_YEAR_LABEL_MIN_DAYS);
  const returns: SeCompanyMarketStatReturn[] = [
    { label: "1M", value: windowReturn(prices, daysBefore(today, 30)) },
    { label: "YTD", value: ytd },
    { label: "1Y", value: windowReturn(prices, yearAgo) },
    {
      label: spansFiveYears ? "5Y" : `Since ${earliest.price_date.slice(0, 4)}`,
      value: endpointValue(last) / endpointValue(earliest) - 1,
    },
  ];

  return {
    high52w,
    low52w,
    avgVolume: volumeDays === 0 ? null : volumeSum / volumeDays,
    returns,
  };
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

  const summaries: SeCompanyMarketSummary[] = summaryRows.map((raw) => ({
    year: Number(raw.year),
    venues: Number(raw.venues),
    lead_venue: raw.lead_venue,
    lead_currency: raw.lead_currency,
    last_close: raw.last_close === null ? null : Number(raw.last_close),
    last_day: raw.last_day ?? "",
    traded_usd: Number(raw.traded_usd),
  }));
  const summary = summaries[0] ?? null;

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
    summaries,
    leadSymbolKey: lead === null ? "" : lead.eodhd_symbol_key,
    prices,
    stats: computeMarketStats(prices, new Date().toISOString().slice(0, 10)),
  };
}
