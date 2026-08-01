/**
 * Filters for a country's contracts list, held entirely in the URL.
 *
 * Separate typed params rather than the company list's `f_<key>` convention. That
 * one is a value SET — repeated params union into an IN clause — and two of these
 * filters are ranges, which a set cannot express: encoding `gt:1000` inside a
 * multi-value param means parsing prefixes back out of user-supplied text and
 * deciding what two `gt:` values mean. So agreement type keeps the set shape and
 * the ranges get their own params:
 *
 *   agreement=Empenho&agreement=Outros    multi-select, raw register values
 *   amount_min=1000&amount_max=50000      either end optional
 *   year=2025                             the common case, expanded to a range
 *   from=2024-03-01&to=2024-06-30         explicit range, wins over year
 *
 * Nothing here throws on a hand-edited URL. An unusable value is simply not a
 * filter, because a 500 on a malformed query string is worse than an unfiltered
 * page — and a filter that silently matches nothing reads as "the data is empty",
 * which is why a reversed amount range is swapped rather than left to match zero.
 *
 * Client-safe: no `.server` imports, so the filter sheet can use it directly.
 */

import { cpvPrefix } from "~/lib/cpv";

export type ContractFilters = {
  /** Raw register values, as stored. The list translates them for display. */
  agreement: string[];
  /**
   * CPV hierarchy PREFIXES — 2 to 8 digits, one per selected node.
   *
   * CPV is a tree read left to right, so a node's significant prefix is shared
   * by everything beneath it: `45` is Construction work, `4521` building
   * construction, `452131` commercial buildings. Selecting a node therefore
   * means "this and every descendant" for free — picking `45` returns 7,323
   * Estonian contracts, `452` returns 4,139, `4521` returns 1,017.
   *
   * Held as prefixes rather than whole codes so a selection never depends on
   * the depth a particular buyer chose to publish at. `cpvPrefix` normalises
   * both ends, so `cpv=45000000` and `cpv=45` are the same filter.
   */
  cpv: string[];
  amountMin: number | null;
  amountMax: number | null;
  /** ISO dates, inclusive. A year is expanded into these. */
  from: string | null;
  to: string | null;
};

/** No filter at all, so a caller need not build one to mean "unfiltered". */
export const EMPTY_CONTRACT_FILTERS: ContractFilters = {
  agreement: [],
  cpv: [],
  amountMin: null,
  amountMax: null,
  from: null,
  to: null,
};

const MAX_AGREEMENT_VALUES = 50;
/** A reader ticking more than this is not filtering; it bounds the query. */
const MAX_CPV_VALUES = 60;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * A row's CPV codes as an ARRAY, whatever shape the register stored.
 *
 * Doffin and TED publish one code per row; Hilma joins several into one string
 * with commas. Splitting in SQL means a contract classified `72317000,
 * 48800000` is found under BOTH divisions instead of only the first — which
 * took Finland's division 48 from 90 contracts to its real 267.
 */
export const CPV_CODES =
  "arrayFilter(c -> c != '', arrayMap(c -> trimBoth(c), splitByChar(',', cpv_code)))";

function positiveNumber(raw: string | null): number | null {
  if (raw == null || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function isoDate(raw: string | null): string | null {
  if (raw == null || !ISO_DATE.test(raw.trim())) return null;
  const value = raw.trim();
  // Rejects 2024-13-99: the regex only proves the shape.
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) || !parsed.toISOString().startsWith(value)
    ? null
    : value;
}

export function parseContractFilters(searchParams: URLSearchParams): ContractFilters {
  const agreement = [
    ...new Set(
      searchParams
        .getAll("agreement")
        .map((v) => v.trim())
        .filter((v) => v !== ""),
    ),
  ].slice(0, MAX_AGREEMENT_VALUES);

  // Normalised to the significant prefix, so `cpv=45000000` and `cpv=45` are
  // one filter and a code pasted from a notice selects the node it belongs to.
  const cpv = [
    ...new Set(
      searchParams
        .getAll("cpv")
        .map((v) => cpvPrefix(v))
        .filter((v): v is string => v !== null),
    ),
  ].slice(0, MAX_CPV_VALUES);

  let amountMin = positiveNumber(searchParams.get("amount_min"));
  let amountMax = positiveNumber(searchParams.get("amount_max"));
  if (amountMin != null && amountMax != null && amountMin > amountMax) {
    [amountMin, amountMax] = [amountMax, amountMin];
  }

  let from = isoDate(searchParams.get("from"));
  let to = isoDate(searchParams.get("to"));
  if (from == null && to == null) {
    // A year is the common case and worth one param instead of two dates.
    const year = searchParams.get("year");
    if (year != null && /^\d{4}$/.test(year.trim())) {
      from = `${year.trim()}-01-01`;
      to = `${year.trim()}-12-31`;
    }
  }
  if (from != null && to != null && from > to) [from, to] = [to, from];

  return { agreement, cpv, amountMin, amountMax, from, to };
}

/**
 * A `WHERE` fragment and its bound parameters.
 *
 * Returns a leading ` AND `-joined fragment so a caller appends it to an existing
 * predicate. Values are always bound, never interpolated: they come from a URL.
 *
 * It must be applied to the COUNT query as well as the page query. Filtering only
 * the page leaves the total — and therefore the pagination — describing a
 * different set of rows than the one on screen.
 */
export function contractFilterSql(
  filters: ContractFilters,
  opts?: { agreementExpr?: string; amountExpr?: string },
): {
  where: string;
  params: Record<string, unknown>;
} {
  // The agreement filter must match the SAME expression the list DISPLAYS, not
  // the raw column: Brazil's agreement_type holds PNCP's {"id":n,"nome":"..."}
  // object, so `agreement_type IN ('Empenho')` would match nothing at all while
  // looking perfectly correct.
  const agreementExpr = opts?.agreementExpr ?? "agreement_type";
  // Which USD figure the range compares. The winner-level tables publish one
  // per winner row as value_amount_usd, NULL where no source reported one;
  // company_contract_rollup publishes the contract's total as amount_usd, with
  // -1.0 standing for the same absence. That sentinel has to be turned back
  // into NULL or an "under $1m" filter would sweep in every contract with no
  // USD figure at all.
  const amountExpr = opts?.amountExpr ?? "value_amount_usd";
  const clauses: string[] = [];
  const params: Record<string, unknown> = {};

  if (filters.agreement.length > 0) {
    clauses.push(`${agreementExpr} IN {agreement:Array(String)}`);
    params.agreement = filters.agreement;
  }
  // Prefix match over the row's codes, which is what makes selecting a node
  // select its whole subtree: '45' matches 45000000 and 45213100 alike, '4521'
  // only the latter. Over the ARRAY, so a Hilma row carrying several codes is
  // found under every one of them rather than only the first.
  if (filters.cpv.length > 0) {
    clauses.push(
      `arrayExists(c -> arrayExists(p -> startsWith(c, p), {cpv:Array(String)}), ${CPV_CODES})`,
    );
    params.cpv = filters.cpv;
  }
  // USD, because a country can mix currencies -- Sweden carries SEK from UHM and
  // EUR from TED -- so a threshold against "the original amount" would compare
  // unlike numbers. The sheet labels the unit so the reader knows.
  if (filters.amountMin != null) {
    clauses.push(`${amountExpr} >= {amount_min:Float64}`);
    params.amount_min = filters.amountMin;
  }
  if (filters.amountMax != null) {
    clauses.push(`${amountExpr} <= {amount_max:Float64}`);
    params.amount_max = filters.amountMax;
  }
  if (filters.from != null) {
    clauses.push("publication_date >= {from:String}");
    params.from = filters.from;
  }
  if (filters.to != null) {
    clauses.push("publication_date <= {to:String}");
    params.to = filters.to;
  }

  return {
    where: clauses.length === 0 ? "" : ` AND ${clauses.join(" AND ")}`,
    params,
  };
}

/** Back to a query string, so a filtered view is linkable and the back button works. */
export function serializeContractFilters(filters: ContractFilters): string {
  const params = new URLSearchParams();
  for (const value of filters.agreement) params.append("agreement", value);
  for (const value of filters.cpv) params.append("cpv", value);
  if (filters.amountMin != null) params.set("amount_min", String(filters.amountMin));
  if (filters.amountMax != null) params.set("amount_max", String(filters.amountMax));
  if (filters.from != null) params.set("from", filters.from);
  if (filters.to != null) params.set("to", filters.to);
  return params.toString();
}

/** How many filters are active, for the button's badge. A range counts once. */
export function contractFilterCount(filters: ContractFilters): number {
  let count = 0;
  if (filters.agreement.length > 0) count += 1;
  if (filters.cpv.length > 0) count += 1;
  if (filters.amountMin != null || filters.amountMax != null) count += 1;
  if (filters.from != null || filters.to != null) count += 1;
  return count;
}
