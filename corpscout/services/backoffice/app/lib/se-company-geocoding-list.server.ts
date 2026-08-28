/**
 * The `/admin/se/companies/geocoding` list: one row per Swedish company that
 * has a published address, showing that address's geocode outcome.
 *
 * SOURCE: corpscout.se_companies_serving -- the consolidated per-company
 * serving materialized view (migration 000335, built from
 * dagster_v3 sweden_company/companies_current.py, superseding the
 * se_companies_current view of migration 000326). It holds ONE ROW PER
 * COMPANY, refreshed every 15 minutes, having ALREADY paid -- once, at
 * refresh time -- the expensive work this tab used to redo on every request:
 * the FINAL merges on se_company_address/se_company_info, the primary-address
 * pick, and the LEFT JOIN to the se_address_geocodes_served overlay. Serving
 * reads the plain MergeTree behind the view: NO FINAL, NO join. The previous
 * ~20s page load (recomputing all of that per request) becomes a sub-second
 * scan of a table keyed ORDER BY company_id.
 *
 * One thing DID widen with 000335: se_companies_current only had companies
 * WITH a current address, while se_companies_serving has ALL of
 * se_company_info -- so every query here filters `has_address = 1`
 * (GEOCODING_ADDRESS_FILTER_SQL) to keep this tab exactly the "companies with
 * a published address" list it has always been.
 *
 * The view carries the PRIMARY address's own summary as flat columns:
 * `primary_geocode_class` (the coarse-aware class -- vocabulary
 * geocoded/coarse/ambiguous/unmatched/no_outcome, computed in the view by the
 * SAME multiIf this module used to spell, with the `centroid_fallback`
 * provider check BEFORE the geocoded-status check so a coarse centroid can
 * never read as a precise match), `primary_geocode_status` (the raw stored
 * status, for the badge tooltip), `primary_geocode_precision`/`_provider`, and
 * the primary row's own `primary_street_address`/`primary_postal_code`/
 * `primary_city`. The list and the counts strip both read those columns
 * straight off the view -- neither re-derives the class, and neither has to
 * re-pick a primary out of the view's `addresses` JSON (which, being a plain
 * map array, carries no `address_key` to tiebreak on).
 *
 * The primary-address pick, the coarse-aware class semantics, and the class
 * vocabulary all live in the view builder now; this module is a thin,
 * pre-computed-column reader. The class vocabulary itself is pinned in
 * se-company-geocoding-filters.ts (GEOCODE_STATUS_CLASSES), which the toggle
 * and this module's filter predicates both key off, so the SQL and the toggle
 * can never name a different set of classes.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  PAGE_LIMIT_OFFSET_SQL,
  SE_COMPANIES_SERVING_TABLE,
} from "~/lib/se-company-info-lists.server";
import type {
  GeocodeListFilter,
  GeocodeStatusClass,
} from "~/lib/se-company-geocoding-filters";

/* -------------------------------------------------------------------- */
/* The serving view: one row per company, geocode summary precomputed    */
/* -------------------------------------------------------------------- */

/** The consolidated per-company serving MV (migration 000335) -- the ONE
 * constant both admin companies pages read, re-exported from the info list
 * module that owns it. Read as a plain MergeTree -- no FINAL, no join: every
 * merge/join/aggregation this tab needs was resolved once at the view's
 * 15-minute refresh. */
export { SE_COMPANIES_SERVING_TABLE };

/** The serving view's base is ALL published companies (000335 widened it past
 * se_companies_current's with-an-address base), so this tab -- the geocode
 * triage of companies WITH a current address -- ANDs the view's precomputed
 * address flag onto every query. `has_address = 1` is exactly "address_count
 * > 0": both are written from the same current-address join at refresh time. */
export const GEOCODING_ADDRESS_FILTER_SQL = "has_address = 1";

/** The precomputed class column every filter predicate and the list badge read.
 * `primary_geocode_class` is the coarse-aware class of the company's PRIMARY
 * address, materialized in the view -- vocabulary
 * geocoded/coarse/ambiguous/unmatched/no_outcome. This module never recomputes
 * it: the coarse-before-geocoded correctness (a `centroid_fallback` row keeps
 * its `match_status` inside the GEOCODED vocabulary and only its provider tells
 * it apart from a building-precise match) is settled in the view builder. */
export const PRIMARY_GEOCODE_CLASS_COLUMN = "primary_geocode_class";

/**
 * One predicate per toggle option, built from the SAME precomputed class column
 * the counts strip and the row list read -- so "Ambiguous: N" in the strip and
 * the N rows `?status=ambiguous` returns can never drift apart.
 *
 * "all" is the literal `1`: every company with a published address, geocoded
 * ones included. "needs_attention" is `!= 'geocoded'`, which is
 * `coarse OR ambiguous OR unmatched OR no_outcome` by construction (the view's
 * class column has no other value), never spelled as that OR directly so it
 * cannot fall out of sync if a class is ever added. A coarse-centroid row DOES
 * need attention: it has a usable coordinate, but never the precise one this
 * tab is triaging toward.
 */
export const GEOCODE_LIST_FILTER_SQL: Record<GeocodeListFilter, string> = {
  needs_attention: `${PRIMARY_GEOCODE_CLASS_COLUMN} != 'geocoded'`,
  all: "1",
  geocoded: `${PRIMARY_GEOCODE_CLASS_COLUMN} = 'geocoded'`,
  coarse: `${PRIMARY_GEOCODE_CLASS_COLUMN} = 'coarse'`,
  ambiguous: `${PRIMARY_GEOCODE_CLASS_COLUMN} = 'ambiguous'`,
  unmatched: `${PRIMARY_GEOCODE_CLASS_COLUMN} = 'unmatched'`,
  no_outcome: `${PRIMARY_GEOCODE_CLASS_COLUMN} = 'no_outcome'`,
};

/* -------------------------------------------------------------------- */
/* The row list                                                          */
/* -------------------------------------------------------------------- */

export interface SeCompanyGeocodingListRow {
  company_id: string;
  legal_name: string;
  street_address: string;
  postal_code: string;
  city: string;
  /** The raw stored status of the primary address (e.g. "matched_exact", "" for
   * never-geocoded) -- kept alongside the class for the badge tooltip, exactly
   * as the Address tab's own cards show it. For a coarse-centroid row this
   * stays 'unmatched'/'ambiguous'/'postal_box' (the precise matcher's own,
   * fallback-eligible outcome -- see FALLBACK_ELIGIBLE_STATUSES in
   * geocode_serving_overlay.py), never 'matched_area';
   * `geocode_precision`/`geocode_provider` carry the served overlay's own
   * answer. Read off the view's `primary_geocode_status`. */
  geocode_status: string;
  /** '' unless the served overlay filled the primary address with a coarse
   * centroid -- 'postcode' or 'city' when it did. Off `primary_geocode_precision`. */
  geocode_precision: string;
  /** '' for a primary the served overlay never touched; 'centroid_fallback'
   * when it supplied a coarse centroid instead of a precise match -- the one
   * value the view's 'coarse' class keys off. Off `primary_geocode_provider`. */
  geocode_provider: string;
  /** Which of the tab's five classes this row falls into, precomputed in the
   * view (`primary_geocode_class`) -- not re-derived here, so a status/provider
   * pair can never be classified two different ways by two copies of the same
   * multiIf. */
  geocode_class: GeocodeStatusClass;
}

export interface SeCompanyGeocodingListQuery {
  filter: GeocodeListFilter;
  page: number;
  pageSize: number;
}

export interface SeCompanyGeocodingListPage {
  rows: SeCompanyGeocodingListRow[];
}

/**
 * The row list: the primary address's display fields plus its precomputed
 * geocode summary, read straight off the serving view. NO FINAL, NO join --
 * the view already resolved all of that at refresh time. The `primary_*`
 * columns are aliased to the interface's own names so the component reads the
 * same shape it always has.
 */
export const GEOCODING_LIST_SELECT_SQL = `SELECT
  company_id AS company_id,
  legal_name AS legal_name,
  primary_street_address AS street_address,
  primary_postal_code AS postal_code,
  primary_city AS city,
  primary_geocode_status AS geocode_status,
  primary_geocode_precision AS geocode_precision,
  primary_geocode_provider AS geocode_provider,
  primary_geocode_class AS geocode_class
FROM ${SE_COMPANIES_SERVING_TABLE}`;

function pageParams(query: { page: number; pageSize: number }): {
  limit: number;
  offset: number;
} {
  const limit = clampPageSize(query.pageSize);
  const page = clampPage(query.page);
  return { limit, offset: (page - 1) * limit };
}

/** No count() query here: the table's pagination total is
 * `loadSeCompanyGeocodingCounts`'s own count for the chosen class, loaded
 * alongside this call for the counts strip anyway -- one fewer full scan of
 * the view per page load, mirroring listSeCompanyInfoPage. */
export async function listSeCompanyGeocodingPage(
  query: SeCompanyGeocodingListQuery,
): Promise<SeCompanyGeocodingListPage> {
  const { limit, offset } = pageParams(query);
  const rows = await chQuery<SeCompanyGeocodingListRow>(
    `${GEOCODING_LIST_SELECT_SQL}
WHERE ${GEOCODING_ADDRESS_FILTER_SQL} AND ${GEOCODE_LIST_FILTER_SQL[query.filter]}
ORDER BY company_id ASC
${PAGE_LIMIT_OFFSET_SQL}`,
    { limit, offset },
  );
  return { rows };
}

/* -------------------------------------------------------------------- */
/* The counts strip                                                      */
/* -------------------------------------------------------------------- */

export interface SeCompanyGeocodingCounts {
  total: number;
  needsAttention: number;
  geocoded: number;
  coarse: number;
  ambiguous: number;
  unmatched: number;
  noOutcome: number;
}

/** One scan of the serving view -- the SAME table (and the same has_address
 * base filter) as the row list -- for every number the strip shows, and for
 * the chosen filter's own pagination total (`total` when `filter` is "all",
 * the matching field otherwise -- see `countForFilter`). Reads the
 * precomputed `primary_geocode_class` buckets; no FINAL, no join, no
 * GROUP BY. */
export const GEOCODING_COUNTS_SQL = `SELECT
  toString(count()) AS total,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.needs_attention})) AS needs_attention,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.geocoded})) AS geocoded,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.coarse})) AS coarse,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.ambiguous})) AS ambiguous,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.unmatched})) AS unmatched,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.no_outcome})) AS no_outcome
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE ${GEOCODING_ADDRESS_FILTER_SQL}`;

export async function loadSeCompanyGeocodingCounts(): Promise<SeCompanyGeocodingCounts> {
  const [row] = await chQuery<{
    total: string;
    needs_attention: string;
    geocoded: string;
    coarse: string;
    ambiguous: string;
    unmatched: string;
    no_outcome: string;
  }>(GEOCODING_COUNTS_SQL);
  return {
    total: Number(row?.total ?? 0),
    needsAttention: Number(row?.needs_attention ?? 0),
    geocoded: Number(row?.geocoded ?? 0),
    coarse: Number(row?.coarse ?? 0),
    ambiguous: Number(row?.ambiguous ?? 0),
    unmatched: Number(row?.unmatched ?? 0),
    noOutcome: Number(row?.no_outcome ?? 0),
  };
}

/** The table's pagination total for whichever class is selected: `counts`
 * already carries every number this can name, so the page never runs a
 * second count() query just to page the class it is already showing. */
export function countForFilter(
  counts: SeCompanyGeocodingCounts,
  filter: GeocodeListFilter,
): number {
  switch (filter) {
    case "all":
      return counts.total;
    case "needs_attention":
      return counts.needsAttention;
    case "geocoded":
      return counts.geocoded;
    case "coarse":
      return counts.coarse;
    case "ambiguous":
      return counts.ambiguous;
    case "unmatched":
      return counts.unmatched;
    case "no_outcome":
      return counts.noOutcome;
    default:
      return counts.needsAttention;
  }
}
