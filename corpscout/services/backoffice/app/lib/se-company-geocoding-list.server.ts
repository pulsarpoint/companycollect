/**
 * The `/admin/se/company-info/geocoding` list: one row per Swedish company
 * that has a published address, showing that address's geocode outcome.
 *
 * Reads exactly the table se-company-address.server.ts reads for the Address
 * tab -- corpscout.se_company_address, `a.geocode_status` -- and NOTHING
 * else. That module's own doc comment explains why there is no join to
 * corpscout.se_address_geocodes_current here: the geocode is read once at
 * resolve time (Dagster's se_company/address.py) and stored on the published
 * row, so a fresh join would risk re-deriving a status that can drift from
 * what the row (and the Address tab, and every correction ledger check
 * against it) actually carries. This module inherits that same discipline --
 * one column, no new join path -- so the tab and the Address tab's own cards
 * can never disagree about a company's geocode outcome.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import type {
  GeocodeListFilter,
  GeocodeStatusClass,
} from "~/lib/se-company-geocoding-filters";

/* -------------------------------------------------------------------- */
/* The published address: one row per company                           */
/* -------------------------------------------------------------------- */

/**
 * A company can publish more than one live row in se_company_address (Task's
 * owner instruction: mirror the address tab's own primary/visiting choice).
 * The address tab itself (se-company-address.server.ts) does not pick one --
 * it shows every live row as its own card -- so the rule mirrored here is the
 * one other primary-address choice in this codebase:
 * address-companies.server.ts's SWEDEN_SAME_BUILDING_QUERY, which ranks a
 * company's own addresses `has(address_types, 'visiting_or_postal') DESC,
 * has(address_types, 'visiting') DESC` before picking the first. That query
 * ranks the LEGACY se_company_address_links_current/address_types array; this
 * one is the same priority (a physical visiting address outranks a postal-only
 * one) read off se_company_address's own `address_type` column instead, since
 * that is the table this tab is required to read.
 *
 * Live-checked 2026-08-25: se_company_address.address_type carries exactly two
 * values today ('postal', 2,855,191 rows; 'visiting_or_postal', 1,818,909),
 * with 'visiting' kept in the ORDER BY as the same three-way rank the legacy
 * query uses, in case a future publish ever writes it. address_key is the
 * final, deterministic tiebreak -- ADDRESSES_SQL orders by the same column for
 * the same reason.
 *
 * `LIMIT 1 BY a.company_id` after that ORDER BY is this codebase's own
 * established "pick the top row per group" idiom (see address-quality.server.
 * ts's `LIMIT 3 BY link.address_id`), not a novel construct.
 *
 * FINAL is required for the reason ADDRESSES_SQL's own comment gives:
 * se_company_address is a ReplacingMergeTree on resolved_at, so without it a
 * re-resolved address would be read once per run. `is_current` excludes a
 * company's tombstoned rows exactly as ADDRESSES_SQL does.
 */
export const GEOCODING_PUBLISHED_ADDRESS_SQL = `SELECT
    a.company_id AS company_id,
    ifNull(a.street_address, '') AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.geocode_status) AS geocode_status
  FROM corpscout.se_company_address AS a FINAL
  WHERE a.is_current
  ORDER BY
    a.company_id,
    a.address_type = 'visiting_or_postal' DESC,
    a.address_type = 'visiting' DESC,
    a.address_key ASC
  LIMIT 1 BY a.company_id`;

/** Reused verbatim by both the row query and the counts query, so a company
 * neither can ever disagree about which address is "published" for it. */
export const GEOCODING_PUBLISHED_CTE_SQL = `WITH published AS (
${GEOCODING_PUBLISHED_ADDRESS_SQL}
)`;

/* -------------------------------------------------------------------- */
/* The geocode class: the tab's own four-way read of geocode_status      */
/* -------------------------------------------------------------------- */

/**
 * The `geocode_status` values Dagster's own geocode_store.py
 * (dagster_v3/defs/sweden_company/geocode_store.py, `GEOCODED_STATUSES`)
 * calls a successful match. `postal_box` and `property_identifier` are valid
 * outcomes there (VALID_STATUSES) but NOT geocoded ones -- geocode_store.py's
 * own `is_geocoded_status` returns `match_status in GEOCODED_STATUSES`, which
 * excludes both -- so this tab's "unmatched" bucket carries them alongside
 * `unmatched`, `invalid_address` and `foreign_address`. Live-checked
 * 2026-08-25 against corpscout.se_company_address (is_current): every one of
 * these eleven statuses (ten non-empty plus '') is actually carried by a
 * published row, so the multiIf below has no branch that can silently swallow
 * a status this tab has never seen.
 */
const GEOCODED_MATCH_STATUSES = [
  "matched_exact",
  "matched_corrected",
  "matched_site",
  "matched_area",
  "matched_street",
] as const;

/**
 * `column` is the SQL text naming the geocode_status expression to classify
 * -- `p.geocode_status` in every query below, `published`'s own output
 * column in each case, so the row list and the counts strip read the exact
 * same text and can never diverge on where a status falls.
 *
 * Branch order matters (multiIf returns the first match): the empty-string
 * check must precede the `IN` check, since '' is never a member of
 * GEOCODED_MATCH_STATUSES but reads clearer named first as its own case
 * ("never reached the geocoder") than folded into the unmatched default.
 */
export function geocodeClassExpr(column: string): string {
  return `multiIf(
    ${column} = '', 'no_outcome',
    ${column} IN (${GEOCODED_MATCH_STATUSES.map((status) => `'${status}'`).join(", ")}), 'geocoded',
    ${column} = 'ambiguous', 'ambiguous',
    'unmatched'
  )`;
}

/** The expression every query below filters and projects: `published`
 * aliased as `p`, which every query FROMs or JOINs it as. */
export const GEOCODE_STATUS_CLASS_EXPR = geocodeClassExpr("p.geocode_status");

/**
 * One predicate per toggle option, built from the SAME class expression the
 * counts strip and the row list project -- so "Ambiguous: 491,817" in the
 * strip and the 491,817 rows `?status=ambiguous` returns can never drift
 * apart the way two hand-written predicates could.
 *
 * "all" is the literal `1`: every company with a published address, geocoded
 * ones included -- the tab's own escape hatch out of the default triage view.
 * "needs_attention" is `!= 'geocoded'`, which is `ambiguous OR unmatched OR
 * no_outcome` by construction (geocodeClassExpr has no other branch), never
 * spelled as that OR directly so it cannot fall out of sync if a class is
 * ever added.
 */
export const GEOCODE_LIST_FILTER_SQL: Record<GeocodeListFilter, string> = {
  needs_attention: `(${GEOCODE_STATUS_CLASS_EXPR}) != 'geocoded'`,
  all: "1",
  geocoded: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'geocoded'`,
  ambiguous: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'ambiguous'`,
  unmatched: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'unmatched'`,
  no_outcome: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'no_outcome'`,
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
  /** The raw column (e.g. "matched_exact", "" for never-geocoded) -- kept
   * alongside the class for a reader who wants the specific outcome, exactly
   * as the Address tab's own cards show it. */
  geocode_status: string;
  /** Which of the tab's four classes `geocode_status` falls into, computed
   * SQL-side by `GEOCODE_STATUS_CLASS_EXPR` -- not re-derived in the
   * component, so a status this tab has never seen cannot be classified two
   * different ways by two copies of the same multiIf. */
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
 * The company's own name, joined from se_company_info -- the same "company
 * info" spine the sibling /admin/se/company-info list itself reads its
 * `legal_name` from (INFO_LIST_SELECT_SQL), so this tab's Company column
 * cannot show a different name than the row this company's own line on the
 * main list shows.
 *
 * INNER JOIN, not LEFT: live-checked 2026-08-25, every one of the 3,523,532
 * companies with a published address also has an se_company_info row (0
 * orphans) -- info_rules.py's merge already requires an SCB row for
 * se_company_info to exist at all, and SCB reach among addressed companies is
 * total in practice. An INNER JOIN costs nothing here that a LEFT JOIN
 * wouldn't, and a future orphan would surface as a company silently missing
 * from this tab rather than one rendering a blank name -- the loud failure,
 * not the quiet one.
 */
export const GEOCODING_LIST_SELECT_SQL = `${GEOCODING_PUBLISHED_CTE_SQL}
SELECT
  p.company_id AS company_id,
  i.legal_name AS legal_name,
  p.street_address AS street_address,
  p.postal_code AS postal_code,
  p.city AS city,
  p.geocode_status AS geocode_status,
  ${GEOCODE_STATUS_CLASS_EXPR} AS geocode_class
FROM published AS p
INNER JOIN corpscout.se_company_info AS i FINAL ON i.company_id = p.company_id`;

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
 * se_company_address per page load, mirroring listSeCompanyInfoPage. */
export async function listSeCompanyGeocodingPage(
  query: SeCompanyGeocodingListQuery,
): Promise<SeCompanyGeocodingListPage> {
  const { limit, offset } = pageParams(query);
  const rows = await chQuery<SeCompanyGeocodingListRow>(
    `${GEOCODING_LIST_SELECT_SQL}
WHERE ${GEOCODE_LIST_FILTER_SQL[query.filter]}
ORDER BY p.company_id ASC
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
  ambiguous: number;
  unmatched: number;
  noOutcome: number;
}

/** One scan of `published` for every number the strip shows, and for the
 * chosen filter's own pagination total (`total` when `filter` is "all", the
 * matching field otherwise -- see `countForFilter`). */
export const GEOCODING_COUNTS_SQL = `${GEOCODING_PUBLISHED_CTE_SQL}
SELECT
  toString(count()) AS total,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.needs_attention})) AS needs_attention,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.geocoded})) AS geocoded,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.ambiguous})) AS ambiguous,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.unmatched})) AS unmatched,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.no_outcome})) AS no_outcome
FROM published AS p`;

export async function loadSeCompanyGeocodingCounts(): Promise<SeCompanyGeocodingCounts> {
  const [row] = await chQuery<{
    total: string;
    needs_attention: string;
    geocoded: string;
    ambiguous: string;
    unmatched: string;
    no_outcome: string;
  }>(GEOCODING_COUNTS_SQL);
  return {
    total: Number(row?.total ?? 0),
    needsAttention: Number(row?.needs_attention ?? 0),
    geocoded: Number(row?.geocoded ?? 0),
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
