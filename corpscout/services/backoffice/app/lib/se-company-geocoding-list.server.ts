/**
 * The `/admin/se/companies/geocoding` list: one row per Swedish company
 * that has a published address, showing that address's geocode outcome.
 *
 * Base population: exactly the table se-company-address.server.ts reads for
 * the Address tab -- corpscout.se_company_address, `a.geocode_status`. That
 * module's own doc comment explains why there is no join to
 * corpscout.se_address_geocodes_current for the STATUS itself: the geocode is
 * read once at resolve time (Dagster's se_company/address.py) and stored on
 * the published row, so a fresh join to the precise store would risk
 * re-deriving a status that can drift from what the row (and the Address
 * tab, and every correction ledger check against it) actually carries. This
 * module keeps that discipline for `geocode_status` -- still read off
 * `se_company_address`, never re-derived.
 *
 * ONE addition (Task 6, 2026-08-26): a LEFT JOIN to
 * corpscout.se_address_geocodes_served (migration 000325) on `address_id`,
 * for `geocode_precision`/`geocode_provider` alone. That view is the SE
 * geocode SERVING OVERLAY -- precise outcomes pass through unchanged, and an
 * identity the precise matcher left unmatched/ambiguous is filled with a
 * coarse postcode-or-city CENTROID (see
 * dagster_v3/defs/sweden_company/geocode_serving_overlay.py). This join
 * cannot reintroduce the drift risk the paragraph above warns about: it is
 * read ONLY for `geocode_precision`/`geocode_provider`, never for
 * `match_status` or coordinates, and `geocode_class` below still keys off
 * `p.geocode_status` (the stored column) for every branch except the new
 * `coarse` one, which keys off `geocode_provider = 'centroid_fallback'` --
 * a value the served view stamps ONLY on a read-time centroid fill, never on
 * a precise row. No FINAL on the join: `se_address_geocodes_served` is a
 * plain VIEW (not a ReplacingMergeTree), and its own base
 * (`se_address_geocodes_current`) is a refreshable MV already carrying
 * exactly one row per address_id -- see that migration's own comment.
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
 *
 * `address_id` (added for Task 6) is the same "" -when-absent text column
 * se-company-address.server.ts's own ADDRESS_PROJECTION carries -- copied
 * verbatim (`ifNull(toString(a.address_id), '')`), not a new idiom. It is
 * the shared-identity join key `published_companies` below LEFT JOINs
 * against corpscout.se_address_geocodes_served for `geocode_precision`/
 * `geocode_provider`; it is not itself projected past that CTE.
 */
export const GEOCODING_PUBLISHED_ADDRESS_SQL = `SELECT
    a.company_id AS company_id,
    ifNull(a.street_address, '') AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    ifNull(toString(a.address_id), '') AS address_id,
    toString(a.geocode_status) AS geocode_status
  FROM corpscout.se_company_address AS a FINAL
  WHERE a.is_current
  ORDER BY
    a.company_id,
    a.address_type = 'visiting_or_postal' DESC,
    a.address_type = 'visiting' DESC,
    a.address_key ASC
  LIMIT 1 BY a.company_id`;

/**
 * Reused verbatim by both the row query and the counts query -- INCLUDING the
 * se_company_info join below -- so the two can never disagree about which
 * companies are even IN the population the counts strip and the table page
 * both describe. Before this, GEOCODING_COUNTS_SQL counted `published` alone
 * while the row list additionally INNER JOINed se_company_info; a company
 * with a published address but no se_company_info row (0 live today,
 * unguarded) would have counted in the strip and the pagination total while
 * never appearing as a row. `published_companies` is the one FROM both
 * queries below now share -- the same fix listSeCompanyInfoPage and
 * loadSeCompanyInfoCounts already apply by sharing one WHERE built from the
 * same filters (se-company-info-lists.server.ts): count and rows must read
 * literally the same set, not two independently-assembled ones that happen
 * to agree today.
 *
 * The join itself: se_company_info is the same "company info" spine the
 * sibling /admin/se/companies list itself reads its `legal_name` from
 * (INFO_LIST_SELECT_SQL), so this tab's Company column cannot show a
 * different name than the row this company's own line on the main list
 * shows. INNER, not LEFT: live-checked 2026-08-25, every one of the
 * 3,523,532 companies with a published address also has an se_company_info
 * row (0 orphans) -- info_rules.py's merge already requires an SCB row for
 * se_company_info to exist at all, and SCB reach among addressed companies
 * is total in practice. A future orphan would surface as a company silently
 * missing from this tab (from both the strip and the rows, now that they
 * share this CTE) rather than one rendering a blank name -- the loud
 * failure, not the quiet one.
 *
 * The second join here (Task 6) is a LEFT JOIN to
 * corpscout.se_address_geocodes_served on `address_id`, for
 * `geocode_precision`/`geocode_provider` alone -- see this module's own top
 * doc comment for why joining that specific view cannot reintroduce the
 * status-drift risk the se_company_info join's comment above does NOT need
 * to worry about (that join is INNER against a spine every published row is
 * live-checked to have; this one is LEFT, since most addresses carry no
 * served-overlay row at all -- either they matched precisely already, or no
 * postcode/city centroid exists to fill them, and `ifNull(..., '')` makes
 * that miss read the same as "no precision to show" every other absent text
 * column on this row already reads as).
 */
export const GEOCODING_PUBLISHED_CTE_SQL = `WITH published AS (
${GEOCODING_PUBLISHED_ADDRESS_SQL}
),
published_companies AS (
  SELECT
    p.company_id AS company_id,
    i.legal_name AS legal_name,
    p.street_address AS street_address,
    p.postal_code AS postal_code,
    p.city AS city,
    p.geocode_status AS geocode_status,
    ifNull(s.geocode_precision, '') AS geocode_precision,
    ifNull(s.geocode_provider, '') AS geocode_provider
  FROM published AS p
  INNER JOIN corpscout.se_company_info AS i FINAL ON i.company_id = p.company_id
  LEFT JOIN corpscout.se_address_geocodes_served AS s ON toString(s.address_id) = p.address_id
)`;

/* -------------------------------------------------------------------- */
/* The geocode class: the tab's own five-way read of geocode outcome     */
/* -------------------------------------------------------------------- */

/**
 * The `geocode_status` values Dagster's own geocode_store.py
 * (dagster_v3/defs/sweden_company/geocode_store.py, `GEOCODED_STATUSES`)
 * calls a successful match. `postal_box` and `property_identifier` are valid
 * outcomes there (VALID_STATUSES) but NOT geocoded ones -- geocode_store.py's
 * own `is_geocoded` returns `match_status in GEOCODED_STATUSES`, which
 * excludes both -- so this tab's "unmatched" bucket carries them alongside
 * `unmatched`, `invalid_address` and `foreign_address`. Live-checked
 * 2026-08-25 against corpscout.se_company_address (is_current): every one of
 * these twelve statuses (eleven non-empty plus '') is actually carried by a
 * published row, so the multiIf below has no branch that can silently swallow
 * a status this tab has never seen.
 *
 * Drift-pinned against the Python source itself, not just this comment: see
 * tests/se-company-geocoding-list.server.test.ts's "GEOCODED_MATCH_STATUSES
 * drift pin" describe block, which reads geocode_store.py off disk and
 * fails if this array and `GEOCODED_STATUSES` there ever disagree.
 */
export const GEOCODED_MATCH_STATUSES = [
  "matched_exact",
  "matched_corrected",
  "matched_site",
  "matched_area",
  "matched_street",
] as const;

/**
 * The `geocode_provider` value the SE geocode serving overlay
 * (corpscout.se_address_geocodes_served, migration 000325 -- see
 * dagster_v3's geocode_serving_overlay.py) stamps on a row it filled with a
 * coarse postcode/city centroid instead of a precise match. This is the
 * ONLY thing that tells such a row apart from an exact one: the overlay
 * deliberately keeps `match_status` inside the GEOCODED vocabulary
 * (`matched_area`, itself a member of GEOCODED_MATCH_STATUSES above) so no
 * consumer filtering on status alone is surprised -- which is exactly why
 * `geocodeClassExpr` below checks this provider value BEFORE the
 * GEOCODED_MATCH_STATUSES membership check, not after: a naive "is
 * geocode_status one of the geocoded statuses?" read would count a coarse
 * centroid fallback as a full building-precise match.
 */
export const CENTROID_FALLBACK_PROVIDER = "centroid_fallback";

/**
 * `statusColumn` is the SQL text naming the geocode_status expression to
 * classify -- `p.geocode_status` in every query below, `published_companies`'s
 * own output column, so the row list and the counts strip read the exact
 * same text and can never diverge on where a status falls. `providerColumn`
 * is the sibling `geocode_provider` column the Task 6 join above fills --
 * '' for a row the served overlay never touched.
 *
 * Branch order matters (multiIf returns the first match):
 * - The empty-string check goes first: '' is never a member of either
 *   GEOCODED_MATCH_STATUSES or CENTROID_FALLBACK_PROVIDER but reads clearer
 *   named on its own ("never reached the geocoder") than folded into the
 *   unmatched default.
 * - The coarse-centroid check goes SECOND, strictly before the
 *   GEOCODED_MATCH_STATUSES membership check -- the key correctness
 *   requirement this column exists to satisfy. A served-overlay row's own
 *   `match_status` is literally `matched_area`, a GEOCODED_MATCH_STATUSES
 *   member; checking `providerColumn` first means a centroid_fallback row
 *   can never fall through to 'geocoded' no matter what status text arrives
 *   on either side of the join.
 */
export function geocodeClassExpr(statusColumn: string, providerColumn: string): string {
  return `multiIf(
    ${statusColumn} = '', 'no_outcome',
    ${providerColumn} = '${CENTROID_FALLBACK_PROVIDER}', 'coarse',
    ${statusColumn} IN (${GEOCODED_MATCH_STATUSES.map((status) => `'${status}'`).join(", ")}), 'geocoded',
    ${statusColumn} = 'ambiguous', 'ambiguous',
    'unmatched'
  )`;
}

/** The expression every query below filters and projects: `published_companies`
 * aliased as `p`, which both the row list and the counts query FROM. */
export const GEOCODE_STATUS_CLASS_EXPR = geocodeClassExpr(
  "p.geocode_status",
  "p.geocode_provider",
);

/**
 * One predicate per toggle option, built from the SAME class expression the
 * counts strip and the row list project -- so "Ambiguous: 491,817" in the
 * strip and the 491,817 rows `?status=ambiguous` returns can never drift
 * apart the way two hand-written predicates could.
 *
 * "all" is the literal `1`: every company with a published address, geocoded
 * ones included -- the tab's own escape hatch out of the default triage view.
 * "needs_attention" is `!= 'geocoded'`, which is `coarse OR ambiguous OR
 * unmatched OR no_outcome` by construction (geocodeClassExpr has no other
 * branch), never spelled as that OR directly so it cannot fall out of sync if
 * a class is ever added. A coarse-centroid row DOES need attention: it has a
 * usable coordinate, but never the precise one this tab is triaging toward.
 */
export const GEOCODE_LIST_FILTER_SQL: Record<GeocodeListFilter, string> = {
  needs_attention: `(${GEOCODE_STATUS_CLASS_EXPR}) != 'geocoded'`,
  all: "1",
  geocoded: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'geocoded'`,
  coarse: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'coarse'`,
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
   * as the Address tab's own cards show it. Stored on se_company_address at
   * resolve time -- for a coarse-centroid row this stays 'unmatched' or
   * 'ambiguous' (the precise matcher's own outcome), never 'matched_area';
   * `geocode_precision`/`geocode_provider` below are what carry the served
   * overlay's own answer. */
  geocode_status: string;
  /** '' unless the served overlay (corpscout.se_address_geocodes_served)
   * filled this identity with a coarse centroid -- 'postcode' or 'city' when
   * it did (Task 6). */
  geocode_precision: string;
  /** '' for a row the served overlay never touched; 'centroid_fallback' when
   * it supplied a coarse centroid instead of a precise match -- the one value
   * `geocode_class`'s 'coarse' branch keys off. */
  geocode_provider: string;
  /** Which of the tab's five classes this row falls into, computed SQL-side
   * by `GEOCODE_STATUS_CLASS_EXPR` -- not re-derived in the component, so a
   * status/provider pair this tab has never seen cannot be classified two
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
 * The row list: every column `published_companies` already carries, plus the
 * computed class. FROMs the shared CTE directly (see
 * GEOCODING_PUBLISHED_CTE_SQL's own doc comment for why the se_company_info
 * join lives there and not here) -- this query adds no join of its own, so it
 * reads exactly the same population GEOCODING_COUNTS_SQL counts.
 */
export const GEOCODING_LIST_SELECT_SQL = `${GEOCODING_PUBLISHED_CTE_SQL}
SELECT
  p.company_id AS company_id,
  p.legal_name AS legal_name,
  p.street_address AS street_address,
  p.postal_code AS postal_code,
  p.city AS city,
  p.geocode_status AS geocode_status,
  p.geocode_precision AS geocode_precision,
  p.geocode_provider AS geocode_provider,
  ${GEOCODE_STATUS_CLASS_EXPR} AS geocode_class
FROM published_companies AS p`;

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
  coarse: number;
  ambiguous: number;
  unmatched: number;
  noOutcome: number;
}

/** One scan of `published_companies` -- the SAME FROM the row list reads --
 * for every number the strip shows, and for the chosen filter's own
 * pagination total (`total` when `filter` is "all", the matching field
 * otherwise -- see `countForFilter`). */
export const GEOCODING_COUNTS_SQL = `${GEOCODING_PUBLISHED_CTE_SQL}
SELECT
  toString(count()) AS total,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.needs_attention})) AS needs_attention,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.geocoded})) AS geocoded,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.coarse})) AS coarse,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.ambiguous})) AS ambiguous,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.unmatched})) AS unmatched,
  toString(countIf(${GEOCODE_LIST_FILTER_SQL.no_outcome})) AS no_outcome
FROM published_companies AS p`;

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
