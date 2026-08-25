/**
 * The URL-facing filter state of `/admin/se/companies/geocoding`: the
 * class a company's published address's geocode fell into, and the search
 * string a toggle click navigates to.
 *
 * Client-safe (no ClickHouse import), mirroring se-company-info-filters.ts's
 * split from its `.server` query builder: the route's loader and the table
 * component both need this, and se-company-geocoding-list.server.ts imports
 * the class catalog so the SQL and the toggle can never name a different set
 * of classes.
 */

/**
 * The tab's own four-way read of a published address's `geocode_status`
 * (se_company_address, mirrored verbatim -- see se-company-geocoding-list.
 * server.ts for the SQL and the citation of Dagster's own GEOCODED_STATUSES):
 *
 * - "geocoded": a successful match with coordinates precise enough that
 *   Dagster itself calls it geocoded.
 * - "ambiguous": more than one OpenStreetMap candidate, no coordinate chosen.
 * - "unmatched": every other non-empty status -- 'unmatched', 'invalid_address',
 *   'foreign_address', 'postal_box' and 'property_identifier' alike. Each of
 *   those is a real, distinct outcome on the Address tab's own detail cards,
 *   but this tab's job is triage, not a taxonomy: none of them is a usable
 *   coordinate, so none of them is "geocoded".
 * - "no_outcome": geocode_status is '' -- the address has never reached the
 *   geocoder at all.
 */
export const GEOCODE_STATUS_CLASSES = [
  "geocoded",
  "ambiguous",
  "unmatched",
  "no_outcome",
] as const;

export type GeocodeStatusClass = (typeof GEOCODE_STATUS_CLASSES)[number];

/**
 * What the tab's toggle actually offers: every class, plus "all" (literally
 * every company with a published address, geocoded ones included) and the
 * default "needs_attention" (owner ruling: has an address but no successful
 * mapping -- ambiguous, unmatched or no_outcome). "needs_attention" is kept as
 * a first-class, re-selectable option rather than merely "no ?status= param",
 * the way the address-quality tab keeps "All reviewable" as its own toggle
 * item beside the narrower classes.
 */
export const GEOCODE_LIST_FILTERS = [
  "needs_attention",
  "all",
  ...GEOCODE_STATUS_CLASSES,
] as const;

export type GeocodeListFilter = (typeof GEOCODE_LIST_FILTERS)[number];

export const DEFAULT_GEOCODE_LIST_FILTER: GeocodeListFilter = "needs_attention";

/** The URL param this tab's toggle reads and writes. */
export const GEOCODE_STATUS_PARAM = "status";

export const GEOCODE_LIST_FILTER_LABELS: Record<GeocodeListFilter, string> = {
  needs_attention: "Needs attention",
  all: "All",
  geocoded: "Geocoded",
  ambiguous: "Ambiguous",
  unmatched: "Unmatched",
  no_outcome: "No outcome",
};

/** Any string in from a URL, whitelisted against the catalog above -- an
 * absent or unrecognised value (a stale bookmark, a hand-typed param) falls
 * back to the tab's own default rather than reading as "all" or erroring. */
export function parseGeocodeListFilter(value: string | null): GeocodeListFilter {
  return (GEOCODE_LIST_FILTERS as readonly string[]).includes(value ?? "")
    ? (value as GeocodeListFilter)
    : DEFAULT_GEOCODE_LIST_FILTER;
}

/** The search string for one toggle choice, preserving every other param
 * (page size included) but always dropping `page`: page 7 of the old class
 * means nothing once the class changes. */
export function geocodeListSearch(
  current: URLSearchParams,
  filter: GeocodeListFilter,
): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  if (filter === DEFAULT_GEOCODE_LIST_FILTER) next.delete(GEOCODE_STATUS_PARAM);
  else next.set(GEOCODE_STATUS_PARAM, filter);
  return `?${next.toString()}`;
}
