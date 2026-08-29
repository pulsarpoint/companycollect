/**
 * The URL-facing state of `/admin/technologies` (the catalog browser's
 * `?q=&category=&page=&pageSize=`) and of `/admin/technologies/:slug`'s
 * adoption tabs (`?tab=&country=&page=&pageSize=`). Mirrors
 * `se-people-sources.ts`: applied values are always strings ('' when absent,
 * never `undefined`), links are built from the CURRENT params plus a patch,
 * and any filter/tab/pageSize change resets `page`.
 *
 * Client-safe (no ClickHouse import): the route loader, the table component
 * and `technologies.server.ts`'s query builders all share these shapes.
 */
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";

/** Applied filter values only -- trimmed, '' when absent. */
export interface TechnologyListFilters {
  q: string;
  category: string;
}

function trimmed(value: string | null): string {
  return value?.trim() ?? "";
}

export function parseTechnologyListFilters(url: URL): TechnologyListFilters {
  return {
    q: trimmed(url.searchParams.get("q")),
    category: trimmed(url.searchParams.get("category")),
  };
}

export interface TechnologyListView {
  page: number;
  pageSize: number;
}

export function parseTechnologyListView(url: URL): TechnologyListView {
  return {
    page: clampPage(Number.parseInt(url.searchParams.get("page") || "1", 10)),
    pageSize: clampPageSize(
      Number.parseInt(
        url.searchParams.get("pageSize") || String(DEFAULT_PAGE_SIZE),
        10,
      ),
    ),
  };
}

/**
 * The next search string for a search submit, a category pick, or a page
 * link. Changing q, category or pageSize always drops `page` (page 7 of the
 * old filter means nothing once the filter changes).
 */
export function technologiesSearch(
  current: URLSearchParams,
  patch: {
    q?: string;
    category?: string;
    page?: number;
    pageSize?: number;
  },
): string {
  const next = new URLSearchParams(current);
  const resets =
    patch.q !== undefined ||
    patch.category !== undefined ||
    patch.pageSize !== undefined;
  if (resets) next.delete("page");

  if (patch.q !== undefined) {
    if (patch.q === "") next.delete("q");
    else next.set("q", patch.q);
  }
  if (patch.category !== undefined) {
    if (patch.category === "") next.delete("category");
    else next.set("category", patch.category);
  }
  if (patch.pageSize !== undefined) next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}

/** The detail page's link target, shared by the index table, the label link
 * mode and the tests so a renamed route cannot silently fork the URLs. */
export function technologyDetailPath(slug: string): string {
  return `/admin/technologies/${encodeURIComponent(slug)}`;
}

/* -------------------------------------------------------------------- */
/* Detail: the adoption section's two tabs                               */
/* -------------------------------------------------------------------- */

/**
 * The detail page's adoption tabs, in reading order: the crawled domains
 * carrying the technology (weekly `technology_top_domains` rollup, ordered by
 * harmonic centrality) and the companies using it (weekly
 * `technology_companies` rollup, filterable by country). One tab is a route
 * (this page), not two -- the active tab, the Companies country filter and
 * the paging all live in the detail route's `?tab=&country=&page=&pageSize=`
 * search params, mirroring `se-people-sources.ts`'s tab catalog.
 */
export const TECHNOLOGY_DETAIL_TABS = [
  { value: "domains", label: "Domains" },
  { value: "companies", label: "Companies" },
] as const;

export type TechnologyDetailTab =
  (typeof TECHNOLOGY_DETAIL_TABS)[number]["value"];

export const DEFAULT_TECHNOLOGY_DETAIL_TAB: TechnologyDetailTab = "domains";

const TECHNOLOGY_DETAIL_TAB_VALUES: readonly string[] =
  TECHNOLOGY_DETAIL_TABS.map((tab) => tab.value);

/** Any string in from a URL, whitelisted against the catalog -- an absent or
 * unrecognised `?tab=` falls back to Domains rather than erroring. */
export function parseTechnologyDetailTab(url: URL): TechnologyDetailTab {
  const value = url.searchParams.get("tab");
  return TECHNOLOGY_DETAIL_TAB_VALUES.includes(value ?? "")
    ? (value as TechnologyDetailTab)
    : DEFAULT_TECHNOLOGY_DETAIL_TAB;
}

/** The Companies tab's applied country filter -- '' means "all countries".
 * Country codes are stored uppercase ('SE'); normalize so a hand-typed
 * `?country=se` still matches. */
export function parseTechnologyDetailCountry(url: URL): string {
  return trimmed(url.searchParams.get("country")).toUpperCase();
}

/**
 * The next search string for a tab switch, a country pick, or a page link --
 * built from the CURRENT params plus a patch, never from component state.
 * Changing tab, country or pageSize always resets `page`; the default tab
 * and the "all countries" value keep the URL clean by deleting their params.
 */
export function technologyDetailSearch(
  current: URLSearchParams,
  patch: {
    tab?: TechnologyDetailTab;
    country?: string;
    page?: number;
    pageSize?: number;
  },
): string {
  const next = new URLSearchParams(current);
  const resets =
    patch.tab !== undefined ||
    patch.country !== undefined ||
    patch.pageSize !== undefined;
  if (resets) next.delete("page");

  if (patch.tab !== undefined) {
    if (patch.tab === DEFAULT_TECHNOLOGY_DETAIL_TAB) next.delete("tab");
    else next.set("tab", patch.tab);
    // The country filter belongs to the Companies tab; a tab switch starts
    // the target tab unfiltered instead of carrying a stale country along.
    next.delete("country");
  }
  if (patch.country !== undefined) {
    if (patch.country === "") next.delete("country");
    else next.set("country", patch.country);
  }
  if (patch.pageSize !== undefined) next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}

/**
 * The Companies tab's per-row company link. Sweden gets the admin company
 * technology area; every other country gets the public company page (there is
 * no per-country admin area for them yet). Client-safe and shared with the
 * tests so the two link shapes cannot silently fork.
 */
export function technologyCompanyPath(
  countryCode: string,
  companyId: string,
): string {
  if (countryCode === "SE") {
    return `/admin/se/company/${encodeURIComponent(companyId)}/technology`;
  }
  return `/company/${encodeURIComponent(countryCode.toLowerCase())}/${encodeURIComponent(companyId)}`;
}
