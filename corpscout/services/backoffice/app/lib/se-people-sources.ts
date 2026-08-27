/**
 * The URL-facing state of `/admin/se/people`: a tabbed browser over the three
 * SE person SOURCE views plus the resolved `se_company_person` table. One tab
 * is a route (this page), not four -- so the active tab, its page and its two
 * filters (company id, name) all live in THIS route's `?tab=&page=&companyId=
 * &name=` search params, and switching tabs is a normal loader navigation,
 * exactly like `se-company-geocoding-filters.ts`'s status toggle.
 *
 * Client-safe (no ClickHouse import): the route's loader, the table component
 * and `se-people-sources.server.ts`'s query builders all need this catalog,
 * so a tab can never be selectable here without a matching query builder
 * there, or vice versa.
 */
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";

/**
 * The five tabs, in reading order: the three raw source views (bolagsverket,
 * esef, wikidata -- Sweden's uniform person source read, see dagster_v3's
 * `company_people/source_views.py`), the resolved `se_company_person` table
 * those sources feed, and Tasks (every people asset/job's latest-run stats,
 * `se-people-tasks.server.ts`). "final" is empty until the owner runs the
 * pipeline's clean-copy step -- an empty tab with a zero count is the
 * correct, unsurprising state, not an error. "tasks" has no company id/name
 * filter and no pagination (see `SePeopleSourcesTable`'s tab switch) -- it is
 * not a row-per-company table like the other four.
 */
export const SE_PEOPLE_SOURCE_TABS = [
  { value: "bolagsverket", label: "Bolagsverket" },
  { value: "esef", label: "ESEF" },
  { value: "wikidata", label: "Wikidata" },
  { value: "final", label: "People (final)" },
  { value: "tasks", label: "Tasks" },
] as const;

export type SePeopleSourceTab = (typeof SE_PEOPLE_SOURCE_TABS)[number]["value"];

export const DEFAULT_SE_PEOPLE_SOURCE_TAB: SePeopleSourceTab = "bolagsverket";

const SE_PEOPLE_SOURCE_TAB_VALUES: readonly string[] = SE_PEOPLE_SOURCE_TABS.map(
  (tab) => tab.value,
);

/** Any string in from a URL, whitelisted against the catalog -- an absent or
 * unrecognised `?tab=` (a stale bookmark, a hand-typed value) falls back to
 * the first tab rather than reading as "final" or erroring. */
export function parseSePeopleSourceTab(url: URL): SePeopleSourceTab {
  const value = url.searchParams.get("tab");
  return SE_PEOPLE_SOURCE_TAB_VALUES.includes(value ?? "")
    ? (value as SePeopleSourceTab)
    : DEFAULT_SE_PEOPLE_SOURCE_TAB;
}

/** Applied filter values only -- trimmed, and '' when absent, never
 * `undefined`, so the component always has a string to put back in an
 * input's `defaultValue`. */
export interface SePeopleSourceFilters {
  companyId: string;
  name: string;
}

function trimmed(value: string | null): string {
  return value?.trim() ?? "";
}

export function parseSePeopleSourceFilters(url: URL): SePeopleSourceFilters {
  return {
    companyId: trimmed(url.searchParams.get("companyId")),
    name: trimmed(url.searchParams.get("name")),
  };
}

export interface SePeopleSourceView {
  page: number;
  pageSize: number;
}

export function parseSePeopleSourceView(url: URL): SePeopleSourceView {
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
 * The next search string for a tab switch, a filter form submit, or a page
 * link -- built from the CURRENT params plus a patch, never from component
 * state, so a link is identical on the server render and in a test.
 * Changing tab, companyId or name always resets `page` (page 7 of the old
 * tab/filter means nothing once either changes); `pageSize` rides along
 * unless the patch itself changes it.
 */
export function sePeopleSourcesSearch(
  current: URLSearchParams,
  patch: {
    tab?: SePeopleSourceTab;
    companyId?: string;
    name?: string;
    page?: number;
    pageSize?: number;
  },
): string {
  const next = new URLSearchParams(current);
  const resets =
    patch.tab !== undefined ||
    patch.companyId !== undefined ||
    patch.name !== undefined ||
    patch.pageSize !== undefined;
  if (resets) next.delete("page");

  if (patch.tab !== undefined) {
    if (patch.tab === DEFAULT_SE_PEOPLE_SOURCE_TAB) next.delete("tab");
    else next.set("tab", patch.tab);
  }
  if (patch.companyId !== undefined) {
    if (patch.companyId === "") next.delete("companyId");
    else next.set("companyId", patch.companyId);
  }
  if (patch.name !== undefined) {
    if (patch.name === "") next.delete("name");
    else next.set("name", patch.name);
  }
  if (patch.pageSize !== undefined) next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}
