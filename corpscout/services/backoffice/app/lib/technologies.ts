/**
 * The URL-facing state of `/admin/technologies`: the catalog browser's search
 * (`?q=`), its optional category filter (`?category=`) and its paging
 * (`?page=&pageSize=`). Mirrors `se-people-sources.ts`: applied values are
 * always strings ('' when absent, never `undefined`), links are built from
 * the CURRENT params plus a patch, and any q/category/pageSize change resets
 * `page`.
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

/** Hard cap on the detail page's live "Swedish companies using it" list --
 * client-safe (the section's caption prints it) and the LIMIT baked into
 * `technologies.server.ts`'s SE companies SQL. */
export const SE_COMPANIES_USING_TECHNOLOGY_LIMIT = 100;
