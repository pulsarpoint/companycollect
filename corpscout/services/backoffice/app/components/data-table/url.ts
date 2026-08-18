import type { SortDir } from "~/lib/countries";
import { FILTER_PREFIX } from "~/lib/filters";

export interface TablePatch {
  q?: string;
  page?: number;
  pageSize?: number;
  sort?: string;
  dir?: SortDir;
}

/**
 * Builds the next table URL search string from the current params and a patch.
 * Changing q, sort, or pageSize resets pagination (deletes page).
 */
export function tableSearch(
  current: URLSearchParams,
  patch: TablePatch,
): string {
  const next = new URLSearchParams(current);
  const resets =
    patch.q !== undefined ||
    patch.sort !== undefined ||
    patch.pageSize !== undefined;
  if (resets) next.delete("page");
  if (patch.q !== undefined) next.set("q", patch.q);
  if (patch.sort !== undefined) next.set("sort", patch.sort);
  if (patch.dir !== undefined) next.set("dir", patch.dir);
  if (patch.pageSize !== undefined)
    next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}

/** Sort keys whose most useful first click is descending (biggest first). */
const DESC_FIRST = new Set(["revenue", "amount_original", "amount_usd"]);

export function nextSortDir(
  currentSort: string,
  currentDir: SortDir,
  key: string,
): SortDir {
  if (currentSort !== key) return DESC_FIRST.has(key) ? "desc" : "asc";
  return currentDir === "asc" ? "desc" : "asc";
}

/** Adds or removes one facet value; any filter change resets pagination. */
export function toggleFilterValue(
  current: URLSearchParams,
  key: string,
  value: string,
): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  const param = `${FILTER_PREFIX}${key}`;
  const values = next.getAll(param);
  next.delete(param);
  const remaining = values.filter((v) => v !== value);
  if (remaining.length === values.length) remaining.push(value);
  for (const v of remaining) next.append(param, v);
  return `?${next.toString()}`;
}

/** Replaces every selected value for one filter and resets pagination. */
export function setFilterValues(
  current: URLSearchParams,
  key: string,
  values: readonly string[],
): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  const param = `${FILTER_PREFIX}${key}`;
  next.delete(param);
  for (const value of values) next.append(param, value);
  return `?${next.toString()}`;
}

/**
 * Removes one facet value (idempotent, unlike toggleFilterValue). Badge-X
 * links must never re-add: during a pending navigation the badge row still
 * renders from the old loader data, so a toggle link on an already-removed
 * value would flip into an add link.
 */
export function removeFilterValue(
  current: URLSearchParams,
  key: string,
  value: string,
): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  const param = `${FILTER_PREFIX}${key}`;
  const remaining = next.getAll(param).filter((v) => v !== value);
  next.delete(param);
  for (const v of remaining) next.append(param, v);
  return `?${next.toString()}`;
}

export function clearAllFilters(current: URLSearchParams): string {
  const next = new URLSearchParams(current);
  next.delete("page");
  for (const key of [...next.keys()]) {
    if (key.startsWith(FILTER_PREFIX)) next.delete(key);
  }
  return `?${next.toString()}`;
}
