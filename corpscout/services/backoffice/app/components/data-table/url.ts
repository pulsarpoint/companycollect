import type { SortDir } from "~/lib/countries";

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
export function tableSearch(current: URLSearchParams, patch: TablePatch): string {
  const next = new URLSearchParams(current);
  const resets = patch.q !== undefined || patch.sort !== undefined || patch.pageSize !== undefined;
  if (resets) next.delete("page");
  if (patch.q !== undefined) next.set("q", patch.q);
  if (patch.sort !== undefined) next.set("sort", patch.sort);
  if (patch.dir !== undefined) next.set("dir", patch.dir);
  if (patch.pageSize !== undefined) next.set("pageSize", String(patch.pageSize));
  if (patch.page !== undefined) next.set("page", String(patch.page));
  return `?${next.toString()}`;
}

export function nextSortDir(currentSort: string, currentDir: SortDir, key: string): SortDir {
  if (currentSort !== key) return "asc";
  return currentDir === "asc" ? "desc" : "asc";
}
