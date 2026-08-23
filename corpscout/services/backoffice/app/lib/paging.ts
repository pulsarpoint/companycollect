/**
 * Shared server-side-paging clamp for the admin list pages
 * (`?page=&pageSize=`). Client-safe (no ClickHouse import) so both route
 * loaders and the `.server` query builders they call can share one
 * definition instead of each keeping their own copy.
 */
export const MIN_PAGE_SIZE = 10;
export const MAX_PAGE_SIZE = 200;
export const DEFAULT_PAGE_SIZE = 50;

export function clampPageSize(value: number): number {
  const n = Math.trunc(value);
  const safe = Number.isFinite(n) && n > 0 ? n : DEFAULT_PAGE_SIZE;
  return Math.min(MAX_PAGE_SIZE, Math.max(MIN_PAGE_SIZE, safe));
}

export function clampPage(value: number): number {
  const n = Math.trunc(value);
  return Number.isFinite(n) && n > 0 ? n : 1;
}
