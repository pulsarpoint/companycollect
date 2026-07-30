/**
 * Country page tabs, in display order. Client-safe (no `.server` import) —
 * both the nested layout's tab nav and the overview route's legacy-redirect
 * check need it, and the layout renders inside the client bundle.
 */
export const COUNTRY_TABS = ["overview", "economy", "trade", "business", "contracts", "markets"] as const;
export type CountryTab = (typeof COUNTRY_TABS)[number];

export function isCountryTab(value: string): value is CountryTab {
  return (COUNTRY_TABS as readonly string[]).includes(value);
}

/**
 * Maps an old `?tab=` query value — from when /countries/:country switched
 * tabs with `setSearchParams` instead of routing — to its new nested route.
 * Returns null when the value is unrecognized (falls through to the index,
 * same as the old code's `parseTab` default) or is "overview" (the index
 * route already IS what `?tab=overview` meant, so there is nothing to
 * redirect to).
 */
export function legacyTabPath(countryCode: string, tabParam: string | null): string | null {
  if (tabParam === null || !isCountryTab(tabParam) || tabParam === "overview") return null;
  return `/countries/${countryCode}/${tabParam}`;
}
