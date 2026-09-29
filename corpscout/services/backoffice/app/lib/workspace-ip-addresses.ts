/**
 * URL-facing filter state of `/admin/ip-addresses`. Client-safe (no ClickHouse
 * import): the route component, the filter sheet, the selection and the loader
 * share one definition. Every helper derives its href from the state it is
 * handed, never from the live location.
 *
 * Multi-value filters repeat their parameter (`?asn=15169&asn=3301`). Region and
 * city only apply when exactly one country is chosen: subdivision codes are only
 * unique within a country, and a city list across countries is not useful.
 */

export interface WorkspaceIpFilters {
  search: string;
  version: "any" | "4" | "6";
  /** Autonomous system numbers as digit strings ("15169"). */
  asn: string[];
  /** ISO 3166-1 alpha-2 country codes, upper case. */
  country: string[];
  /** Subdivision ISO codes within the single chosen country. */
  region: string[];
  /** City names within the single chosen country. */
  city: string[];
}

export const EMPTY_WORKSPACE_IP_FILTERS: WorkspaceIpFilters = {
  search: "",
  version: "any",
  asn: [],
  country: [],
  region: [],
  city: [],
};

export const IP_LIST_FILTER_KEYS = ["asn", "country", "region", "city"] as const;
export type IpListFilterKey = (typeof IP_LIST_FILTER_KEYS)[number];

/** The most values one multi-value filter accepts (URL, selection and Dagster config). */
export const MAX_FILTER_VALUES = 50;
const MAX_ASN = 4294967295;
const MAX_CITY_LENGTH = 200;

/** "15169", "AS15169" or "as 15169" → "15169"; anything else → "". */
export function normalizeAsn(value: string): string {
  const match = /^(?:as\s*)?(\d{1,10})$/i.exec(value.trim());
  if (!match) return "";
  const number = Number(match[1]);
  return number > 0 && number <= MAX_ASN ? String(number) : "";
}

export function isCountryCode(value: string): boolean {
  return /^[A-Z]{2}$/.test(value);
}

export function isRegionCode(value: string): boolean {
  return /^[A-Z0-9]{1,10}$/.test(value);
}

export function isCityName(value: string): boolean {
  return (
    value.length > 0 &&
    value.length <= MAX_CITY_LENGTH &&
    value.trim() === value &&
    // No control characters; names are shown and bound as parameters only.
    // eslint-disable-next-line no-control-regex
    !/[\u0000-\u001f\u007f]/.test(value)
  );
}

function unique(values: string[]): string[] {
  return [...new Set(values)].slice(0, MAX_FILTER_VALUES);
}

/** Region and city are dropped unless exactly one country remains. */
export function withLocationScope(
  filters: WorkspaceIpFilters,
): WorkspaceIpFilters {
  return filters.country.length === 1
    ? filters
    : { ...filters, region: [], city: [] };
}

export function parseWorkspaceIpFilters(
  params: URLSearchParams,
): WorkspaceIpFilters {
  const version = params.get("version");
  return withLocationScope({
    search: (params.get("search") ?? "").trim().toLowerCase(),
    version: version === "4" || version === "6" ? version : "any",
    asn: unique(params.getAll("asn").map(normalizeAsn).filter(Boolean)),
    country: unique(
      params
        .getAll("country")
        .map((value) => value.trim().toUpperCase())
        .filter(isCountryCode),
    ),
    region: unique(
      params
        .getAll("region")
        .map((value) => value.trim().toUpperCase())
        .filter(isRegionCode),
    ),
    city: unique(
      params
        .getAll("city")
        .map((value) => value.trim())
        .filter(isCityName),
    ),
  });
}

/** Key order of the list: ASN filters and the unfiltered list page through the
 * table's sort key; location-only filters through the location projection. */
export function workspaceIpListOrder(
  filters: WorkspaceIpFilters,
): "asn" | "location" {
  return filters.asn.length === 0 && filters.country.length > 0
    ? "location"
    : "asn";
}

export function workspaceIpAddressesHref(
  filters: WorkspaceIpFilters,
  after = "",
) {
  const params = new URLSearchParams();
  if (filters.search) params.set("search", filters.search);
  if (filters.version !== "any") params.set("version", filters.version);
  const scoped = withLocationScope(filters);
  for (const key of IP_LIST_FILTER_KEYS)
    for (const value of scoped[key]) params.append(key, value);
  if (after) params.set("after", after);
  return `/admin/ip-addresses${params.size ? `?${params}` : ""}`;
}

/** The same filters without one chip's value (a chip is `key:value`). */
export function withoutFilterValue(
  filters: WorkspaceIpFilters,
  chip: string,
): WorkspaceIpFilters {
  const separator = chip.indexOf(":");
  const key = chip.slice(0, separator);
  const value = chip.slice(separator + 1);
  if (!(IP_LIST_FILTER_KEYS as readonly string[]).includes(key)) return filters;
  const filterKey = key as IpListFilterKey;
  return withLocationScope({
    ...filters,
    [filterKey]: filters[filterKey].filter((item) => item !== value),
  });
}
