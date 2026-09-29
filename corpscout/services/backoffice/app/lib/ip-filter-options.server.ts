/**
 * Options with counts for the IP address list's ASN, country, region and city
 * pickers. Every list is read from an aggregate projection of
 * corpscout.ip_enrichment_search (asn_counts, country_counts, location_counts),
 * so a list costs a few thousand projection rows, not a 49M-row scan, and is
 * cached in process for ten minutes (the table itself changes once a day or
 * after an enrichment task).
 */
import { chQuery } from "~/lib/clickhouse.server";
import { rankFacetOptions, type FacetOption } from "~/lib/facets.server";
import { IP_SEARCH_RELATION } from "~/lib/workspace-ip-addresses.server";
import {
  isCountryCode,
  isRegionCode,
  normalizeAsn,
} from "~/lib/workspace-ip-addresses";

const TTL_MS = 10 * 60 * 1000;
const EMPTY_Q_LIMIT = 200;
const TYPED_Q_LIMIT = 50;
const SETTINGS = "SETTINGS max_threads=4, max_execution_time=20";

const cache = new Map<
  string,
  { loadedAt: number; pending: Promise<FacetOption[]> }
>();

export function clearIpFilterOptionCache(): void {
  cache.clear();
}

function cached(
  key: string,
  load: () => Promise<FacetOption[]>,
): Promise<FacetOption[]> {
  const hit = cache.get(key);
  if (hit && Date.now() - hit.loadedAt < TTL_MS) return hit.pending;
  const pending = load();
  cache.set(key, { loadedAt: Date.now(), pending });
  // A failed load is not cached: the next request tries again.
  pending.catch(() => {
    if (cache.get(key)?.pending === pending) cache.delete(key);
  });
  return pending;
}

function options(
  rows: { value: string; label: string; count: string | number }[],
): FacetOption[] {
  return rows.map((row) => ({
    value: String(row.value),
    label: row.label,
    count: Number(row.count),
  }));
}

/** Every known ASN with its most common organization name, largest first. */
export function getIpAsnOptions(): Promise<FacetOption[]> {
  return cached("asn", async () =>
    options(
      await chQuery(
        // The inner GROUP BY is the asn_counts projection's own shape.
        `SELECT toString(asn) AS value, argMax(asn_organization, addresses) AS label,
           toString(sum(addresses)) AS count
         FROM (
           SELECT asn, asn_organization, count() AS addresses FROM ${IP_SEARCH_RELATION}
           GROUP BY asn, asn_organization
         )
         WHERE asn != 0
         GROUP BY asn ORDER BY sum(addresses) DESC, asn ${SETTINGS}`,
      ),
    ),
  );
}

/** Rows of a count projection keyed by (value, label): one option per value,
 * counts summed, the most common label kept (names can differ between GeoLite2
 * builds). Selecting the label as a GROUP BY key, not any(label), is what lets
 * ClickHouse answer from the aggregate projection. */
function mergeLabels(
  rows: { value: string; label: string; count: string | number }[],
): FacetOption[] {
  const merged = new Map<string, FacetOption & { best: number }>();
  for (const row of rows) {
    const count = Number(row.count);
    const option = merged.get(row.value);
    if (!option) {
      merged.set(row.value, { value: row.value, label: row.label, count, best: count });
    } else {
      option.count += count;
      if (count > option.best) Object.assign(option, { label: row.label, best: count });
    }
  }
  return [...merged.values()]
    .map(({ value, label, count }) => ({ value, label, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
}

export function getIpCountryOptions(): Promise<FacetOption[]> {
  return cached("country", async () =>
    mergeLabels(
      await chQuery(
        `SELECT country_iso_code AS value, country_name AS label, toString(count()) AS count
         FROM ${IP_SEARCH_RELATION} WHERE country_iso_code != ''
         GROUP BY country_iso_code, country_name ${SETTINGS}`,
      ),
    ),
  );
}

export function getIpRegionOptions(country: string): Promise<FacetOption[]> {
  if (!isCountryCode(country)) return Promise.resolve([]);
  return cached(`region:${country}`, async () =>
    mergeLabels(
      await chQuery(
        `SELECT subdivision_iso_code AS value, subdivision_name AS label,
           toString(count()) AS count
         FROM ${IP_SEARCH_RELATION}
         WHERE country_iso_code = {country:String} AND subdivision_iso_code != ''
         GROUP BY subdivision_iso_code, subdivision_name ${SETTINGS}`,
        { country },
      ),
    ),
  );
}

export function getIpCityOptions(
  country: string,
  regions: string[],
): Promise<FacetOption[]> {
  if (!isCountryCode(country)) return Promise.resolve([]);
  const scoped = [...new Set(regions.filter(isRegionCode))].sort();
  return cached(`city:${country}:${scoped.join(",")}`, async () =>
    options(
      await chQuery(
        `SELECT city_name AS value, city_name AS label, toString(count()) AS count
         FROM ${IP_SEARCH_RELATION}
         WHERE country_iso_code = {country:String} AND city_name != ''
           ${scoped.length ? "AND subdivision_iso_code IN {regions:Array(String)}" : ""}
         GROUP BY city_name ORDER BY count() DESC LIMIT 20000 ${SETTINGS}`,
        { country, regions: scoped },
      ),
    ),
  );
}

export type IpFilterOptionKind = "asn" | "country" | "region" | "city";

/** Typeahead: prefix matches first, then substrings, largest first within each. */
export async function searchIpFilterOptions(
  kind: IpFilterOptionKind,
  q: string,
  scope: { country?: string; regions?: string[] } = {},
): Promise<FacetOption[]> {
  const all =
    kind === "asn"
      ? await getIpAsnOptions()
      : kind === "country"
        ? await getIpCountryOptions()
        : kind === "region"
          ? await getIpRegionOptions(scope.country ?? "")
          : await getIpCityOptions(scope.country ?? "", scope.regions ?? []);
  const trimmed = q.trim();
  if (trimmed === "") return all.slice(0, EMPTY_Q_LIMIT);
  // "AS15169" and "as 15169" search the number; other text the organization.
  const needle = kind === "asn" ? normalizeAsn(trimmed) || trimmed : trimmed;
  return rankFacetOptions(all, needle, TYPED_Q_LIMIT);
}

/** Display labels for the chosen values (chips), falling back to the value. */
export async function ipFilterLabels(filters: {
  asn: string[];
  country: string[];
  region: string[];
}): Promise<Record<string, string>> {
  const labels: Record<string, string> = {};
  const load = async (
    key: string,
    values: string[],
    list: () => Promise<FacetOption[]>,
  ) => {
    if (!values.length) return;
    try {
      const byValue = new Map((await list()).map((o) => [o.value, o.label]));
      for (const value of values) {
        const label = byValue.get(value);
        if (label) labels[`${key}:${value}`] = label;
      }
    } catch {
      // Labels are cosmetic: chips fall back to the raw value.
    }
  };
  await Promise.all([
    load("asn", filters.asn, getIpAsnOptions),
    load("country", filters.country, getIpCountryOptions),
    load("region", filters.region, () =>
      getIpRegionOptions(filters.country.length === 1 ? filters.country[0] : ""),
    ),
  ]);
  return labels;
}
