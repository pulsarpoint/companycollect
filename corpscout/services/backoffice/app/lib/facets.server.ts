import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

export interface FacetOption {
  value: string;
  label: string;
  count: number;
}

const TTL_MS = 24 * 60 * 60 * 1000;
const EMPTY_Q_LIMIT = 200;
const TYPED_Q_LIMIT = 50;

const cache = new Map<string, { loadedAt: number; options: FacetOption[] }>();

export function clearFacetCache(): void {
  cache.clear();
}

/** Lowercase + strip diacritics so "Osaühing" matches "osauhing". */
export function normalizeFacetText(s: string): string {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

/**
 * Substring match on normalized value OR label; prefix matches first,
 * ties by count descending.
 */
export function rankFacetOptions(
  options: FacetOption[],
  q: string,
  limit: number,
): FacetOption[] {
  const needle = normalizeFacetText(q);
  const prefix: FacetOption[] = [];
  const substring: FacetOption[] = [];
  for (const option of options) {
    const value = normalizeFacetText(option.value);
    const label = normalizeFacetText(option.label);
    if (value.startsWith(needle) || label.startsWith(needle)) {
      prefix.push(option);
    } else if (value.includes(needle) || label.includes(needle)) {
      substring.push(option);
    }
  }
  // Input lists are already count-sorted, so group order is preserved.
  return [...prefix, ...substring].slice(0, limit);
}

function facetSql(country: CountryConfig, facetKey: string): string {
  const column = country.columns.find(
    (c) => c.filterable && c.key === facetKey,
  );
  if (!column) throw new Error(`unknown facet: ${facetKey}`);
  // Identifiers/expressions come from the static registry only.
  return `SELECT toString(${column.expr}) AS value,
       toString(${column.expr}) AS label,
       count() AS cnt
FROM ${country.companiesTable}
WHERE toString(${column.expr}) != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`;
}

export async function getFacetOptions(
  country: CountryConfig,
  facetKey: string,
): Promise<FacetOption[]> {
  const cacheKey = `${country.code}:${facetKey}`;
  const hit = cache.get(cacheKey);
  if (hit && Date.now() - hit.loadedAt < TTL_MS) return hit.options;

  let sql: string;
  if (facetKey === "industry") {
    if (!country.industryFacetQuery) throw new Error(`unknown facet: ${facetKey}`);
    sql = country.industryFacetQuery;
  } else {
    sql = facetSql(country, facetKey);
  }
  const rows = await chQuery<{ value: string; label: string; cnt: string }>(sql);
  const options: FacetOption[] = rows.map((r) => ({
    value: r.value,
    label: r.label,
    count: Number(r.cnt),
  }));
  cache.set(cacheKey, { loadedAt: Date.now(), options });
  return options;
}

export async function searchFacetOptions(
  country: CountryConfig,
  facetKey: string,
  q: string,
): Promise<FacetOption[]> {
  const options = await getFacetOptions(country, facetKey);
  const trimmed = q.trim();
  if (trimmed === "") return options.slice(0, EMPTY_Q_LIMIT);
  return rankFacetOptions(options, trimmed, TYPED_Q_LIMIT);
}
