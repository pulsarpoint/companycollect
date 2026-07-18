import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";
import { FACET_COLUMN } from "~/lib/filters";

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

const COMPANIES_ALL = "companies_all";

// Country-scoped facets against companies_all. Column NAMES only ever come
// from the fixed FACET_COLUMN map (never a user-supplied facetKey); the
// country code is always bound via the {code:String} named param.
function facetSql(column: string): string {
  return `SELECT ${column} AS value, ${column} AS label, count() AS cnt
FROM ${COMPANIES_ALL}
WHERE country_code = {code:String} AND ${column} != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`;
}

const INDUSTRY_SQL = `SELECT industry_code AS value, any(industry_label) AS label, count() AS cnt
FROM ${COMPANIES_ALL}
WHERE country_code = {code:String} AND industry_code != ''
GROUP BY value
ORDER BY cnt DESC
LIMIT 50000`;

export async function getFacetOptions(
  country: CountryConfig,
  facetKey: string,
): Promise<FacetOption[]> {
  const cacheKey = `${country.code}:${facetKey}`;
  const hit = cache.get(cacheKey);
  if (hit && Date.now() - hit.loadedAt < TTL_MS) return hit.options;

  let sql: string;
  if (facetKey === "industry") {
    sql = INDUSTRY_SQL;
  } else {
    const column = Object.hasOwn(FACET_COLUMN, facetKey) ? FACET_COLUMN[facetKey] : undefined;
    if (!column) throw new Error(`unknown facet: ${facetKey}`);
    sql = facetSql(column);
  }
  const rows = await chQuery<{ value: string; label: string; cnt: string }>(sql, {
    code: country.code,
  });
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
