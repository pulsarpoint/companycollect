import { COUNTRIES, type CountryConfig } from "~/lib/countries";

export const FILTER_PREFIX = "f_";
const MAX_VALUES_PER_FILTER = 50;

export type CompanyFilters = Record<string, string[]>;

/** Facet keys this country supports. Task 5 appends "industry". */
export function filterableFacetKeys(country: CountryConfig): string[] {
  const keys = country.columns.filter((c) => c.filterable).map((c) => c.key);
  if (country.industryFilterExpr) keys.push("industry");
  return keys;
}

/** Extracts whitelisted f_<key> params. Unknown keys are ignored, never errors. */
export function parseFilters(
  searchParams: URLSearchParams,
  country: CountryConfig,
): CompanyFilters {
  const filters: CompanyFilters = {};
  for (const key of filterableFacetKeys(country)) {
    const values = [
      ...new Set(
        searchParams
          .getAll(`${FILTER_PREFIX}${key}`)
          .map((v) => v.trim())
          .filter((v) => v !== ""),
      ),
    ].slice(0, MAX_VALUES_PER_FILTER);
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}

const COLUMN_FACET_KEYS = [
  ...new Set(COUNTRIES.flatMap((c) => c.columns.filter((col) => col.filterable).map((col) => col.key))),
];
export const UNIFIED_FACET_KEYS = ["country", "has_financials", ...COLUMN_FACET_KEYS, "industry"];

/**
 * Facet key → companies_all column name. SQL identifiers in WHERE/GROUP BY
 * clauses come ONLY from this fixed map, never from a user-supplied facet
 * key — the map's keys double as the whitelist.
 *
 * Adding a key here requires THREE other updates in lockstep: a matching
 * `companies_all` column (`dagster_v3/defs/companies_all/tables.py` +
 * `sql.py`), the dagster build's per-country SQL (`sql.py`) populating it,
 * and the parity sweep (`tests/companies-all-parity.test.ts`) asserting it.
 * `tests/companies-all-parity.test.ts`'s "FACET_COLUMN registry invariant"
 * test enforces the reverse direction (every `filterable: true` column in
 * `countries.ts` has a FACET_COLUMN entry) so a registry facet key can't
 * silently no-op the unified WHERE clause -- but it can't catch a
 * FACET_COLUMN key added without the companies_all/sql.py side, so do all
 * three together.
 */
export const FACET_COLUMN: Record<string, string> = {
  status: "status",
  legal_form: "legal_form",
  place: "place",
  size: "size",
};

export const UNIFIED_FACET_LABELS: Record<string, string> = {
  country: "Country",
  has_financials: "Has financials",
  status: "Status",
  legal_form: "Legal form",
  place: "Place",
  size: "Size",
  industry: "Industry",
};

const COUNTRY_CODES = new Set(COUNTRIES.map((c) => c.code));

export function parseUnifiedFilters(searchParams: URLSearchParams): CompanyFilters {
  const filters: CompanyFilters = {};
  for (const key of UNIFIED_FACET_KEYS) {
    let values = [
      ...new Set(
        searchParams.getAll(`${FILTER_PREFIX}${key}`).map((v) => v.trim()).filter((v) => v !== ""),
      ),
    ].slice(0, MAX_VALUES_PER_FILTER);
    if (key === "country") values = values.filter((v) => COUNTRY_CODES.has(v));
    if (key === "has_financials") values = values.filter((v) => v === "true");
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}
