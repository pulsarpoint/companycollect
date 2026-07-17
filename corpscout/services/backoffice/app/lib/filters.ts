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
export const UNIFIED_FACET_KEYS = ["country", ...COLUMN_FACET_KEYS, "industry"];

export const UNIFIED_FACET_LABELS: Record<string, string> = {
  country: "Country",
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
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}
