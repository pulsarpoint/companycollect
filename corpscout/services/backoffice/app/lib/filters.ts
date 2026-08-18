import type { CountryConfig } from "~/lib/countries";
import { flagFilterKeys } from "~/lib/company-flags";
import {
  FINANCIAL_FILING_FILTER_KEY,
  isFinancialFilingStatus,
} from "~/lib/financial-filing-status";

export const FILTER_PREFIX = "f_";
const MAX_VALUES_PER_FILTER = 50;

export type CompanyFilters = Record<string, string[]>;

/** Facet keys this country supports, from its own column registry. */
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
  // Flag filters are whitelisted here but NOT in filterableFacetKeys: they are
  // yes/no toggles, not a searchable list of a column's distinct values.
  const keys = [
    ...filterableFacetKeys(country),
    ...flagFilterKeys(country.code),
    ...(country.code === "se" ? [FINANCIAL_FILING_FILTER_KEY] : []),
  ];
  for (const key of keys) {
    const values = [
      ...new Set(
        searchParams
          .getAll(`${FILTER_PREFIX}${key}`)
          .map((v) => v.trim())
          .filter((v) => v !== ""),
      ),
    ]
      .filter(
        (value) =>
          key !== FINANCIAL_FILING_FILTER_KEY || isFinancialFilingStatus(value),
      )
      .slice(0, MAX_VALUES_PER_FILTER);
    if (values.length > 0) filters[key] = values;
  }
  return filters;
}
