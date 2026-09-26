import { chStreamQuery } from "~/lib/clickhouse.server";
import {
  EMPTY_INFO_FILTERS,
  PROFILE_DATATYPES,
  PROFILE_SOURCE_VALUES,
  YES_NO_VALUES,
  type SeCompanyInfoTableFilters,
} from "~/lib/se-company-info-filters";
import { buildInfoListFilter, SE_COMPANIES_SERVING_TABLE } from "~/lib/se-company-info-lists.server";
import type { SeCompanySelection } from "~/lib/se-company-selection";

function companyIds(value: unknown): string[] {
  if (!Array.isArray(value) || value.some((id) => typeof id !== "string" || !/^([0-9]{10}|[0-9]{12})$/.test(id))) {
    throw new Error("Selection must contain valid Swedish company IDs.");
  }
  return [...new Set<string>(value)];
}

/** Fail closed for malformed bulk requests. Unlike URL browsing, an unknown
 * filter must not be silently dropped and turn an action into a wider one.
 * Require the complete applied filter object returned by the list loader. */
export function parseSeCompanySelection(value: unknown): SeCompanySelection {
  if (typeof value !== "object" || value === null || !("mode" in value)) {
    throw new Error("Choose companies or all companies matching a query.");
  }
  if (value.mode === "ids" && "companyIds" in value) {
    if (Object.keys(value).some((key) => !["mode", "companyIds"].includes(key))) {
      throw new Error("Invalid company selection fields.");
    }
    return { mode: "ids", companyIds: companyIds(value.companyIds) };
  }
  if (value.mode !== "query" || !("query" in value) || !("excludedCompanyIds" in value)) {
    throw new Error("Invalid company selection.");
  }
  if (Object.keys(value).some((key) => !["mode", "query", "excludedCompanyIds"].includes(key))) {
    throw new Error("Invalid company selection fields.");
  }
  const query = value.query;
  if (typeof query !== "object" || query === null || Array.isArray(query)) {
    throw new Error("A query selection requires the applied company filters.");
  }
  const fields = Object.keys(EMPTY_INFO_FILTERS);
  if (Object.keys(query).some((key) => !fields.includes(key))) {
    throw new Error("Unknown company filter.");
  }
  const filters = { ...EMPTY_INFO_FILTERS };
  for (const key of fields as (keyof SeCompanyInfoTableFilters)[]) {
    if (key === "datatypes") continue;
    const field = Object.getOwnPropertyDescriptor(query, key)?.value;
    if (typeof field !== "string" || field !== field.trim() || field === "any") {
      throw new Error(`Invalid company filter: ${key}.`);
    }
    filters[key] = field;
  }
  if (
    !["", "legal", "sole"].includes(filters.entity) ||
    !["", ...YES_NO_VALUES].includes(filters.description) ||
    !["", ...PROFILE_SOURCE_VALUES].includes(filters.source)
  ) {
    throw new Error("Invalid company filter value.");
  }
  const datatypes = Object.getOwnPropertyDescriptor(query, "datatypes")?.value;
  if (typeof datatypes !== "object" || datatypes === null || Array.isArray(datatypes)
      || Object.entries(datatypes).some(([key, presence]) =>
        !PROFILE_DATATYPES.some(datatype => datatype.key === key)
        || (presence !== "has" && presence !== "missing"))) {
    throw new Error("Invalid company datatype filter.");
  }
  filters.datatypes = {};
  for (const {key} of PROFILE_DATATYPES) {
    const presence = Object.getOwnPropertyDescriptor(datatypes, key)?.value;
    if (presence !== undefined) filters.datatypes[key] = presence;
  }
  return { mode: "query", query: filters, excludedCompanyIds: companyIds(value.excludedCompanyIds) };
}

/** Call from an action handler after validating its action name, passing the
 * submitted `selection`. Materialize against the same SE serving view and
 * predicates as the list, at execution time, with no pagination or row cap.
 * Stream rows so only the resulting ID list is retained in server memory. */
export async function resolveSeCompanySelection(value: unknown): Promise<string[]> {
  const selection = parseSeCompanySelection(value);
  if (selection.mode === "ids") return selection.companyIds;

  const { where, params } = buildInfoListFilter(selection.query);
  if (selection.excludedCompanyIds.length > 0) {
    where.push("i.company_id NOT IN {excludedCompanyIds:Array(String)}");
    params.excludedCompanyIds = selection.excludedCompanyIds;
  }
  const sql = `SELECT i.company_id AS company_id
FROM ${SE_COMPANIES_SERVING_TABLE} AS i
${where.length > 0 ? `WHERE ${where.join(" AND ")}` : ""}
ORDER BY i.company_id ASC`;
  const ids: string[] = [];
  for await (const row of chStreamQuery<{ company_id: string }>(sql, params)) {
    ids.push(row.company_id);
  }
  return ids;
}
