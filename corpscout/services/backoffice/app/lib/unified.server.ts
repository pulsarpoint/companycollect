import { chQuery } from "~/lib/clickhouse.server";
import { getCountry, PAGE_SIZES, type SortDir } from "~/lib/countries";
import { FACET_COLUMN, UNIFIED_FACET_KEYS, type CompanyFilters } from "~/lib/filters";
import { rankFacetOptions, type FacetOption } from "~/lib/facets.server";

export interface UnifiedRow {
  country_code: string;
  id: string;
  name: string;
  active: 0 | 1;
  industry_code?: string | null;
  industry_label?: string | null;
  revenue_usd: number | null;
  fiscal_year: number | null;
}

export interface UnifiedSearchResult {
  rows: UnifiedRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
}

const UNIFIED_SORTS = new Set(["country", "name", "revenue"]);
const COMPANIES_ALL = "companies_all";

/**
 * Builds the WHERE clause for companies_all from parseUnifiedFilters'
 * output. Column NAMES only ever come from the fixed FACET_COLUMN map (or
 * literal column references below) — never from a user-supplied key.
 * User VALUES are always bound via named params.
 */
function buildWhere(
  q: string,
  filters: CompanyFilters,
  params: Record<string, unknown>,
): string {
  const conds: string[] = [];

  const country = filters.country;
  if (country?.length) {
    conds.push(`country_code IN {f_country:Array(String)}`);
    params.f_country = country;
  }
  if (filters.has_financials?.length) {
    conds.push(`has_financials = 1`);
  }
  const industry = filters.industry;
  if (industry?.length) {
    conds.push(`industry_code IN {f_industry:Array(String)}`);
    params.f_industry = industry;
  }
  for (const [key, column] of Object.entries(FACET_COLUMN)) {
    const values = filters[key];
    if (!values?.length) continue;
    conds.push(`${column} IN {f_${key}:Array(String)}`);
    params[`f_${key}`] = values;
  }
  if (q) {
    conds.push(`name_normalized LIKE {pattern:String}`);
    params.pattern = `%${q.toLowerCase()}%`;
  }

  return conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
}

interface CompaniesAllRow {
  country_code: string;
  id: string;
  name: string;
  active: 0 | 1;
  industry_code: string;
  industry_label: string;
  revenue_usd: number | null;
  fiscal_year: number | null;
}

export async function searchUnifiedCompanies(opts: {
  q?: string;
  page?: number;
  pageSize?: number;
  sort?: string | null;
  dir?: string | null;
  filters?: CompanyFilters;
}): Promise<UnifiedSearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const q = (opts.q ?? "").trim();
  const filters = opts.filters ?? {};
  const sort = UNIFIED_SORTS.has(opts.sort ?? "") ? (opts.sort as string) : "country";
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";

  const params: Record<string, unknown> = {};
  const where = buildWhere(q, filters, params);

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${COMPANIES_ALL} ${where}`,
    params,
  );
  const total = Number(countRows[0].total);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const requestedRaw = Number.isFinite(opts.page as number) ? Math.trunc(opts.page as number) : 1;
  const page = Math.min(Math.max(1, requestedRaw), lastPage);

  const dirSql = dir === "desc" ? "DESC" : "ASC";
  const orderBy =
    sort === "name"
      ? `name_normalized = '' ASC, name_normalized ${dirSql}, country_code, company_id`
      : sort === "revenue"
        ? `isNull(revenue_usd) ASC, revenue_usd ${dirSql}, country_code, company_id`
        : `country_code ${dirSql}, company_id ${dirSql}`;

  const rows = await chQuery<CompaniesAllRow>(
    `SELECT country_code, company_id AS id, name, is_active AS active,
       industry_code, industry_label, revenue_usd, fiscal_year
     FROM ${COMPANIES_ALL}
     ${where}
     ORDER BY ${orderBy}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  const mapped: UnifiedRow[] = rows.map((row) => ({
    country_code: row.country_code,
    id: row.id,
    name: row.name,
    active: row.active,
    industry_code: row.industry_code === "" ? null : row.industry_code,
    industry_label: row.industry_label === "" ? null : row.industry_label,
    revenue_usd: row.revenue_usd,
    fiscal_year: row.fiscal_year,
  }));

  return { rows: mapped, total, page, pageSize, sort, dir };
}

// ---------- Unified facets ----------
// One table, one query path: every facet below is a single GROUP BY (or
// aggregate) over companies_all — no per-country branching or merge.

const FACET_TTL_MS = 24 * 60 * 60 * 1000;
const facetCache = new Map<string, { loadedAt: number; options: FacetOption[] }>();

function cached(key: string): FacetOption[] | undefined {
  const hit = facetCache.get(key);
  if (hit && Date.now() - hit.loadedAt < FACET_TTL_MS) return hit.options;
  return undefined;
}

function store(key: string, options: FacetOption[]): FacetOption[] {
  facetCache.set(key, { loadedAt: Date.now(), options });
  return options;
}

async function countryFacet(): Promise<FacetOption[]> {
  const hit = cached("country");
  if (hit) return hit;
  const rows = await chQuery<{ value: string; cnt: string }>(
    `SELECT country_code AS value, count() AS cnt FROM ${COMPANIES_ALL} GROUP BY value ORDER BY cnt DESC`,
  );
  const options = rows.map((r) => ({
    value: r.value,
    label: getCountry(r.value)?.name ?? r.value,
    count: Number(r.cnt),
  }));
  return store("country", options);
}

async function hasFinancialsFacet(): Promise<FacetOption[]> {
  const hit = cached("has_financials");
  if (hit) return hit;
  const rows = await chQuery<{ value: string; label: string; cnt: string }>(
    `SELECT 'true' AS value, 'yes' AS label, countIf(has_financials = 1) AS cnt FROM ${COMPANIES_ALL}`,
  );
  const options = rows.map((r) => ({ value: r.value, label: r.label, count: Number(r.cnt) }));
  return store("has_financials", options);
}

async function industryFacet(): Promise<FacetOption[]> {
  const hit = cached("industry");
  if (hit) return hit;
  const rows = await chQuery<{ value: string; label: string; cnt: string }>(
    `SELECT industry_code AS value, any(industry_label) AS label, count() AS cnt
     FROM ${COMPANIES_ALL}
     WHERE industry_code != ''
     GROUP BY value
     ORDER BY cnt DESC
     LIMIT 50000`,
  );
  const options = rows.map((r) => ({
    value: r.value,
    label: r.label || r.value,
    count: Number(r.cnt),
  }));
  return store("industry", options);
}

async function columnFacet(facetKey: string, column: string): Promise<FacetOption[]> {
  const hit = cached(facetKey);
  if (hit) return hit;
  const rows = await chQuery<{ value: string; cnt: string }>(
    `SELECT ${column} AS value, count() AS cnt
     FROM ${COMPANIES_ALL}
     WHERE ${column} != ''
     GROUP BY value
     ORDER BY cnt DESC
     LIMIT 50000`,
  );
  const options = rows.map((r) => ({ value: r.value, label: r.value, count: Number(r.cnt) }));
  return store(facetKey, options);
}

export async function getUnifiedFacetOptions(facetKey: string): Promise<FacetOption[]> {
  if (!UNIFIED_FACET_KEYS.includes(facetKey)) throw new Error(`unknown facet: ${facetKey}`);
  if (facetKey === "country") return countryFacet();
  if (facetKey === "has_financials") return hasFinancialsFacet();
  if (facetKey === "industry") return industryFacet();
  const column = FACET_COLUMN[facetKey];
  if (!column) throw new Error(`unknown facet: ${facetKey}`);
  return columnFacet(facetKey, column);
}

export async function searchUnifiedFacetOptions(facetKey: string, q: string): Promise<FacetOption[]> {
  const options = await getUnifiedFacetOptions(facetKey);
  const trimmed = q.trim();
  if (trimmed === "") return options.slice(0, 200);
  return rankFacetOptions(options, trimmed, 50);
}
