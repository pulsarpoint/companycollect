import { chQuery } from "~/lib/clickhouse.server";
import {
  COUNTRIES,
  getCountry,
  MAX_UNIFIED_PAGE,
  PAGE_SIZES,
  type CountryConfig,
  type SortDir,
} from "~/lib/countries";

export { MAX_UNIFIED_PAGE };
import { UNIFIED_FACET_KEYS, type CompanyFilters } from "~/lib/filters";
import { getFacetOptions, rankFacetOptions, type FacetOption } from "~/lib/facets.server";

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

function canAnswer(c: CountryConfig, key: string): boolean {
  if (key === "industry") return Boolean(c.industryFilterExpr);
  if (key === "has_financials") return Boolean(c.financialsLatest);
  return c.columns.some((col) => col.filterable && col.key === key);
}

function branchCountries(filters: CompanyFilters): CountryConfig[] {
  let list = COUNTRIES;
  const wanted = filters.country;
  if (wanted?.length) {
    const set = new Set(wanted);
    list = list.filter((c) => set.has(c.code));
  }
  const activeKeys = Object.keys(filters).filter(
    (k) => k !== "country" && (filters[k]?.length ?? 0) > 0,
  );
  return list.filter((c) => activeKeys.every((k) => canAnswer(c, k)));
}

function branchWhere(
  c: CountryConfig,
  q: string,
  filters: CompanyFilters,
  params: Record<string, unknown>,
): string {
  const conds: string[] = [];
  if (q) {
    conds.push(`${c.nameColumn} ILIKE {pattern:String}`);
    params.pattern = `%${q}%`;
  }
  for (const col of c.columns) {
    if (!col.filterable) continue;
    const values = filters[col.key];
    if (!values || values.length === 0) continue;
    conds.push(`${col.expr} IN {f_${col.key}:Array(String)}`);
    params[`f_${col.key}`] = values;
  }
  const industry = filters.industry;
  if (industry?.length && c.industryFilterExpr) {
    conds.push(c.industryFilterExpr);
    params.f_industry = industry;
  }
  if (filters.has_financials?.length && c.financialsLatest) {
    conds.push(`${c.financialsLatest.companyKeyExpr} IN (SELECT company_id FROM ${c.financialsLatest.table})`);
  }
  return conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";
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
  const branches = branchCountries(filters);
  if (branches.length === 0) {
    return { rows: [], total: 0, page: 1, pageSize, sort, dir };
  }

  const params: Record<string, unknown> = {};
  const whereByCode = new Map(branches.map((c) => [c.code, branchWhere(c, q, filters, params)]));

  const countSql = branches
    .map((c) => `SELECT count() AS c FROM ${c.companiesTable} ${whereByCode.get(c.code)}`)
    .join(" UNION ALL ");
  const countRows = await chQuery<{ total: string }>(
    `SELECT sum(c) AS total FROM (${countSql})`,
    params,
  );
  const total = Number(countRows[0].total);
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const requestedRaw = Number.isFinite(opts.page as number) ? Math.trunc(opts.page as number) : 1;
  const page = Math.min(Math.max(1, requestedRaw), lastPage, MAX_UNIFIED_PAGE);

  const dirSql = dir === "desc" ? "DESC" : "ASC";
  // Merge-order invariant: branch ORDER BY native idColumn == outer ORDER BY
  // toString(id) ONLY because every registry idColumn is String (asserted in
  // the live-schema sweep).
  const branchSql = (c: CountryConfig) => {
    const ik = c.industryJoinKeyExpr ?? c.idColumn;
    const fin = c.financialsLatest;
    // Every branch emits identically-typed revenue_usd/fiscal_year columns —
    // countries without a financialsLatest summary table emit typed NULL
    // literals so the UNION ALL columns line up.
    const finSelect = fin
      ? `toNullable(fin.revenue_amount_usd) AS revenue_usd, toNullable(fin.fiscal_year) AS fiscal_year`
      : `CAST(NULL AS Nullable(Float64)) AS revenue_usd, CAST(NULL AS Nullable(Int32)) AS fiscal_year`;
    const finJoin = fin
      ? `LEFT JOIN ${fin.table} AS fin ON fin.company_id = toString(${fin.companyKeyExpr})`
      : "";
    const orderBy =
      sort === "revenue"
        ? `isNull(revenue_usd) ASC, revenue_usd ${dirSql}, ${c.idColumn}`
        : (() => {
            const sortExpr = sort === "name" ? c.nameColumn : c.idColumn;
            return `coalesce(toString(${sortExpr}), '') = '' ASC, ${sortExpr} ${dirSql}, ${c.idColumn}`;
          })();
    return `SELECT '${c.code}' AS country_code, toString(${c.idColumn}) AS id, ${c.nameColumn} AS name,
      toUInt8(${c.activeExpr}) AS active, toString(${ik}) AS __ik, ${finSelect}
    FROM ${c.companiesTable} ${finJoin} ${whereByCode.get(c.code)}
    ORDER BY ${orderBy}
    LIMIT ${page * pageSize}`;
  };
  const outerSort =
    sort === "name"
      ? `coalesce(name, '') = '' ASC, name ${dirSql}, country_code, id`
      : sort === "revenue"
        ? `isNull(revenue_usd) ASC, revenue_usd ${dirSql}, country_code, id`
        : `country_code ${dirSql}, id ${dirSql}`;

  const rows = await chQuery<UnifiedRow & { __ik?: string }>(
    `SELECT country_code, id, name, active, __ik, revenue_usd, fiscal_year
     FROM (${branches.map(branchSql).join(" UNION ALL ")})
     ORDER BY ${outerSort}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  // Per-country industry merge for just the visible page.
  const byCountry = new Map<string, (UnifiedRow & { __ik?: string })[]>();
  for (const row of rows) {
    const group = byCountry.get(row.country_code) ?? [];
    group.push(row);
    byCountry.set(row.country_code, group);
  }
  await Promise.all(
    [...byCountry.entries()].map(async ([code, group]) => {
      const country = getCountry(code)!;
      if (!country.industryQuery) {
        for (const row of group) {
          row.industry_code = null;
          row.industry_label = null;
          delete row.__ik;
        }
        return;
      }
      const ids = group.map((r) => r.__ik ?? "").filter((v) => v !== "");
      const industries = ids.length
        ? await chQuery<{ company_id: string; industry_code: string | null; industry_label: string | null }>(
            country.industryQuery,
            { ids },
          )
        : [];
      const byId = new Map(industries.map((i) => [i.company_id, i]));
      for (const row of group) {
        const hit = byId.get(row.__ik ?? "");
        row.industry_code = hit?.industry_code ?? null;
        row.industry_label = hit?.industry_label ?? null;
        delete row.__ik;
      }
    }),
  );

  return { rows, total, page, pageSize, sort, dir };
}

// ---------- Unified facets ----------

let countryFacetCache: { loadedAt: number; options: FacetOption[] } | null = null;
const COUNTRY_FACET_TTL_MS = 24 * 60 * 60 * 1000;

async function countryFacet(): Promise<FacetOption[]> {
  if (countryFacetCache && Date.now() - countryFacetCache.loadedAt < COUNTRY_FACET_TTL_MS) {
    return countryFacetCache.options;
  }
  const sql = COUNTRIES.map(
    (c) => `SELECT '${c.code}' AS value, count() AS cnt FROM ${c.companiesTable}`,
  ).join(" UNION ALL ");
  const rows = await chQuery<{ value: string; cnt: string }>(`SELECT value, cnt FROM (${sql}) ORDER BY cnt DESC`);
  const options = rows.map((r) => ({
    value: r.value,
    label: getCountry(r.value)?.name ?? r.value,
    count: Number(r.cnt),
  }));
  countryFacetCache = { loadedAt: Date.now(), options };
  return options;
}

export async function getUnifiedFacetOptions(facetKey: string): Promise<FacetOption[]> {
  if (!UNIFIED_FACET_KEYS.includes(facetKey)) throw new Error(`unknown facet: ${facetKey}`);
  if (facetKey === "country") return countryFacet();

  const countries = COUNTRIES.filter((c) => canAnswer(c, facetKey));
  const lists = await Promise.all(countries.map((c) => getFacetOptions(c, facetKey)));
  const merged = new Map<string, FacetOption>();
  for (const list of lists) {
    for (const option of list) {
      const existing = merged.get(option.value);
      if (existing) {
        existing.count += option.count;
        if (existing.label === existing.value && option.label !== option.value) {
          existing.label = option.label;
        }
      } else {
        merged.set(option.value, { ...option });
      }
    }
  }
  return [...merged.values()].sort((a, b) => b.count - a.count);
}

export async function searchUnifiedFacetOptions(facetKey: string, q: string): Promise<FacetOption[]> {
  const options = await getUnifiedFacetOptions(facetKey);
  const trimmed = q.trim();
  if (trimmed === "") return options.slice(0, 200);
  return rankFacetOptions(options, trimmed, 50);
}
