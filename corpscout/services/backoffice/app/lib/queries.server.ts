import { chQuery } from "~/lib/clickhouse.server";
import {
  getSortColumn,
  PAGE_SIZES,
  type CountryConfig,
  type SortDir,
} from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";

export interface CountryStats {
  total: number;
  active: number;
}

function clampInt(value: number | undefined, min: number, max: number, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(value)));
}

export async function getCountryStats(country: CountryConfig): Promise<CountryStats> {
  // Table/column identifiers come from the static registry, never from users.
  const rows = await chQuery<{ total: string; active: string }>(
    `SELECT count() AS total, countIf(${country.activeExpr}) AS active
     FROM ${country.companiesTable}`,
  );
  const row = rows[0];
  return { total: Number(row.total), active: Number(row.active) };
}

// Re-exported for callers that historically imported PAGE_SIZES from here
// (e.g. Task 2's tests). Source of truth moved to ~/lib/countries so
// client-bundled code (data-table/pagination.tsx) doesn't import a .server
// module.
export { PAGE_SIZES };

export type CompanyListRow = Record<string, string | number | null> & {
  active: 0 | 1;
};

export interface CompanySearchResult {
  rows: CompanyListRow[];
  total: number;
  page: number;
  pageSize: number;
  sort: string;
  dir: SortDir;
}

interface IndustryRow {
  company_id: string;
  industry_code: string | null;
  industry_label: string | null;
}

function buildCompanySelectList(country: CountryConfig): string {
  const joinKeyExpr = country.industryJoinKeyExpr ?? country.idColumn;
  return [
    ...country.columns.map((c) => `${c.expr} AS ${c.key}`),
    `toUInt8(${country.activeExpr}) AS active`,
    ...(country.industryQuery ? [`toString(${joinKeyExpr}) AS __industry_key`] : []),
  ].join(",\n       ");
}

export async function searchCompanies(
  country: CountryConfig,
  opts: {
    q?: string;
    page?: number;
    pageSize?: number;
    sort?: string | null;
    dir?: string | null;
    filters?: CompanyFilters;
  },
): Promise<CompanySearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const q = (opts.q ?? "").trim();
  const sortColumn = getSortColumn(country, opts.sort ?? null);
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";

  const conds: string[] = [];
  const params: Record<string, unknown> = {};
  if (q) {
    conds.push(`${country.nameColumn} ILIKE {pattern:String}`);
    params.pattern = `%${q}%`;
  }
  for (const column of country.columns) {
    if (!column.filterable) continue;
    const values = opts.filters?.[column.key];
    if (!values || values.length === 0) continue;
    // Column expr from registry; values bound as an Array(String) param.
    conds.push(`${column.expr} IN {f_${column.key}:Array(String)}`);
    params[`f_${column.key}`] = values;
  }
  const industryValues = opts.filters?.industry;
  if (industryValues?.length && country.industryFilterExpr) {
    conds.push(country.industryFilterExpr);
    params.f_industry = industryValues;
  }
  const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")}` : "";

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );
  const total = Number(countRows[0].total);

  // Clamp the requested page to the real page range (count runs first on purpose).
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(requestedPage, lastPage);

  const selectList = buildCompanySelectList(country);

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     ${where}
     ORDER BY coalesce(toString(${sortColumn.expr}), '') = '' ASC, ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  if (country.industryQuery) {
    const ids = rows.map((r) => r.__industry_key ?? "").filter((v) => v !== "");
    const industries = ids.length
      ? await chQuery<IndustryRow>(country.industryQuery, { ids })
      : [];
    const byId = new Map(industries.map((i) => [i.company_id, i]));
    for (const row of rows) {
      const hit = byId.get(row.__industry_key ?? "");
      row.industry_code = hit?.industry_code ?? null;
      row.industry_label = hit?.industry_label ?? null;
      delete row.__industry_key;
    }
  }

  return { rows, total, page, pageSize, sort: sortColumn.key, dir };
}

export interface FinancialYearRow {
  fiscal_year: string;
  currency: string;
  revenue_amount_original: number | null;
  revenue_amount_usd: number | null;
  net_result_amount_original: number | null;
  net_result_amount_usd: number | null;
  total_assets_amount_usd: number | null;
  equity_amount_usd: number | null;
  employees: number | null;
}

export interface ContactRow {
  contact_type: string;
  contact_value: string;
}

export interface DomainRow {
  domain: string;
  website_url: string | null;
  domain_source: string;
  confidence: number | null;
  is_primary: 0 | 1;
}

export interface CompanyDetail {
  company: CompanyListRow;
  record: Record<string, unknown>;
  financials: FinancialYearRow[];
  contacts: ContactRow[];
  domains: DomainRow[];
  statements: Record<string, unknown>[];
}

export async function getCompanyDetail(
  country: CountryConfig,
  id: string,
): Promise<CompanyDetail | null> {
  const selectList = buildCompanySelectList(country);

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     WHERE ${country.idColumn} = {id:String}
     LIMIT 1`,
    { id },
  );
  if (rows.length === 0) return null;
  const company = rows[0];

  // Kick off the record + section queries immediately — they only depend on
  // `id` — so they run concurrently with the industry round-trip below
  // instead of after it.
  const recordPromise = chQuery<Record<string, unknown>>(
    country.detail?.recordQuery ??
      `SELECT * FROM ${country.companiesTable} WHERE ${country.idColumn} = {id:String} LIMIT 1`,
    { id },
  );
  const sectionsPromise = Promise.all([
    country.detail?.financialsQuery
      ? chQuery<FinancialYearRow>(country.detail.financialsQuery, { id })
      : Promise.resolve([]),
    country.detail?.contactsQuery
      ? chQuery<ContactRow>(country.detail.contactsQuery, { id })
      : Promise.resolve([]),
    country.detail?.domainsQuery
      ? chQuery<DomainRow>(country.detail.domainsQuery, { id })
      : Promise.resolve([]),
  ]);
  const statementsPromise = country.detail?.statementsQuery
    ? chQuery<Record<string, unknown>>(country.detail.statementsQuery, { id })
    : Promise.resolve([]);
  // No-op guards close the unhandled-rejection window between promise
  // construction and the `await` below — the await still surfaces real errors.
  recordPromise.catch(() => {});
  sectionsPromise.catch(() => {});
  statementsPromise.catch(() => {});

  if (country.industryQuery) {
    const key = company.__industry_key ?? "";
    const industries = key
      ? await chQuery<IndustryRow>(country.industryQuery, { ids: [key] })
      : [];
    company.industry_code = industries[0]?.industry_code ?? null;
    company.industry_label = industries[0]?.industry_label ?? null;
    delete company.__industry_key;
  }

  const [records, [financials, contacts, domains], statements] = await Promise.all([
    recordPromise,
    sectionsPromise,
    statementsPromise,
  ]);

  return { company, record: records[0] ?? {}, financials, contacts, domains, statements };
}
