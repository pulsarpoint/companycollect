import { chQuery } from "~/lib/clickhouse.server";
import {
  getSortColumn,
  PAGE_SIZES,
  type CountryConfig,
  type SortDir,
} from "~/lib/countries";

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

export async function searchCompanies(
  country: CountryConfig,
  opts: {
    q?: string;
    page?: number;
    pageSize?: number;
    sort?: string | null;
    dir?: string | null;
  },
): Promise<CompanySearchResult> {
  const pageSize = PAGE_SIZES.includes(opts.pageSize as 25 | 50 | 100)
    ? (opts.pageSize as number)
    : 50;
  const requestedPage = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const q = (opts.q ?? "").trim();
  const sortColumn = getSortColumn(country, opts.sort ?? null);
  const dir: SortDir = opts.dir === "desc" ? "desc" : "asc";

  const where = q ? `WHERE ${country.nameColumn} ILIKE {pattern:String}` : "";
  const params = q ? { pattern: `%${q}%` } : undefined;

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );
  const total = Number(countRows[0].total);

  // Clamp the requested page to the real page range (count runs first on purpose).
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(requestedPage, lastPage);

  const joinKeyExpr = country.industryJoinKeyExpr ?? country.idColumn;
  const selectList = [
    ...country.columns.map((c) => `${c.expr} AS ${c.key}`),
    `toUInt8(${country.activeExpr}) AS active`,
    ...(country.industryQuery ? [`toString(${joinKeyExpr}) AS __industry_key`] : []),
  ].join(",\n       ");

  const rows = await chQuery<CompanyListRow & { __industry_key?: string }>(
    `SELECT ${selectList}
     FROM ${country.companiesTable}
     ${where}
     ORDER BY ${sortColumn.expr} ${dir === "desc" ? "DESC" : "ASC"}, ${country.idColumn}
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
