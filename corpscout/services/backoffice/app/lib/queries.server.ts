import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

export interface CountryStats {
  total: number;
  active: number;
}

export interface CompanyRow {
  id: string;
  name: string;
  active: 0 | 1;
}

export interface CompanySearchResult {
  rows: CompanyRow[];
  total: number;
  page: number;
  pageSize: number;
}

const MAX_PAGE_SIZE = 100;

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

export async function searchCompanies(
  country: CountryConfig,
  opts: { q?: string; page?: number; pageSize?: number },
): Promise<CompanySearchResult> {
  const page = clampInt(opts.page, 1, Number.MAX_SAFE_INTEGER, 1);
  const pageSize = clampInt(opts.pageSize, 1, MAX_PAGE_SIZE, 50);
  const q = (opts.q ?? "").trim();

  const where = q ? `WHERE ${country.nameColumn} ILIKE {pattern:String}` : "";
  const params = q ? { pattern: `%${q}%` } : undefined;

  const countRows = await chQuery<{ total: string }>(
    `SELECT count() AS total FROM ${country.companiesTable} ${where}`,
    params,
  );

  const rows = await chQuery<CompanyRow>(
    `SELECT
       ${country.idColumn} AS id,
       ${country.nameColumn} AS name,
       toUInt8(${country.activeExpr}) AS active
     FROM ${country.companiesTable}
     ${where}
     ORDER BY ${country.nameColumn}, ${country.idColumn}
     LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`,
    params,
  );

  return { rows, total: Number(countRows[0].total), page, pageSize };
}
