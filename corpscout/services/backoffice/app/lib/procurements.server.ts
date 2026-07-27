/**
 * Per-source procurement pages.
 *
 * These read the SOURCE tables, not the country contract views, and that is the
 * whole point of them. A country view answers "what did this country buy"; it
 * filters winners to that country's own register, projects onto 26 canonical
 * columns, and drops whatever does not fit. A source page answers a different
 * question — "what does this register contain, and did we read it correctly" —
 * which is only answerable against the register's own shape.
 *
 * Two consequences follow, both deliberate:
 *
 * 1. **Not country-scoped.** A TED page lists every TED notice, including the
 *    Danish company that won in Sweden. Those rows appear in no country view at
 *    all, so this is the only place they are visible.
 * 2. **Not company-gated.** A record whose winner never matched a company still
 *    belongs here. Only the *link* to a company is conditional.
 *
 * Everything about a register — its tables, its key column, its licence — comes
 * from `procurement_registers` rather than a map in this file. That table is
 * the reason §3.3 exists.
 */
import { chQuery } from "./clickhouse.server";
import { sourceSlugToPath } from "./procurement-paths";

export interface ProcurementRegister {
  source_slug: string;
  register_name: string;
  operator: string;
  country_codes: string[];
  homepage_url: string;
  api_or_download_url: string;
  retrieval_method: string;
  documentation_url: string;
  licence: string;
  coverage_description: string;
  open_tenders_url: string;
  grain_description: string;
  source_tables: string[];
  notice_table: string;
  notice_key_column: string;
  notes: string;
}

const REGISTER_COLUMNS = `
  source_slug, register_name, operator, country_codes, homepage_url,
  api_or_download_url, retrieval_method, documentation_url, licence,
  coverage_description, open_tenders_url, grain_description, source_tables,
  notice_table, notice_key_column, notes`;

export async function listRegisters(): Promise<ProcurementRegister[]> {
  return chQuery<ProcurementRegister>(
    `SELECT ${REGISTER_COLUMNS} FROM procurement_registers FINAL
     ORDER BY register_name`,
  );
}

export async function getRegisterByPath(
  path: string,
): Promise<ProcurementRegister | null> {
  const all = await listRegisters();
  return all.find((r) => sourceSlugToPath(r.source_slug) === path) ?? null;
}

/** Coverage is per (country, source) and lives in company_signal_coverage, so a
 * source serving three countries has three rows. Shown as-is rather than
 * merged: "Norway from 2024" and "Sweden from 2021" are both true and merging
 * them would state neither. */
export interface SourceCoverage {
  country_code: string;
  coverage_from: string | null;
  coverage_to: string | null;
  caveat: string;
}

export async function getCoverage(
  register: ProcurementRegister,
): Promise<SourceCoverage[]> {
  return chQuery<SourceCoverage>(
    `SELECT country_code,
            toString(coverage_from) AS coverage_from,
            toString(coverage_to) AS coverage_to,
            caveat
     FROM company_signal_coverage
     WHERE has(source_slugs, {slug:String})
     ORDER BY country_code`,
    { slug: register.source_slug },
  );
}

/** A row is whatever the source publishes, so columns are discovered rather
 * than declared. Anything else would be the canonical projection again. */
export type SourceRow = Record<string, unknown>;

export interface SourceRecords {
  columns: string[];
  rows: SourceRow[];
  total: number;
}

/** The tables are named in `procurement_registers`, which is trusted config,
 * but they are interpolated into SQL — so they are checked against the register
 * before use rather than taken on faith from a URL. */
function assertKnownTable(register: ProcurementRegister, table: string): string {
  if (!register.source_tables.includes(table)) {
    throw new Response(`Unknown table for ${register.source_slug}`, {
      status: 400,
    });
  }
  if (!/^[a-z0-9_]+$/.test(table)) {
    throw new Response("Invalid table name", { status: 400 });
  }
  return table;
}

async function columnsOf(table: string): Promise<string[]> {
  const rows = await chQuery<{ name: string }>(
    `SELECT name FROM system.columns
     WHERE database = currentDatabase() AND table = {table:String}
     ORDER BY position`,
    { table },
  );
  return rows.map((r) => r.name);
}

/** The column a source's rows are ordered and filtered by. Discovered from the
 * table rather than declared, because every one of these registers happens to
 * publish a publication or issue date but not under the same name. */
function dateColumn(columns: string[]): string | null {
  for (const candidate of [
    "publication_date",
    "issue_date",
    "data_publicacao_pncp",
  ]) {
    if (columns.includes(candidate)) return candidate;
  }
  return null;
}

function countryColumn(columns: string[]): string | null {
  for (const candidate of ["country_iso2", "country_code"]) {
    if (columns.includes(candidate)) return candidate;
  }
  return null;
}

export interface SourceQuery {
  table?: string;
  country?: string;
  from?: string;
  to?: string;
  buyer?: string;
  winner?: string;
  noticeType?: string;
  awardResult?: string;
  valueMin?: number;
  valueMax?: number;
  limit?: number;
  offset?: number;
}

export interface FilterColumns {
  date: string | null;
  country: string | null;
  buyerName: string | null;
  winnerName: string | null;
  winnerId: string | null;
  noticeType: string | null;
  awardResult: string | null;
  usdValue: string | null;
}

function firstPresent(columns: string[], candidates: string[]): string | null {
  for (const candidate of candidates) {
    if (columns.includes(candidate)) return candidate;
  }
  return null;
}

/** Column discovery per register table. Candidate lists, like dateColumn's,
 * because the registers publish the same concepts under different names. */
export function filterColumns(columns: string[]): FilterColumns {
  return {
    date: dateColumn(columns),
    country: countryColumn(columns),
    buyerName: firstPresent(columns, ["buyer_name", "buyer_name_fi", "buyer_unit_name"]),
    winnerName: firstPresent(columns, ["winner_name", "supplier_name"]),
    winnerId: firstPresent(columns, [
      "winner_org_number",
      "winner_national_id",
      "winner_business_id",
      "supplier_id_normalized",
      "supplier_cnpj",
    ]),
    noticeType: firstPresent(columns, ["notice_type", "procedure_type"]),
    awardResult: firstPresent(columns, ["award_result"]),
    usdValue: firstPresent(columns, [
      "value_amount_usd",
      "awarded_amount_usd",
      "total_value_amount_usd",
      "procurement_value_amount_usd",
      "valor_global_usd",
    ]),
  };
}

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

export function buildSourceFilter(
  columns: string[],
  query: SourceQuery,
): { where: string[]; params: Record<string, unknown> } {
  const cols = filterColumns(columns);
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  const country = nonEmpty(query.country);
  if (cols.country && country) {
    where.push(`upper(${cols.country}) = upper({country:String})`);
    params.country = country;
  }
  const from = nonEmpty(query.from);
  if (cols.date && from) {
    where.push(`${cols.date} >= toDate({from:String})`);
    params.from = from;
  }
  const to = nonEmpty(query.to);
  if (cols.date && to) {
    where.push(`${cols.date} <= toDate({to:String})`);
    params.to = to;
  }
  const buyer = nonEmpty(query.buyer);
  if (cols.buyerName && buyer) {
    where.push(`positionCaseInsensitiveUTF8(${cols.buyerName}, {buyer:String}) > 0`);
    params.buyer = buyer;
  }
  const winner = nonEmpty(query.winner);
  if ((cols.winnerName || cols.winnerId) && winner) {
    const parts: string[] = [];
    if (cols.winnerName) {
      parts.push(`positionCaseInsensitiveUTF8(${cols.winnerName}, {winner:String}) > 0`);
    }
    if (cols.winnerId) parts.push(`${cols.winnerId} = {winner:String}`);
    where.push(parts.length > 1 ? `(${parts.join(" OR ")})` : parts[0]);
    params.winner = winner;
  }
  const noticeType = nonEmpty(query.noticeType);
  if (cols.noticeType && noticeType) {
    where.push(`${cols.noticeType} = {noticeType:String}`);
    params.noticeType = noticeType;
  }
  const awardResult = nonEmpty(query.awardResult);
  if (cols.awardResult && awardResult) {
    where.push(`${cols.awardResult} = {awardResult:String}`);
    params.awardResult = awardResult;
  }
  if (cols.usdValue && query.valueMin != null && Number.isFinite(query.valueMin)) {
    where.push(`${cols.usdValue} >= {valueMin:Float64}`);
    params.valueMin = query.valueMin;
  }
  if (cols.usdValue && query.valueMax != null && Number.isFinite(query.valueMax)) {
    where.push(`${cols.usdValue} <= {valueMax:Float64}`);
    params.valueMax = query.valueMax;
  }
  return { where, params };
}

export async function listSourceRecords(
  register: ProcurementRegister,
  query: SourceQuery = {},
): Promise<SourceRecords & { filters: FilterColumns }> {
  const table = assertKnownTable(register, query.table ?? register.notice_table);
  const columns = await columnsOf(table);
  const cols = filterColumns(columns);

  const { where, params } = buildSourceFilter(columns, query);
  params.limit = Math.min(query.limit ?? 50, 200);
  params.offset = Math.max(query.offset ?? 0, 0);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const order = cols.date ? `ORDER BY ${cols.date} DESC` : "";

  const [rows, counted] = await Promise.all([
    chQuery<SourceRow>(
      `SELECT * FROM ${table} ${filter} ${order}
       LIMIT {limit:UInt32} OFFSET {offset:UInt32}`,
      params,
    ),
    chQuery<{ total: string }>(
      `SELECT toString(count()) AS total FROM ${table} ${filter}`,
      params,
    ),
  ]);

  return { columns, rows, total: Number(counted[0]?.total ?? 0), filters: cols };
}

/** Every field the source publishes for one record, plus the other tables that
 * describe it. TED's lots and winners live beside its notices, and a page that
 * showed only the notice would be the canonical projection wearing a different
 * hat. */
export interface SourceRecordDetail {
  key: string;
  primary: { table: string; row: SourceRow } | null;
  related: { table: string; rows: SourceRow[] }[];
}

export async function getSourceRecord(
  register: ProcurementRegister,
  key: string,
): Promise<SourceRecordDetail | null> {
  const keyColumn = register.notice_key_column;
  if (!/^[a-z0-9_]+$/.test(keyColumn)) return null;

  const primaryTable = assertKnownTable(register, register.notice_table);
  const primaryRows = await chQuery<SourceRow>(
    `SELECT * FROM ${primaryTable} WHERE ${keyColumn} = {key:String} LIMIT 1`,
    { key },
  );
  if (primaryRows.length === 0) return null;

  const related: { table: string; rows: SourceRow[] }[] = [];
  for (const table of register.source_tables) {
    if (table === primaryTable) continue;
    assertKnownTable(register, table);
    const columns = await columnsOf(table);
    // Only tables that carry the same key describe the same record.
    if (!columns.includes(keyColumn)) continue;
    const rows = await chQuery<SourceRow>(
      `SELECT * FROM ${table} WHERE ${keyColumn} = {key:String} LIMIT 200`,
      { key },
    );
    if (rows.length > 0) related.push({ table, rows });
  }

  return { key, primary: { table: primaryTable, row: primaryRows[0] }, related };
}

/** Row counts per table, for the index and the source header. Cheap: these are
 * MergeTree counts, not scans. */
export async function countRows(tables: string[]): Promise<Record<string, number>> {
  const safe = tables.filter((t) => /^[a-z0-9_]+$/.test(t));
  if (safe.length === 0) return {};
  const rows = await chQuery<{ table: string; rows: string }>(
    `SELECT table, toString(sum(rows)) AS rows
     FROM system.parts
     WHERE database = currentDatabase() AND active AND table IN {tables:Array(String)}
     GROUP BY table`,
    { tables: safe },
  );
  return Object.fromEntries(rows.map((r) => [r.table, Number(r.rows)]));
}

/** Distinct values for the sheet's enum dropdowns plus which countries have
 * rows. One grouped query per column, bounded: these are LowCardinality
 * columns with a handful of values. */
export async function getFilterOptions(
  register: ProcurementRegister,
  table?: string,
): Promise<{ noticeTypes: string[]; awardResults: string[]; activeCountries: string[] }> {
  const safeTable = assertKnownTable(register, table ?? register.notice_table);
  const cols = filterColumns(await columnsOf(safeTable));

  async function distinct(column: string | null): Promise<string[]> {
    if (!column) return [];
    const rows = await chQuery<{ v: string }>(
      `SELECT DISTINCT ${column} AS v FROM ${safeTable}
       WHERE ${column} != '' ORDER BY v LIMIT 100`,
    );
    return rows.map((r) => r.v);
  }

  const [noticeTypes, awardResults, activeCountries] = await Promise.all([
    distinct(cols.noticeType),
    distinct(cols.awardResult),
    distinct(cols.country).then((codes) => codes.map((c) => c.toUpperCase())),
  ]);
  return { noticeTypes, awardResults, activeCountries };
}

/** Which of these org ids exist in the company register, and every register
 * they exist in. Buyers are mostly public institutions, but their org
 * numbers are in the national registers (SE ~98%, NO ~95% measured), so the
 * company page doubles as the buyer page.
 *
 * National org-number formats collide across countries — the same digits
 * are a valid id in more than one register (a Czech ICO equalling a
 * Brazilian id was observed live) — so this returns every hit per id rather
 * than picking one with `any(country_code)`. Which hit, if any, a cell links
 * to is decided by `pickCompanyMatch` in `~/lib/company-match.ts`, using the
 * row's own country when it has one. */
export async function matchCompanies(
  ids: string[],
): Promise<Record<string, { country_code: string; company_id: string }[]>> {
  const unique = [...new Set(ids.filter((id) => id !== ""))];
  if (unique.length === 0) return {};
  const rows = await chQuery<{ company_id: string; country_code: string }>(
    `SELECT DISTINCT company_id, country_code
     FROM companies_all
     WHERE company_id IN {ids:Array(String)}`,
    { ids: unique },
  );
  const matches: Record<string, { country_code: string; company_id: string }[]> = {};
  for (const row of rows) {
    (matches[row.company_id] ??= []).push(row);
  }
  return matches;
}
