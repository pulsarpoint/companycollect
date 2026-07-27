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
  limit?: number;
  offset?: number;
}

export async function listSourceRecords(
  register: ProcurementRegister,
  query: SourceQuery = {},
): Promise<SourceRecords & { dateColumn: string | null; countryColumn: string | null }> {
  const table = assertKnownTable(register, query.table ?? register.notice_table);
  const columns = await columnsOf(table);
  const dateCol = dateColumn(columns);
  const countryCol = countryColumn(columns);

  const where: string[] = [];
  const params: Record<string, unknown> = {
    limit: Math.min(query.limit ?? 50, 200),
    offset: Math.max(query.offset ?? 0, 0),
  };
  if (countryCol && query.country) {
    where.push(`upper(${countryCol}) = upper({country:String})`);
    params.country = query.country;
  }
  if (dateCol && query.from) {
    where.push(`${dateCol} >= toDate({from:String})`);
    params.from = query.from;
  }
  if (dateCol && query.to) {
    where.push(`${dateCol} <= toDate({to:String})`);
    params.to = query.to;
  }
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const order = dateCol ? `ORDER BY ${dateCol} DESC` : "";

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

  return {
    columns,
    rows,
    total: Number(counted[0]?.total ?? 0),
    dateColumn: dateCol,
    countryColumn: countryCol,
  };
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
