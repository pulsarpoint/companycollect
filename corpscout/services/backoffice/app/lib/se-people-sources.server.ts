/**
 * `/admin/se/people`'s four tabs: server-side-paged reads over the three SE
 * person SOURCE views (`se_company_person_bolagsverket` / `_esef` /
 * `_wikidata` -- dagster_v3's `company_people/source_views.py`, migration
 * 000330/000331) and the resolved `se_company_person` table (migration
 * 000291). Mirrors se-company-info-lists.server.ts / se-company-geocoding-
 * list.server.ts's shape: explicit column aliasing, a dynamically built WHERE
 * shared by the row page and its own count(), LIMIT/OFFSET as named params.
 *
 * Unlike those two (which share one count() with a counts strip), each tab
 * here has no strip -- so `countSePeopleSourceRows` is a plain, separate
 * count() per tab, run alongside the page query for pagination only.
 *
 * The three source views already resolve their own FINAL/join internally
 * (see source_views.py's per-view docstrings: esef and wikidata read FINAL
 * UNDER the view; bolagsverket's underlying table is a non-versioned
 * MergeTree needing none) -- this module reads all three as plain tables, no
 * FINAL, no join. `se_company_person` is a live ReplacingMergeTree and IS
 * read FINAL here, same as se-company-person.server.ts's PERSON_SQL.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import type { SePeopleSourceFilters, SePeopleSourceTab } from "~/lib/se-people-sources";

function nonEmpty(value: string): string | null {
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

/** The two filters every tab offers: an exact `company_id` match and a
 * case-insensitive `ILIKE` contains on the tab's own name column. Mirrors
 * se-company-info-lists.server.ts's `buildInfoListFilter` -- a filter is
 * appended to WHERE only when present, never as a SQL no-op. */
function buildSourceWhere(
  filters: SePeopleSourceFilters,
  nameColumn: string,
): { where: string[]; params: Record<string, unknown> } {
  const where: string[] = [];
  const params: Record<string, unknown> = {};
  const companyId = nonEmpty(filters.companyId);
  if (companyId) {
    where.push("company_id = {companyId:String}");
    params.companyId = companyId;
  }
  const name = nonEmpty(filters.name);
  if (name) {
    where.push(`${nameColumn} ILIKE {name:String}`);
    params.name = `%${name}%`;
  }
  return { where, params };
}

function whereClause(where: string[]): string {
  return where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
}

function pageParams(page: number, pageSize: number): { limit: number; offset: number } {
  const limit = clampPageSize(pageSize);
  const clampedPage = clampPage(page);
  return { limit, offset: (clampedPage - 1) * limit };
}

async function listRows<Row>(
  selectSql: string,
  orderBy: string,
  where: string[],
  params: Record<string, unknown>,
  page: number,
  pageSize: number,
): Promise<Row[]> {
  const { limit, offset } = pageParams(page, pageSize);
  return chQuery<Row>(
    `${selectSql}
${whereClause(where)}
ORDER BY ${orderBy}
${PAGE_LIMIT_OFFSET_SQL}`,
    { ...params, limit, offset },
  );
}

async function countRows(
  fromSql: string,
  where: string[],
  params: Record<string, unknown>,
): Promise<number> {
  const [row] = await chQuery<{ total: string }>(
    `SELECT toString(count()) AS total
FROM ${fromSql}
${whereClause(where)}`,
    params,
  );
  return Number(row?.total ?? 0);
}

/* -------------------------------------------------------------------- */
/* Bolagsverket -- se_company_person_bolagsverket                        */
/* -------------------------------------------------------------------- */

export const BOLAGSVERKET_TABLE = "corpscout.se_company_person_bolagsverket";

export interface SePeopleBolagsverketRow {
  company_id: string;
  full_name: string;
  first_name: string;
  last_name: string;
  role_original: string;
  role_kind: string;
  signatory_kind: string;
  /** '' when the source's fiscal_year is the 0 sentinel -- see the shared
   * source_observations CTE in se-company-person.server.ts, which normalizes
   * the same column the same way for the person evidence panel. */
  fiscal_year: number | null;
}

export const BOLAGSVERKET_SELECT_SQL = `SELECT
  company_id AS company_id,
  full_name AS full_name,
  first_name AS first_name,
  last_name AS last_name,
  role_original AS role_original,
  role_kind AS role_kind,
  signatory_kind AS signatory_kind,
  if(fiscal_year > 0, toNullable(toUInt16(fiscal_year)), CAST(NULL, 'Nullable(UInt16)')) AS fiscal_year
FROM ${BOLAGSVERKET_TABLE}`;

export async function listSePeopleBolagsverketPage(
  filters: SePeopleSourceFilters,
  page: number,
  pageSize: number,
): Promise<SePeopleBolagsverketRow[]> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return listRows(
    BOLAGSVERKET_SELECT_SQL,
    "company_id ASC, full_name ASC",
    where,
    params,
    page,
    pageSize,
  );
}

export async function countSePeopleBolagsverketRows(
  filters: SePeopleSourceFilters,
): Promise<number> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return countRows(BOLAGSVERKET_TABLE, where, params);
}

/* -------------------------------------------------------------------- */
/* ESEF -- se_company_person_esef                                        */
/* -------------------------------------------------------------------- */

export const ESEF_TABLE = "corpscout.se_company_person_esef";

export interface SePeopleEsefRow {
  company_id: string;
  full_name: string;
  role: string;
  role_category: string;
  organization: string;
  status: string;
  effective_from: string | null;
  effective_to: string | null;
  confidence: number | null;
}

export const ESEF_SELECT_SQL = `SELECT
  company_id AS company_id,
  full_name AS full_name,
  role AS role,
  role_category AS role_category,
  organization AS organization,
  status AS status,
  effective_from AS effective_from,
  effective_to AS effective_to,
  confidence AS confidence
FROM ${ESEF_TABLE}`;

export async function listSePeopleEsefPage(
  filters: SePeopleSourceFilters,
  page: number,
  pageSize: number,
): Promise<SePeopleEsefRow[]> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return listRows(
    ESEF_SELECT_SQL,
    "company_id ASC, full_name ASC",
    where,
    params,
    page,
    pageSize,
  );
}

export async function countSePeopleEsefRows(
  filters: SePeopleSourceFilters,
): Promise<number> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return countRows(ESEF_TABLE, where, params);
}

/* -------------------------------------------------------------------- */
/* Wikidata -- se_company_person_wikidata                                */
/* -------------------------------------------------------------------- */

export const WIKIDATA_TABLE = "corpscout.se_company_person_wikidata";

export interface SePeopleWikidataRow {
  company_id: string;
  full_name: string;
  person_wikidata_id: string;
  role_property: string;
  start_date: string | null;
  end_date: string | null;
  birth_year: number | null;
  description: string | null;
}

export const WIKIDATA_SELECT_SQL = `SELECT
  company_id AS company_id,
  full_name AS full_name,
  person_wikidata_id AS person_wikidata_id,
  role_property AS role_property,
  start_date AS start_date,
  end_date AS end_date,
  birth_year AS birth_year,
  description AS description
FROM ${WIKIDATA_TABLE}`;

export async function listSePeopleWikidataPage(
  filters: SePeopleSourceFilters,
  page: number,
  pageSize: number,
): Promise<SePeopleWikidataRow[]> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return listRows(
    WIKIDATA_SELECT_SQL,
    "company_id ASC, full_name ASC",
    where,
    params,
    page,
    pageSize,
  );
}

export async function countSePeopleWikidataRows(
  filters: SePeopleSourceFilters,
): Promise<number> {
  const { where, params } = buildSourceWhere(filters, "full_name");
  return countRows(WIKIDATA_TABLE, where, params);
}

/* -------------------------------------------------------------------- */
/* Final -- se_company_person (migration 000291, ReplacingMergeTree)     */
/* -------------------------------------------------------------------- */

export const FINAL_PEOPLE_TABLE = "corpscout.se_company_person";

export interface SePeopleFinalRow {
  company_id: string;
  person_id: string;
  name: string;
  description: string | null;
  model_provider: string;
  model_name: string;
  updated_at: string;
}

/** FINAL: se_company_person is a live ReplacingMergeTree(updated_at), unlike
 * the three read-only source views above -- same reasoning as
 * se-company-person.server.ts's PERSON_SQL. Empty until the owner runs the
 * pipeline's clean-copy step; an empty result here is correct, not an
 * error -- no row ever fails this query, it just returns none. */
export const FINAL_SELECT_SQL = `SELECT
  company_id AS company_id,
  toString(person_id) AS person_id,
  name AS name,
  description AS description,
  toString(model_provider) AS model_provider,
  model_name AS model_name,
  toString(updated_at) AS updated_at
FROM ${FINAL_PEOPLE_TABLE} FINAL`;

export async function listSePeopleFinalPage(
  filters: SePeopleSourceFilters,
  page: number,
  pageSize: number,
): Promise<SePeopleFinalRow[]> {
  const { where, params } = buildSourceWhere(filters, "name");
  return listRows(
    FINAL_SELECT_SQL,
    "company_id ASC, person_id ASC",
    where,
    params,
    page,
    pageSize,
  );
}

export async function countSePeopleFinalRows(
  filters: SePeopleSourceFilters,
): Promise<number> {
  const { where, params } = buildSourceWhere(filters, "name");
  return countRows(`${FINAL_PEOPLE_TABLE} FINAL`, where, params);
}

/* -------------------------------------------------------------------- */
/* Dispatch: one call per tab, from the route loader                     */
/* -------------------------------------------------------------------- */

export type SePeopleSourcePage =
  | { tab: "bolagsverket"; rows: SePeopleBolagsverketRow[]; total: number }
  | { tab: "esef"; rows: SePeopleEsefRow[]; total: number }
  | { tab: "wikidata"; rows: SePeopleWikidataRow[]; total: number }
  | { tab: "final"; rows: SePeopleFinalRow[]; total: number };

/** The loader's one call: runs the active tab's page + count queries (two
 * scans of ONE table, never all four) and returns a tab-tagged result the
 * component can switch on. */
export async function loadSePeopleSourcePage(
  tab: SePeopleSourceTab,
  filters: SePeopleSourceFilters,
  page: number,
  pageSize: number,
): Promise<SePeopleSourcePage> {
  switch (tab) {
    case "bolagsverket": {
      const [rows, total] = await Promise.all([
        listSePeopleBolagsverketPage(filters, page, pageSize),
        countSePeopleBolagsverketRows(filters),
      ]);
      return { tab, rows, total };
    }
    case "esef": {
      const [rows, total] = await Promise.all([
        listSePeopleEsefPage(filters, page, pageSize),
        countSePeopleEsefRows(filters),
      ]);
      return { tab, rows, total };
    }
    case "wikidata": {
      const [rows, total] = await Promise.all([
        listSePeopleWikidataPage(filters, page, pageSize),
        countSePeopleWikidataRows(filters),
      ]);
      return { tab, rows, total };
    }
    case "final": {
      const [rows, total] = await Promise.all([
        listSePeopleFinalPage(filters, page, pageSize),
        countSePeopleFinalRows(filters),
      ]);
      return { tab, rows, total };
    }
  }
}
