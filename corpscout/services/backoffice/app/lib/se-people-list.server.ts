/**
 * The `/admin/se/people` list (person spec section 7): one row per published person of
 * `se_company_person`, read through FINAL, filtered, counted and paged server-side.
 * No writes and no fold -- the company's People tab owns every decision.
 *
 * The company NAME is not on the person table (there is no cross-company person
 * identity to hang it on), so it arrives in two reads instead of a join: a name filter
 * resolves to company ids through the serving view first, and the page's own rows are
 * named afterwards, at most 50 ids at a time. A join would make ClickHouse read the
 * 3.5M-row view for every page.
 */
import { chQuery } from "~/lib/clickhouse.server";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  PAGE_LIMIT_OFFSET_SQL,
  SE_COMPANIES_SERVING_TABLE,
} from "~/lib/se-company-info-lists.server";
import { SE_COMPANY_PERSON_TABLE } from "~/lib/se-person-tables";
import type { SePeopleFilters } from "~/lib/se-people-filters";

/** How many company ids a company-name filter may carry into the page/counts
 * predicates -- beyond this the reviewer sees a "first 200 matching companies" note
 * (see `resolveSePeopleCompanyIds`'s `truncated`). */
export const COMPANY_MATCH_LIMIT = 200;

export const PEOPLE_LIST_SELECT_SQL = `SELECT
  p.company_id AS company_id, toString(p.person_key) AS person_key,
  p.display_name AS display_name, ifNull(toString(p.birth_year), '') AS birth_year,
  ifNull(p.wikidata_id, '') AS wikidata_id, arrayMap(x -> toString(x), p.sources) AS sources,
  p.current_roles AS current_roles, p.role_years AS role_years,
  ifNull(toString(p.first_year), '') AS first_year, ifNull(toString(p.last_year), '') AS last_year,
  toUInt8(p.active) AS active, toString(p.inactive_reason) AS inactive_reason
FROM ${SE_COMPANY_PERSON_TABLE} AS p FINAL`;

export const PEOPLE_COUNTS_SQL = `SELECT
  toString(count()) AS persons,
  toString(countIf(p.active = 1)) AS active,
  toString(uniqExact(p.company_id)) AS companies
FROM ${SE_COMPANY_PERSON_TABLE} AS p FINAL`;

export const PEOPLE_COMPANY_NAMES_SQL = `SELECT company_id, legal_name
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE company_id IN {companyIds:Array(String)}`;

export const PEOPLE_COMPANY_SEARCH_SQL = `SELECT company_id
FROM ${SE_COMPANIES_SERVING_TABLE}
WHERE legal_name ILIKE {name:String}
ORDER BY legal_name
LIMIT {limit:UInt32}`;

/** A raw hit of the company-name search, before it is reduced to a bare id list. */
export interface SePeopleCompanyMatch {
  company_id: string;
}

/** One row of `PEOPLE_LIST_SELECT_SQL`, before the page's own company names are
 * attached (see `SePeopleListRow`). */
type SePeopleRawRow = {
  company_id: string;
  person_key: string;
  display_name: string;
  birth_year: string;
  wikidata_id: string;
  sources: string[];
  current_roles: string[];
  role_years: number[];
  first_year: string;
  last_year: string;
  active: number;
  inactive_reason: string;
};

/** The list's own row: the raw person row plus the company's `legal_name`, attached
 * from `PEOPLE_COMPANY_NAMES_SQL` -- `""` when the serving view has no row for that
 * company (a company can be published in the person entity, but not yet on the
 * serving view, or vice versa; the reader must never see `undefined`). */
export interface SePeopleListRow extends SePeopleRawRow {
  legal_name: string;
}

export interface SePeopleCounts {
  persons: number;
  active: number;
  companies: number;
}

/* -------------------------------------------------------------------- */
/* Resolving the company-name filter -- once, in the route's loader      */
/* -------------------------------------------------------------------- */

const ALL_DIGITS = /^[0-9]+$/;

/**
 * `resolveSePeopleCompanyIds(company)` runs FIRST and ONCE, in the route's loader:
 * `""` gives `{ companyIds: null, truncated: false }` (no company predicate at all,
 * so `buildSePeopleFilter` adds nothing), an all-digits company gives
 * `{ companyIds: [company] }` without any query, and anything else is a name prefix
 * read through `PEOPLE_COMPANY_SEARCH_SQL` (`name: "<company>%"`,
 * `limit: COMPANY_MATCH_LIMIT`) with `truncated` set when the read comes back full.
 *
 * The ids then go to BOTH readers (`listSePeoplePage` and `loadSePeopleCounts`),
 * which is what keeps the counts strip and the pager total honest under a name
 * filter (pre-flight review 3.3): without it the strip reports the whole
 * 1,126,402 / 578,289 over a page of one company, and the pager offers 22,528 empty
 * pages.
 */
export async function resolveSePeopleCompanyIds(
  company: string,
): Promise<{ companyIds: string[] | null; truncated: boolean }> {
  const trimmed = company.trim();
  if (trimmed === "") return { companyIds: null, truncated: false };
  if (ALL_DIGITS.test(trimmed)) return { companyIds: [trimmed], truncated: false };
  const rows = await chQuery<SePeopleCompanyMatch>(PEOPLE_COMPANY_SEARCH_SQL, {
    name: `${trimmed}%`,
    limit: COMPANY_MATCH_LIMIT,
  });
  return {
    companyIds: rows.map((row) => row.company_id),
    truncated: rows.length >= COMPANY_MATCH_LIMIT,
  };
}

/* -------------------------------------------------------------------- */
/* The WHERE clause both readers share                                   */
/* -------------------------------------------------------------------- */

export interface SePeopleFilterSql {
  where: string[];
  params: Record<string, unknown>;
}

/**
 * `buildSePeopleFilter(filters, companyIds)`: no company predicate when `companyIds`
 * is `null` (the loader resolved no company filter at all), `p.company_id IN
 * {companyIds:Array(String)}` otherwise -- ALWAYS an id set by the time it reaches
 * SQL, never the raw `filters.company` text -- plus one predicate per remaining
 * filter. `role` filters on `role_codes` (every role the person ever held, per spec
 * 7), not `current_roles` (the currently-held subset the row displays).
 */
export function buildSePeopleFilter(
  filters: SePeopleFilters,
  companyIds: string[] | null = null,
): SePeopleFilterSql {
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  if (companyIds !== null) {
    where.push("p.company_id IN {companyIds:Array(String)}");
    params.companyIds = companyIds;
  }
  const name = filters.name.trim();
  if (name !== "") {
    where.push("p.display_name ILIKE {name:String}");
    params.name = `${name}%`;
  }
  const source = filters.source.trim();
  if (source !== "") {
    where.push("has(p.sources, {source:String})");
    params.source = source;
  }
  const role = filters.role.trim();
  if (role !== "") {
    where.push("has(p.role_codes, {role:String})");
    params.role = role;
  }
  const year = filters.year.trim();
  if (year !== "") {
    where.push("has(p.role_years, {year:UInt16})");
    params.year = Number(year);
  }
  if (filters.status === "active") {
    where.push("p.active = 1");
  } else if (filters.status === "hidden") {
    where.push("p.inactive_reason = 'hidden'");
  } else if (filters.status === "withdrawn") {
    where.push("p.inactive_reason = 'withdrawn'");
  }
  return { where, params };
}

function whereSql(where: readonly string[]): string {
  return where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
}

/* -------------------------------------------------------------------- */
/* The page                                                              */
/* -------------------------------------------------------------------- */

export interface SePeopleListQuery extends SePeopleFilters {
  companyIds: string[] | null;
  page: number;
  pageSize: number;
}

export interface SePeopleListPage {
  rows: SePeopleListRow[];
}

/**
 * Reads the page (`PEOPLE_LIST_SELECT_SQL` + the shared WHERE, sorted by company then
 * display name, `clampPage`/`clampPageSize` paged), then names the page's own
 * distinct company ids in ONE `PEOPLE_COMPANY_NAMES_SQL` read and attaches
 * `legal_name` (`""` when the view has no row) -- never a join, and never one lookup
 * per row.
 */
export async function listSePeoplePage(query: SePeopleListQuery): Promise<SePeopleListPage> {
  const { where, params } = buildSePeopleFilter(query, query.companyIds);
  const limit = clampPageSize(query.pageSize);
  const page = clampPage(query.page);
  const offset = (page - 1) * limit;
  const sql = `${PEOPLE_LIST_SELECT_SQL}
${whereSql(where)}
ORDER BY p.company_id, p.display_name
${PAGE_LIMIT_OFFSET_SQL}`;
  const rawRows = await chQuery<SePeopleRawRow>(sql, { ...params, limit, offset });

  const companyIds = [...new Set(rawRows.map((row) => row.company_id))];
  const names =
    companyIds.length > 0
      ? await chQuery<{ company_id: string; legal_name: string }>(PEOPLE_COMPANY_NAMES_SQL, {
          companyIds,
        })
      : [];
  const nameByCompanyId = new Map(names.map((row) => [row.company_id, row.legal_name]));

  return {
    rows: rawRows.map((row) => ({
      ...row,
      legal_name: nameByCompanyId.get(row.company_id) ?? "",
    })),
  };
}

/* -------------------------------------------------------------------- */
/* The counts strip                                                      */
/* -------------------------------------------------------------------- */

export interface SePeopleCountsQuery extends SePeopleFilters {
  companyIds: string[] | null;
}

/**
 * `PEOPLE_COUNTS_SQL` under the SAME `where`/params `listSePeoplePage` built (never a
 * second, differently-filtered scan) -- `persons` is also the pager's total.
 */
export async function loadSePeopleCounts(query: SePeopleCountsQuery): Promise<SePeopleCounts> {
  const { where, params } = buildSePeopleFilter(query, query.companyIds);
  const sql = `${PEOPLE_COUNTS_SQL}
${whereSql(where)}`;
  const [row] = await chQuery<{ persons: string; active: string; companies: string }>(sql, params);
  return {
    persons: Number(row?.persons ?? 0),
    active: Number(row?.active ?? 0),
    companies: Number(row?.companies ?? 0),
  };
}
