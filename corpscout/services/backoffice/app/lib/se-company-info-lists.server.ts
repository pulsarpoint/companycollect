/**
 * The two `/admin/se/company-info*` list pages: every published
 * `se_company_info` row (3.5M, ReplacingMergeTree ORDER BY company_id) and
 * the full `se_company_info_correction` ledger. Both mirror
 * se-company-info.server.ts's client/query style -- explicit column
 * aliasing, hash/UUID columns wrapped in toString() -- but add server-side
 * paging and optional URL-driven filters, so the WHERE clause is built
 * dynamically (like procurements.server.ts's buildSourceFilter) instead of
 * being a fixed string.
 */
import { chQuery } from "~/lib/clickhouse.server";
import type { LegalFormLabels } from "~/lib/se-legal-form";
import type { SortDir } from "~/lib/countries";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  SE_INFO_CORRECTION_KINDS,
  SE_INFO_CORRECTION_STATUSES,
  type SeInfoCorrectionStatus,
} from "~/lib/se-info-corrections";
import {
  ANY_FILTER_VALUE,
  NONE_FILTER_VALUE,
  PROFILE_DATATYPES,
  PROFILE_SOURCES,
  type ProfileDatatypeKey,
  type ProfileSourceValue,
} from "~/lib/se-company-info-filters";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export type { SeInfoCorrectionStatus };

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

/**
 * A data-driven discrete filter's value: absent when unset or when the select
 * says "Any", and the empty string when it says "none" (a company with no
 * legal form code or no status recorded). Values reach SQL only as named
 * params, never as text -- the option lists these come from are read from the
 * column itself (see loadSeCompanyInfoFilterOptions), so there is no fixed enum
 * to whitelist them against the way the ledger's `kind` has.
 */
function discreteValue(value: string | undefined): string | null {
  const trimmed = nonEmpty(value);
  if (trimmed === null || trimmed === ANY_FILTER_VALUE) return null;
  return trimmed === NONE_FILTER_VALUE ? "" : trimmed;
}

/* -------------------------------------------------------------------- */
/* Server-side sorting                                                   */
/* -------------------------------------------------------------------- */

interface SortTerm {
  expr: string;
  dir: SortDir;
}

/**
 * ORDER BY for one whitelisted column plus the list's own stable tiebreak,
 * with any tiebreak that IS the sorted column dropped (so the default sort
 * still reads `ORDER BY i.company_id ASC`, not the same column twice).
 *
 * Cost note: se_company_info is a 3.5M-row ReplacingMergeTree ordered by
 * company_id, so sorting a page by any other column is a top-N sort over the
 * FINAL-merged result (`ORDER BY ... LIMIT/OFFSET`), which ClickHouse does in
 * one pass with a bounded heap -- fine for an admin list. A DEEP page on such
 * a sort (offset in the hundreds of thousands) degenerates into a full sort;
 * that is accepted here rather than restricting sorting to the key.
 */
function orderBySql(primary: SortTerm, tiebreaks: readonly SortTerm[]): string {
  const terms = [
    primary,
    ...tiebreaks.filter((term) => term.expr !== primary.expr),
  ];
  return `ORDER BY ${terms
    .map((term) => `${term.expr} ${term.dir === "desc" ? "DESC" : "ASC"}`)
    .join(", ")}`;
}

function resolveDir(dir: string | undefined, fallback: SortDir): SortDir {
  return dir === "asc" || dir === "desc" ? dir : fallback;
}

function resolveSortKey<K extends string>(
  columns: Record<K, string>,
  sort: string | undefined,
  fallback: K,
): K {
  return sort !== undefined && Object.hasOwn(columns, sort) ? (sort as K) : fallback;
}

function pageParams(query: { page: number; pageSize: number }): {
  limit: number;
  offset: number;
} {
  const limit = clampPageSize(query.pageSize);
  const page = clampPage(query.page);
  return { limit, offset: (page - 1) * limit };
}

/** Every server-side page reads/pages this way: LIMIT/OFFSET as named
 * UInt32 params, never interpolated. */
export const PAGE_LIMIT_OFFSET_SQL = "LIMIT {limit:UInt32} OFFSET {offset:UInt32}";

/* -------------------------------------------------------------------- */
/* Page 1: /admin/se/company-info -- the se_company_info table          */
/* -------------------------------------------------------------------- */

/**
 * One company, as this list shows it. Task 17 (owner addendum 2026-08-23):
 * /admin/se/company-info is a COMPANIES list, not a description view -- so it
 * carries the register's own columns plus exactly one description fact, whether
 * the company has a published description at all. Everything about WHERE that
 * description came from, who reviewed it and whether the model wrote it lives
 * on the detail page, which has room to say it properly.
 */
export interface SeCompanyInfoListRow {
  company_id: string;
  legal_name: string;
  status: string;
  legal_form_code: string;
  /**
   * What that code is called, both languages, copied onto the row by Dagster
   * from the curated corpscout.se_code_labels dictionary (migration 000306).
   * '' when the dictionary does not name the code -- the column shows the code
   * itself then, never a blank cell.
   */
  legal_form_label_en: string;
  legal_form_label_sv: string;
  /** "legal" (10-digit org number) | "sole" (12-digit personnummer-based id). */
  entity_type: string;
  /** 0 | 1 -- `description IS NOT NULL`, projected as UInt8 (see the SELECT). */
  has_description: number;
  /** 0 | 1 -- has a live row in corpscout.se_company_address. */
  has_address: number;
  /** 0 | 1 -- has annual accounts from Bolagsverket or ESEF. */
  has_financial: number;
  /** 0 | 1 -- has a published row in corpscout.se_company_person. */
  has_people: number;
  /** 0 | 1 -- has a Swedish row in the unified corpscout.company_domains. */
  has_domains: number;
  /**
   * Which registers built this profile, as the catalog's letters in canonical
   * order: 'B' (Bolagsverket), 'S' (SCB, always), 'E' (ESEF), 'W' (Wikidata).
   * The UNION across all five datatypes, not the description's provenance:
   * derived in SQL (PROFILE_SOURCES_EXPR) rather than assembled here, so the
   * same string is what the column shows AND what the header sorts by.
   */
  profile_sources: string;
}

export interface SeCompanyInfoListFilters {
  companyId?: string;
  name?: string;
  status?: string;
  legalForm?: string;
  /** "legal" | "sole" -- anything else is absent. Plain `string` so the
   * routes' parsed, already-validated filter object is assignable as-is
   * instead of being re-spelled field by field. */
  entity?: string;
  /** "yes" | "no" -- anything else (including the select's "any") is absent. */
  description?: string;
  /** A key of PROFILE_SOURCE_PREDICATES ("bolagsverket" | "scb" | "esef" |
   * "wikidata") -- anything else (a stale value, the select's "any") is
   * absent. Plain `string` for the same reason `entity` is. It asks "has this
   * register in ANY datatype", which is exactly what the column's letters say.
   */
  source?: string;
}

export interface SeCompanyInfoListQuery extends SeCompanyInfoListFilters {
  page: number;
  pageSize: number;
  sort?: string;
  dir?: string;
}

/* -------------------------------------------------------------------- */
/* The Sources column: which registers built this company's profile      */
/* -------------------------------------------------------------------- */

/**
 * One SET of company ids per datatype-and-register question this page asks,
 * each read from that datatype's OWN final/serving table and each spelled
 * exactly once. Everything downstream -- the four presence columns, the four
 * source predicates, the Sources letters, the `?source=` filter -- is built by
 * naming a member of this record, so a table or a flag can only be wrong here.
 *
 * They are used as `company_id IN (<set>)` (see `hasCompanyIn`) rather than as
 * LEFT JOINs: ClickHouse builds each distinct subquery into one hash set once
 * per query and probes it per row, while a LEFT JOIN onto the 3.5M-row
 * `se_company_info ... FINAL` on the left is the very shape that made
 * PEOPLE_ROLES_SQL fail with NOT_FOUND_COLUMN_IN_BLOCK (see
 * se-company-people.server.ts). Two members with the SAME text are one set at
 * runtime, which is why the ESEF and Bolagsverket financial arms are reused
 * verbatim by `has_financial` instead of being spelled a second way.
 *
 * Which table is authoritative for which register, verified read-only against
 * the pipelines that write them (2026-08-24):
 *
 * - address: `se_company_address.sources` is written by se_company/address.py,
 *   whose ARTIFACT_TABLES map is exactly {bolagsverket, scb} -- so 'bolagsverket'
 *   in that array is the register itself, not a guess. FINAL is not optional:
 *   `is_current` is flipped in place by a later part when an address is
 *   tombstoned, so a pre-merge part would report a removed address as live.
 * - financial (Bolagsverket): se_bolagsverket_financial_metrics is the table
 *   `company_financials_latest/sql.py` builds SE's se_company_financials_latest
 *   FROM (`SOURCES["se"]["table"]`), and the one se_financials_bolagsverket_current
 *   -- the view the Financial tab renders -- aggregates. Reading the metrics
 *   directly is self-evidently Bolagsverket's; the serving table has no source
 *   column at all. Live check: both name the same 577,645 companies.
 * - financial (ESEF): esef_financial_metrics is keyed by LEI, so it reaches a
 *   company through corpscout.company_identifier exactly as
 *   se_financials_esef_current's own INNER JOIN does (issuer_scheme 'lei',
 *   country SE, is_current). Live check: this predicate and the view both name
 *   404 companies.
 * - people: se_company_person is the published row (no is_current, no
 *   tombstone -- a row is never withdrawn, so no FINAL is needed to answer
 *   "does this company have one"), and se_company_person_role.sources is the
 *   people datatype's own provenance, written by company_people/roles.py as
 *   `groupUniqArray(roles.source)` over drafts whose `source` literal is
 *   'bolagsverket' or 'esef' (company_people/draft.py). The DRAFT layer carries
 *   the same registers at much larger scale, but it is evidence, not a final,
 *   and adds nothing here: live check 2026-08-24 found exactly 1 company whose
 *   Bolagsverket people are not already Bolagsverket by address or accounts,
 *   and 0 for ESEF and Wikidata.
 * - domains: company_domains is the UNIFIED register, so it is filtered to
 *   'SE' here exactly as se-company-domains.server.ts does. Its own
 *   source_names ('common_crawl_identity', 'wikidata', 'esef_filing') are NOT
 *   folded into the letters -- see PROFILE_SOURCE_PREDICATES.
 */
export const COMPANY_SETS = {
  address: `SELECT company_id FROM corpscout.se_company_address FINAL WHERE is_current`,
  addressBolagsverket: `SELECT company_id FROM corpscout.se_company_address FINAL WHERE is_current AND has(sources, 'bolagsverket')`,
  financialBolagsverket: `SELECT company_id FROM corpscout.se_bolagsverket_financial_metrics`,
  financialEsef: `SELECT ci.company_id FROM corpscout.company_identifier AS ci WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM corpscout.esef_financial_metrics AS m)`,
  people: `SELECT company_id FROM corpscout.se_company_person`,
  peopleBolagsverket: `SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'bolagsverket')`,
  peopleEsef: `SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'esef')`,
  domains: `SELECT company_id FROM corpscout.company_domains WHERE country_code = 'SE'`,
} as const;

/** "This list row's company is in that set." The alias is the list query's
 * own `i`, so every derived expression below is valid in its SELECT, its
 * WHERE and its ORDER BY alike. */
function hasCompanyIn(set: string): string {
  return `i.company_id IN (${set})`;
}

/** `(a OR b OR ...)`, parenthesised so it can be dropped into a concat arm, a
 * WHERE clause or an ORDER BY term without changing what it means. */
function anyOf(...terms: readonly string[]): string {
  return `(${terms.join(" OR ")})`;
}

/**
 * The four presence columns, one expression per datatype, keyed by the
 * client-safe catalog's own keys -- so a datatype added to PROFILE_DATATYPES
 * without an expression here is a compile error, never a column of blanks.
 *
 * `has_financial` is deliberately the OR of the two REGISTER arms rather than
 * a third read of se_company_financials_latest: those two views are exactly
 * what the Financial tab renders (SOURCE_VIEWS in se-company-financial.server
 * .ts), and defining it this way makes an invariant hold by construction --
 * a row can never show Financial ✓ with neither B nor E among its letters.
 */
export const DATATYPE_PRESENCE_EXPR: Record<ProfileDatatypeKey, string> = {
  has_address: hasCompanyIn(COMPANY_SETS.address),
  has_financial: anyOf(
    hasCompanyIn(COMPANY_SETS.financialBolagsverket),
    hasCompanyIn(COMPANY_SETS.financialEsef),
  ),
  has_people: hasCompanyIn(COMPANY_SETS.people),
  has_domains: hasCompanyIn(COMPANY_SETS.domains),
};

/**
 * One SQL predicate per source of `PROFILE_SOURCES` -- "this company HAS that
 * register, in ANY of its five datatypes". They are the single definition of a
 * source's presence: the Sources column's letters are built from them (see
 * PROFILE_SOURCES_EXPR) and the `?source=` filter pushes the very same text as
 * its WHERE predicate, so the letter a row shows and the rows a filter returns
 * can never disagree.
 *
 * - bolagsverket: the address it registered, the annual accounts it holds, or
 *   the roles its people evidence carried. Near-universal in practice (2.86M
 *   of 3.52M companies) -- that is the register's reach, not a bug.
 * - scb: the tautology, on the owner's ruling that SCB is the register base.
 *   It is true by construction, not by luck: info_rules.py's merge returns
 *   None without an SCB row, so an `se_company_info` row without SCB behind it
 *   cannot exist. Kept as a real predicate so the filter has one uniform shape.
 * - esef: the description artifact, an ESEF-sourced filing behind the
 *   Financial tab, or ESEF role evidence. The FILING arm is what makes this
 *   letter worth anything: description_sources names ESEF for 2 companies,
 *   while 404 have ESEF financials. `lei IS NOT NULL` is kept as belt and
 *   braces, NOT for coverage -- se_company_info.lei is written only from the
 *   ESEF artifact (info_rules.py: `lei=_text(esef.values.get("lei")) if esef
 *   else None`, and no correction kind can set it), but today every ESEF merge
 *   participation also writes 'esef' into description_sources, so the two arms
 *   name the same 2 companies. It guards a future filing that carries a LEI
 *   and no description.
 * - wikidata: the mirror image -- wikidata_id is written only from the
 *   Wikidata artifact, and a Wikidata row may exist without contributing a
 *   description.
 *
 * NOT folded in: company_domains.source_names. A domain suggested by
 * 'wikidata' or 'esef_filing' is evidence about a WEBSITE, not a register that
 * built the company profile, and 'common_crawl_identity' has no letter at all;
 * the Domains presence column is where that datatype speaks. (Live check
 * 2026-08-24, corrected by review 2026-08-25: folding them in would move ZERO companies --
 * every published company with a wikidata-sourced domain already carries W via wikidata_id.)
 *
 * Keyed by ProfileSourceValue, so adding a source to the catalog without a
 * predicate here is a type error rather than a silently missing letter.
 */
export const PROFILE_SOURCE_PREDICATES: Record<ProfileSourceValue, string> = {
  bolagsverket: anyOf(
    hasCompanyIn(COMPANY_SETS.addressBolagsverket),
    hasCompanyIn(COMPANY_SETS.financialBolagsverket),
    hasCompanyIn(COMPANY_SETS.peopleBolagsverket),
  ),
  scb: "1",
  esef: anyOf(
    "has(i.description_sources, 'esef')",
    "i.lei IS NOT NULL",
    hasCompanyIn(COMPANY_SETS.financialEsef),
    hasCompanyIn(COMPANY_SETS.peopleEsef),
  ),
  wikidata: anyOf(
    "i.wikidata_id IS NOT NULL",
    "has(i.description_sources, 'wikidata')",
  ),
};

/**
 * The Sources column itself: one compact string per row ('BS', 'SW', 'BSEW'),
 * assembled in the catalog's canonical order so it needs no sorting and so
 * sorting the LIST by it groups like profiles together.
 *
 * Built by mapping the catalog rather than spelled out, so the column, the
 * legend above the table and the filter options are one list in one order.
 */
export const PROFILE_SOURCES_EXPR = `concat(\n${PROFILE_SOURCES.map(
  (source) =>
    `  if(${PROFILE_SOURCE_PREDICATES[source.value]}, '${source.letter}', '')`,
).join(",\n")}\n)`;

/**
 * Every column a header may sort by, mapped to the expression that sorts it.
 * The keys are exactly the columns of `SeCompanyInfoListRow` (the component's
 * `sortKey`s are typed against them), and a request naming anything else falls
 * back to the default -- nothing from the request is ever interpolated. Every
 * computed column sorts by its own expression rather than by the SELECT alias:
 * ClickHouse does not guarantee an alias is visible to ORDER BY at the same
 * query level. `entity_type` sorts by the id length it is derived from, which
 * orders legal (10) before sole (12) -- the same order the labels do.
 */
export const INFO_SORT_COLUMNS = {
  company_id: "i.company_id",
  legal_name: "i.legal_name",
  status: "i.status",
  legal_form_code: "i.legal_form_code",
  entity_type: "length(i.company_id)",
  has_description: "(i.description IS NOT NULL)",
  // Each presence column sorts by the very IN-set expression it is projected
  // from, so "show me the companies with no address" is one header click.
  has_address: DATATYPE_PRESENCE_EXPR.has_address,
  has_financial: DATATYPE_PRESENCE_EXPR.has_financial,
  has_people: DATATYPE_PRESENCE_EXPR.has_people,
  has_domains: DATATYPE_PRESENCE_EXPR.has_domains,
  profile_sources: PROFILE_SOURCES_EXPR,
} as const;

export type SeCompanyInfoSortKey = keyof typeof INFO_SORT_COLUMNS;

export const DEFAULT_INFO_SORT: SeCompanyInfoSortKey = "company_id";
export const DEFAULT_INFO_DIR: SortDir = "asc";

export function resolveInfoSort(
  sort: string | undefined,
  dir: string | undefined,
): { sort: SeCompanyInfoSortKey; dir: SortDir } {
  return {
    sort: resolveSortKey(INFO_SORT_COLUMNS, sort, DEFAULT_INFO_SORT),
    dir: resolveDir(dir, DEFAULT_INFO_DIR),
  };
}

/** company_id is the table's ORDER BY key, so it is a total tiebreak: two
 * pages of a name sort never show the same row twice or skip one. */
export function infoOrderBySql(sort: SeCompanyInfoSortKey, dir: SortDir): string {
  return orderBySql({ expr: INFO_SORT_COLUMNS[sort], dir }, [
    { expr: INFO_SORT_COLUMNS.company_id, dir: "asc" },
  ]);
}

/** No `total` here on purpose: the table's pagination total is
 * `loadSeCompanyInfoCounts`'s `total` (see the route), which the page already
 * loads for the counts strip -- so listing a page of rows never needs its own
 * separate `count()` scan. */
export interface SeCompanyInfoListPage {
  rows: SeCompanyInfoListRow[];
}

/** The counts strip: how many companies match, and how many of them have a
 * published description. Nothing about the model or the review queue -- the
 * Pipeline page owns those numbers. */
export interface SeCompanyInfoListCounts {
  total: number;
  withDescription: number;
  withoutDescription: number;
}

/**
 * WHERE predicates shared by the table query and its counts strip, so the
 * strip always describes exactly what the table shows. Every predicate is
 * appended only when its filter is present -- an absent filter must not
 * appear in the SQL at all, not just evaluate to a no-op.
 */
export function buildInfoListFilter(
  filters: SeCompanyInfoListFilters,
): { where: string[]; params: Record<string, unknown> } {
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  const companyId = nonEmpty(filters.companyId);
  if (companyId) {
    where.push("i.company_id = {companyId:String}");
    params.companyId = companyId;
  }
  const name = nonEmpty(filters.name);
  if (name) {
    where.push("i.legal_name ILIKE {name:String}");
    params.name = `%${name}%`;
  }
  if (filters.entity === "legal") {
    where.push("length(i.company_id) = 10");
  } else if (filters.entity === "sole") {
    where.push("length(i.company_id) = 12");
  }
  const status = discreteValue(filters.status);
  if (status !== null) {
    where.push("toString(i.status) = {status:String}");
    params.status = status;
  }
  // legal_form_code is the one Nullable column among the discrete filters, so
  // its predicate carries the same ifNull guard its option list does --
  // otherwise "no legal form code" could never be selected.
  const legalForm = discreteValue(filters.legalForm);
  if (legalForm !== null) {
    where.push("ifNull(i.legal_form_code, '') = {legalForm:String}");
    params.legalForm = legalForm;
  }
  // IS NOT NULL, never `!= ''`: description is Nullable and the merge writes
  // NULL for "this company has no description", which is what this asks.
  if (filters.description === "yes") {
    where.push("i.description IS NOT NULL");
  } else if (filters.description === "no") {
    where.push("i.description IS NULL");
  }
  // The value NAMES a predicate, it never becomes one: an unknown source (the
  // select's "any", a stale `?source=llm`, anything hand-typed) simply finds
  // no entry and adds nothing, so no filter value ever reaches SQL as text.
  const source = nonEmpty(filters.source);
  if (source !== null && Object.hasOwn(PROFILE_SOURCE_PREDICATES, source)) {
    where.push(PROFILE_SOURCE_PREDICATES[source as ProfileSourceValue]);
  }
  return { where, params };
}

/** No description TEXT crosses the wire for this list: the page shows whether
 * there is one, not what it says, so 3.5M descriptions stay in ClickHouse.
 * entity_type is derived from the id length -- 000299 admitted 12-digit
 * personnummer-based sole-trader ids beside the 10-digit org numbers.
 *
 * The presence columns are projected FROM the catalog, in its order, so the
 * SELECT cannot offer a column the header row does not name (or vice versa),
 * and each is a UInt8 for the same reason has_description is: one predictable
 * JSON shape rather than whatever the driver makes of a boolean. */
export const INFO_LIST_SELECT_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  toString(i.status) AS status,
  ifNull(i.legal_form_code, '') AS legal_form_code,
  i.legal_form_label_en AS legal_form_label_en,
  i.legal_form_label_sv AS legal_form_label_sv,
  if(length(i.company_id) = 12, 'sole', 'legal') AS entity_type,
  toUInt8(i.description IS NOT NULL) AS has_description,
${PROFILE_DATATYPES.map(
  (datatype) =>
    `  toUInt8(${DATATYPE_PRESENCE_EXPR[datatype.key]}) AS ${datatype.key},`,
).join("\n")}
  ${PROFILE_SOURCES_EXPR} AS profile_sources
FROM corpscout.se_company_info AS i FINAL`;

/** One scan for all three numbers: the strip's totals and the table's own
 * pagination total (see `listSeCompanyInfoPage`). */
export const INFO_COUNTS_SQL = `SELECT
  toString(count()) AS total,
  toString(countIf(i.description IS NOT NULL)) AS with_description,
  toString(countIf(i.description IS NULL)) AS without_description
FROM corpscout.se_company_info AS i FINAL`;

function infoFilterClause(filters: SeCompanyInfoListFilters): {
  filter: string;
  where: string[];
  params: Record<string, unknown>;
} {
  const { where, params } = buildInfoListFilter(filters);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  return { filter, where, params };
}

/** No count() query: the page's total is `loadSeCompanyInfoCounts`'s `total`,
 * which shares this exact WHERE and is loaded alongside this call anyway for
 * the counts strip -- one fewer FINAL scan over 3.5M rows per page load. */
export async function listSeCompanyInfoPage(
  query: SeCompanyInfoListQuery,
): Promise<SeCompanyInfoListPage> {
  const { filter, params } = infoFilterClause(query);
  const { limit, offset } = pageParams(query);
  const sort = resolveInfoSort(query.sort, query.dir);

  const rows = await chQuery<SeCompanyInfoListRow>(
    `${INFO_LIST_SELECT_SQL}
${filter}
${infoOrderBySql(sort.sort, sort.dir)}
${PAGE_LIMIT_OFFSET_SQL}`,
    { ...params, limit, offset },
  );
  return { rows };
}

/**
 * Total / with description / without description, computed with the exact same
 * WHERE as `listSeCompanyInfoPage` (built once from the same filters) so the
 * strip never drifts from the table it sits above. `total` is also the table's
 * pagination total.
 */
export async function loadSeCompanyInfoCounts(
  filters: SeCompanyInfoListFilters,
): Promise<SeCompanyInfoListCounts> {
  const { filter, params } = infoFilterClause(filters);
  const [row] = await chQuery<{
    total: string;
    with_description: string;
    without_description: string;
  }>(`${INFO_COUNTS_SQL}\n${filter}`, params);
  return {
    total: Number(row?.total ?? 0),
    withDescription: Number(row?.with_description ?? 0),
    withoutDescription: Number(row?.without_description ?? 0),
  };
}

/* -------------------------------------------------------------------- */
/* Discrete filter option lists (shared by both pages' filter sheets)     */
/* -------------------------------------------------------------------- */

/**
 * How long a table's option lists are served from memory. They are the
 * DISTINCT values of a few low-cardinality columns -- a new status or legal
 * form code appearing ten minutes late in a filter list costs nothing, while
 * scanning 3.5M FINAL-merged rows on every page load would cost a second on
 * every reviewer's every click.
 */
export const FILTER_OPTIONS_TTL_MS = 10 * 60 * 1000;

export interface SeCompanyInfoFilterOptions {
  statuses: string[];
  /**
   * Every legal-form code IN USE (including '' for "no code"), each labelled
   * from the curated dictionary. Codes come from the published rows and labels
   * from corpscout.se_code_labels, so a code the dictionary does not name still
   * appears -- with empty labels, which the option renderer falls back on.
   */
  legalForms: LegalFormLabels[];
}

export interface SeCompanyInfoCorrectionFilterOptions {
  decidedBy: string[];
}

/**
 * ONE query for the two data-driven option lists of se_company_info, read FINAL
 * like every other query against it (a company's status is whatever its newest
 * version says, so the pre-merge parts would offer values no live row has).
 * `groupUniqArray` over a LowCardinality column is a dictionary walk, not a
 * per-row aggregation; `arraySort` makes the option order stable between
 * loads. legal_form_code is Nullable, so its NULL is folded into '' -- the
 * value the "none" option filters on.
 *
 * The legal-form LABELS come from the curated dictionary rather than from the
 * rows' own copies of them: se_company_info carries one label pair per ROW, and
 * during a label rollout the same code appears with the old pair on rows not
 * yet re-resolved and the new pair on the rest -- a groupUniqArray over that
 * would offer the same code twice. The dictionary has exactly one live row per
 * code. It is read in the SAME statement (a scalar subquery over a 57-row
 * table): the option lists of a page are one round trip, not two.
 *
 * The tuple is CAST to a NAMED tuple so JSONEachRow renders each entry as an
 * object rather than a positional array -- a reordered CAST would then fail to
 * type-check here instead of silently transposing code and label.
 */
export const INFO_FILTER_OPTIONS_SQL = `WITH legal_form_labels AS (
  SELECT
    l.code AS code,
    argMax(l.label_sv, l.version) AS label_sv,
    argMax(l.label_en, l.version) AS label_en
  FROM corpscout.se_code_labels AS l
  WHERE l.code_type = 'legal_form'
  GROUP BY l.code
)
SELECT
  arraySort(groupUniqArray(toString(i.status))) AS statuses,
  arraySort(groupUniqArray(ifNull(i.legal_form_code, ''))) AS legal_form_codes,
  (
    SELECT groupArray(CAST(
      (code, label_sv, label_en),
      'Tuple(code String, label_sv String, label_en String)'
    ))
    FROM legal_form_labels
  ) AS legal_form_labels
FROM corpscout.se_company_info AS i FINAL`;

/** The ledger's only data-driven discrete column: correction_kind and the
 * computed status are fixed enums the review page already defines. No FINAL --
 * se_company_info_correction is an append-only MergeTree, not Replacing. */
export const CORRECTION_FILTER_OPTIONS_SQL = `SELECT
  arraySort(groupUniqArray(c.decided_by)) AS decided_by
FROM corpscout.se_company_info_correction AS c`;

interface OptionsCacheEntry<T> {
  value: T;
  at: number;
}

let infoOptionsCache: OptionsCacheEntry<SeCompanyInfoFilterOptions> | null = null;
let correctionOptionsCache: OptionsCacheEntry<SeCompanyInfoCorrectionFilterOptions> | null =
  null;

function isFresh(entry: OptionsCacheEntry<unknown> | null): boolean {
  return entry !== null && Date.now() - entry.at < FILTER_OPTIONS_TTL_MS;
}

/** Drops both cached option lists. Used by tests; also the one lever if a
 * reviewer ever needs the lists refreshed before the TTL expires. */
export function resetSeCompanyInfoFilterOptionsCache(): void {
  infoOptionsCache = null;
  correctionOptionsCache = null;
}

export async function loadSeCompanyInfoFilterOptions(): Promise<SeCompanyInfoFilterOptions> {
  if (isFresh(infoOptionsCache)) return infoOptionsCache!.value;
  const [row] = await chQuery<{
    statuses: string[];
    legal_form_codes: string[];
    legal_form_labels: LegalFormLabels[];
  }>(INFO_FILTER_OPTIONS_SQL);
  // The dictionary keyed by code, then one option per code IN USE: a curated
  // code nobody carries is not an option (it would filter to nothing), and a
  // code in use that the dictionary does not name still is -- unlabelled.
  const labels = new Map(
    (row?.legal_form_labels ?? []).map((entry) => [entry.code, entry]),
  );
  const value: SeCompanyInfoFilterOptions = {
    statuses: row?.statuses ?? [],
    legalForms: (row?.legal_form_codes ?? []).map((code) => ({
      code,
      label_sv: labels.get(code)?.label_sv ?? "",
      label_en: labels.get(code)?.label_en ?? "",
    })),
  };
  infoOptionsCache = { value, at: Date.now() };
  return value;
}

export async function loadSeCompanyInfoCorrectionFilterOptions(): Promise<SeCompanyInfoCorrectionFilterOptions> {
  if (isFresh(correctionOptionsCache)) return correctionOptionsCache!.value;
  const [row] = await chQuery<{ decided_by: string[] }>(CORRECTION_FILTER_OPTIONS_SQL);
  const value: SeCompanyInfoCorrectionFilterOptions = {
    decidedBy: row?.decided_by ?? [],
  };
  correctionOptionsCache = { value, at: Date.now() };
  return value;
}

/* -------------------------------------------------------------------- */
/* Page 2: /admin/se/company-info/corrections -- the correction ledger  */
/* -------------------------------------------------------------------- */

export interface SeCompanyInfoCorrectionListRow {
  correction_id: string;
  company_id: string;
  created_at: string;
  correction_kind: string;
  payload: string;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  status: SeInfoCorrectionStatus;
}

export interface SeCompanyInfoCorrectionListFilters {
  companyId?: string;
  kind?: string;
  status?: string;
  decidedBy?: string;
}

export interface SeCompanyInfoCorrectionListQuery extends SeCompanyInfoCorrectionListFilters {
  page: number;
  pageSize: number;
  sort?: string;
  dir?: string;
}

export interface SeCompanyInfoCorrectionListPage {
  rows: SeCompanyInfoCorrectionListRow[];
  total: number;
}

/** A later undo's supersedes_correction_id names the id it cancels, across
 * every company -- not scoped to one company_id like se-company-info.server
 * ts's per-company `superseded` CTE, since this ledger lists every company. */
export const UNDONE_CTE_SQL = `WITH undone AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_info_correction
  WHERE supersedes_correction_id IS NOT NULL
)`;

/**
 * Status precedence mirrors Dagster's apply_info_ledger ranking: an undo
 * always wins regardless of anything else; then whether the *published* row
 * still names this correction_id (has() on correction_ids, an Array(UUID) --
 * a LEFT JOIN miss defaults it to [], so unlike evidence_set_hash it needs
 * no ifNull guard); then whether the evidence has moved since the row was
 * decided (guarded with ifNull -- a FixedString column turns Nullable on a
 * LEFT JOIN miss under join_use_nulls); else pending. Branch ORDER matters:
 * multiIf evaluates left to right and returns the first match, so undone
 * must precede applied must precede stale must precede the pending default.
 */
export const CORRECTION_STATUS_EXPR = `multiIf(
    c.correction_id IN (SELECT id FROM undone), 'undone',
    has(p.correction_ids, c.correction_id) != 0, 'applied',
    toString(c.evidence_hash) != {zeroHash:String}
      AND toString(c.evidence_hash) != ifNull(toString(p.evidence_set_hash), ''), 'stale',
    'pending'
  )`;

/**
 * The published row, scoped to only the companies actually present in the
 * ledger before FINAL collapses it -- an unscoped `LEFT JOIN
 * corpscout.se_company_info AS p FINAL` re-merges and reads the whole 3.5M
 * -row/334MB table to decorate a handful of ledger rows. Scoping the join's
 * own subquery to `company_id IN (SELECT company_id FROM
 * se_company_info_correction)` first (verified byte-identical output, ~2.5MB
 * read) keeps FINAL's merge work bounded to just those companies. Only the
 * three columns CORRECTION_STATUS_EXPR reads are projected.
 */
export const SCOPED_PUBLISHED_JOIN_SQL = `LEFT JOIN (
  SELECT company_id, correction_ids, evidence_set_hash
  FROM corpscout.se_company_info FINAL
  WHERE company_id IN (SELECT company_id FROM corpscout.se_company_info_correction)
) AS p ON p.company_id = c.company_id`;

export const CORRECTIONS_LIST_SELECT_SQL = `${UNDONE_CTE_SQL}
SELECT
  toString(c.correction_id) AS correction_id,
  c.company_id AS company_id,
  toString(c.created_at) AS created_at,
  c.correction_kind AS correction_kind,
  c.payload AS payload,
  c.reason AS reason,
  c.decided_by AS decided_by,
  toString(c.supersedes_correction_id) AS supersedes_correction_id,
  ${CORRECTION_STATUS_EXPR} AS status
FROM corpscout.se_company_info_correction AS c
${SCOPED_PUBLISHED_JOIN_SQL}`;

export const CORRECTIONS_LIST_COUNT_SQL = `${UNDONE_CTE_SQL}
SELECT toString(count()) AS total
FROM corpscout.se_company_info_correction AS c
${SCOPED_PUBLISHED_JOIN_SQL}`;

/** The status filter reuses CORRECTION_STATUS_EXPR verbatim as a WHERE
 * predicate (not a reference to the SELECT alias) -- ClickHouse does not
 * guarantee a SELECT-list alias is visible to WHERE at the same query
 * level, so the same expression text is evaluated again rather than relied
 * on by name. zeroHash is always included: CORRECTION_STATUS_EXPR sits in
 * the SELECT list of every query built from CORRECTIONS_LIST_SELECT_SQL
 * regardless of which filters are set. `kind`/`status` are whitelisted
 * against the same enums the review page and its ledger use, so an
 * unrecognized value (including the filter form's "any" sentinel) is
 * silently treated as absent rather than filtering on literal garbage. */
export function buildCorrectionsListFilter(
  filters: SeCompanyInfoCorrectionListFilters,
): { where: string[]; params: Record<string, unknown> } {
  const where: string[] = [];
  const params: Record<string, unknown> = { zeroHash: ZERO_EVIDENCE_HASH };

  const companyId = nonEmpty(filters.companyId);
  if (companyId) {
    where.push("c.company_id = {companyId:String}");
    params.companyId = companyId;
  }
  const kind = nonEmpty(filters.kind);
  if (kind && (SE_INFO_CORRECTION_KINDS as readonly string[]).includes(kind)) {
    where.push("c.correction_kind = {kind:String}");
    params.kind = kind;
  }
  const status = nonEmpty(filters.status);
  if (status && (SE_INFO_CORRECTION_STATUSES as readonly string[]).includes(status)) {
    where.push(`(${CORRECTION_STATUS_EXPR}) = {status:String}`);
    params.status = status;
  }
  // decided_by has no enum to whitelist against (it is whoever wrote the row:
  // the backoffice today, another actor tomorrow), so its options come from
  // the ledger itself and the chosen one travels as a named param.
  const decidedBy = discreteValue(filters.decidedBy);
  if (decidedBy !== null) {
    where.push("c.decided_by = {decidedBy:String}");
    params.decidedBy = decidedBy;
  }
  return { where, params };
}

/**
 * Every column of the ledger a header may sort by. `status` is computed, so it
 * sorts by CORRECTION_STATUS_EXPR itself rather than by the SELECT alias --
 * ClickHouse does not guarantee an alias is visible to ORDER BY at the same
 * query level any more than it does to WHERE.
 */
export const CORRECTION_SORT_COLUMNS = {
  created_at: "c.created_at",
  company_id: "c.company_id",
  correction_id: "c.correction_id",
  correction_kind: "c.correction_kind",
  payload: "c.payload",
  reason: "c.reason",
  decided_by: "c.decided_by",
  status: `(${CORRECTION_STATUS_EXPR})`,
} as const;

export type SeCompanyInfoCorrectionSortKey = keyof typeof CORRECTION_SORT_COLUMNS;

export const DEFAULT_CORRECTION_SORT: SeCompanyInfoCorrectionSortKey = "created_at";
export const DEFAULT_CORRECTION_DIR: SortDir = "desc";

export function resolveCorrectionsSort(
  sort: string | undefined,
  dir: string | undefined,
): { sort: SeCompanyInfoCorrectionSortKey; dir: SortDir } {
  return {
    sort: resolveSortKey(CORRECTION_SORT_COLUMNS, sort, DEFAULT_CORRECTION_SORT),
    dir: resolveDir(dir, DEFAULT_CORRECTION_DIR),
  };
}

/** (created_at DESC, correction_id DESC) stays the tiebreak -- and, when
 * nothing else is chosen, IS the sort, unchanged from before sorting existed. */
export function correctionsOrderBySql(
  sort: SeCompanyInfoCorrectionSortKey,
  dir: SortDir,
): string {
  return orderBySql({ expr: CORRECTION_SORT_COLUMNS[sort], dir }, [
    { expr: CORRECTION_SORT_COLUMNS.created_at, dir: "desc" },
    { expr: CORRECTION_SORT_COLUMNS.correction_id, dir: "desc" },
  ]);
}

export async function listSeCompanyInfoCorrectionsPage(
  query: SeCompanyInfoCorrectionListQuery,
): Promise<SeCompanyInfoCorrectionListPage> {
  const { where, params } = buildCorrectionsListFilter(query);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const { limit, offset } = pageParams(query);
  const sort = resolveCorrectionsSort(query.sort, query.dir);

  const [rows, counted] = await Promise.all([
    chQuery<SeCompanyInfoCorrectionListRow>(
      `${CORRECTIONS_LIST_SELECT_SQL}
${filter}
${correctionsOrderBySql(sort.sort, sort.dir)}
${PAGE_LIMIT_OFFSET_SQL}`,
      { ...params, limit, offset },
    ),
    chQuery<{ total: string }>(`${CORRECTIONS_LIST_COUNT_SQL}\n${filter}`, params),
  ]);
  return { rows, total: Number(counted[0]?.total ?? 0) };
}
