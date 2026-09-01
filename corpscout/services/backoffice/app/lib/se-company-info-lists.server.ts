/**
 * The `/admin/se/companies` list page: every published SE company (3.5M
 * rows, read off the consolidated serving view
 * `corpscout.se_companies_serving` -- see SE_COMPANIES_SERVING_TABLE). It
 * mirrors se-company-info.server.ts's client/query style -- explicit column
 * aliasing, hash/UUID columns wrapped in toString() -- but adds server-side
 * paging and optional URL-driven filters, so the WHERE clause is built
 * dynamically (like procurements.server.ts's buildSourceFilter) instead of
 * being a fixed string. The paging/sorting helpers and the option-cache TTL
 * are shared with the address ledger's list (se-company-address-lists.server.ts).
 */
import { chQuery } from "~/lib/clickhouse.server";
import type { LegalFormLabels } from "~/lib/se-legal-form";
import type { SortDir } from "~/lib/countries";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  ANY_FILTER_VALUE,
  NONE_FILTER_VALUE,
  PROFILE_DATATYPES,
  PROFILE_SOURCES,
  type ProfileDatatypeKey,
  type ProfileSourceValue,
} from "~/lib/se-company-info-filters";

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
 * Cost note: se_companies_serving is a 3.5M-row plain MergeTree ordered by
 * company_id, so sorting a page by any other column is a top-N sort
 * (`ORDER BY ... LIMIT/OFFSET`), which ClickHouse does in one pass with a
 * bounded heap -- fine for an admin list. A DEEP page on such a sort (offset
 * in the hundreds of thousands) degenerates into a full sort; that is
 * accepted here rather than restricting sorting to the key.
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
/* Page 1: /admin/se/companies -- the se_companies_serving view      */
/* -------------------------------------------------------------------- */

/**
 * One company, as this list shows it. Task 17 (owner addendum 2026-08-23):
 * /admin/se/companies is a COMPANIES list, not a description view -- so it
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
  /** 0 | 1 -- the serving view's own precomputed flag (`description IS NOT
   * NULL` at refresh time). */
  has_description: number;
  /** 0 | 1 -- has a live row in corpscout.se_company_address. */
  has_address: number;
  /**
   * 0 | 1 -- ANY financial data: extracted Bolagsverket/ESEF metrics OR a
   * filed/parsed report with nothing extracted yet. Owner ruling (2026-08-24
   * addendum, applied 2026-08-25): the list must never show "—" for a company
   * whose Financial tab renders a "Filed reports" card, so ✓ means any
   * financial data, not "has extracted figures" -- the list matches the tab
   * by principle.
   */
  has_financial: number;
  /** 0 | 1 -- has a published row in corpscout.se_company_person. */
  has_people: number;
  /** 0 | 1 -- has a Swedish row in the unified corpscout.company_domains. */
  has_domains: number;
  /** 0 | 1 -- an ESEF filing exists for the company's LEI (listed on an EU
   * regulated market). */
  is_publicly_traded: number;
  /** 0 | 1 -- exact-matched winner rows in corpscout.se_government_contracts. */
  has_government_contracts: number;
  /** 0 | 1 -- Platsbanken job-ad history (open now or in the past). */
  has_job_ads: number;
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
  /**
   * Keys of DATATYPE_PRESENCE_EXPR -- companies that have ALL of them (one
   * `= 1` predicate each, ANDed like every other filter). Plain `string[]`
   * for the same reason `entity` is; anything else in the array is absent.
   */
  datatypes?: readonly string[];
}

export interface SeCompanyInfoListQuery extends SeCompanyInfoListFilters {
  page: number;
  pageSize: number;
  sort?: string;
  dir?: string;
}

/* -------------------------------------------------------------------- */
/* The serving view: presence flags and Sources letters, precomputed     */
/* -------------------------------------------------------------------- */

/**
 * SOURCE: `corpscout.se_companies_serving` -- the consolidated per-company
 * serving MV (migration 000335, refreshed every 15 minutes, built by
 * dagster_v3 sweden_company/companies_current.py `build_se_companies_serving_sql`).
 * ONE wide row per published SE company, plain MergeTree ORDER BY company_id,
 * read with NO FINAL.
 *
 * The five presence flags (has_description/has_address/has_financial/
 * has_people/has_domains) and the three stored source flags
 * (source_bolagsverket/source_esef/source_wikidata) are PRECOMPUTED there.
 * This page used to derive them per load as `company_id IN (<subquery>)` sets
 * over each datatype's own final table (~1.4s for the list + ~1.6s for the
 * counts, every click); the serving view pays that once at refresh time and
 * answers the same questions as plain-column reads in 40-80ms.
 *
 * The flags' single source of truth is now the Dagster builder: which table
 * is authoritative for which register, the is_current/FINAL reasoning per
 * datatype, the owner ruling that `has_financial` means ANY financial data
 * (extracted Bolagsverket/ESEF metrics OR a filed/parsed report), and the
 * ruling that the filed-reports arm feeds NO letter all live in
 * `build_se_companies_serving_sql`'s doc comments -- this module only names
 * the columns. The SCB flag is deliberately NOT stored: it is 1 for every row
 * by construction (owner ruling: SCB is the register base), so its letter is
 * hard-coded via the `"1"` predicate below.
 */
export const SE_COMPANIES_SERVING_TABLE = "corpscout.se_companies_serving";

/**
 * The four presence columns, keyed by the client-safe catalog's own keys --
 * so a datatype added to PROFILE_DATATYPES without a column here is a compile
 * error, never a column of blanks. Each is the serving view's precomputed
 * UInt8 flag (semantics settled in the Dagster builder -- see
 * SE_COMPANIES_SERVING_TABLE's doc comment), read with the list query's own
 * `i` alias so the same text is valid in its SELECT, WHERE and ORDER BY alike.
 */
export const DATATYPE_PRESENCE_EXPR: Record<ProfileDatatypeKey, string> = {
  has_address: "i.has_address",
  has_financial: "i.has_financial",
  has_people: "i.has_people",
  has_domains: "i.has_domains",
  is_publicly_traded: "i.is_publicly_traded",
  has_government_contracts: "i.has_government_contracts",
  has_job_ads: "i.has_job_ads",
};

/**
 * One SQL predicate per source of `PROFILE_SOURCES` -- "this company HAS that
 * register, in ANY of its five datatypes". They are the single definition of a
 * source's presence: the Sources column's letters are built from them (see
 * PROFILE_SOURCES_EXPR) and the `?source=` filter pushes the very same text as
 * its WHERE predicate, so the letter a row shows and the rows a filter returns
 * can never disagree.
 *
 * Each is the serving view's stored flag; WHICH tables earn a register its
 * flag (addresses/accounts/people for Bolagsverket; description/LEI/filing/
 * people for ESEF; wikidata_id/description for Wikidata; NOT
 * company_domains.source_names, NOT the filed-reports arm) is settled in the
 * Dagster builder now -- see SE_COMPANIES_SERVING_TABLE's doc comment.
 *
 * - scb: the tautology, on the owner's ruling that SCB is the register base.
 *   True by construction (info_rules.py's merge returns None without an SCB
 *   row), which is exactly why the view stores no flag for it; kept as a real
 *   predicate so the filter has one uniform shape.
 *
 * Keyed by ProfileSourceValue, so adding a source to the catalog without a
 * predicate here is a type error rather than a silently missing letter.
 */
export const PROFILE_SOURCE_PREDICATES: Record<ProfileSourceValue, string> = {
  bolagsverket: "i.source_bolagsverket = 1",
  scb: "1",
  esef: "i.source_esef = 1",
  wikidata: "i.source_wikidata = 1",
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
  has_description: "i.has_description",
  // Each presence column sorts by the very flag column it is projected from,
  // so "show me the companies with no address" is one header click.
  has_address: DATATYPE_PRESENCE_EXPR.has_address,
  has_financial: DATATYPE_PRESENCE_EXPR.has_financial,
  has_people: DATATYPE_PRESENCE_EXPR.has_people,
  has_domains: DATATYPE_PRESENCE_EXPR.has_domains,
  is_publicly_traded: DATATYPE_PRESENCE_EXPR.is_publicly_traded,
  has_government_contracts: DATATYPE_PRESENCE_EXPR.has_government_contracts,
  has_job_ads: DATATYPE_PRESENCE_EXPR.has_job_ads,
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
 * Pipeline sheet owns those numbers. */
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
    where.push("i.status = {status:String}");
    params.status = status;
  }
  // legal_form_code was Nullable on se_company_info; the serving view folds
  // its NULL to '' (the same '' the "none" option filters on), so the
  // predicate needs no ifNull guard any more.
  const legalForm = discreteValue(filters.legalForm);
  if (legalForm !== null) {
    where.push("i.legal_form_code = {legalForm:String}");
    params.legalForm = legalForm;
  }
  // The serving view's precomputed flag (description IS NOT NULL at refresh
  // time) -- the description text itself is not a column of the view at all.
  if (filters.description === "yes") {
    where.push("i.has_description = 1");
  } else if (filters.description === "no") {
    where.push("i.has_description = 0");
  }
  // The value NAMES a predicate, it never becomes one: an unknown source (the
  // select's "any", a stale `?source=llm`, anything hand-typed) simply finds
  // no entry and adds nothing, so no filter value ever reaches SQL as text.
  const source = nonEmpty(filters.source);
  if (source !== null && Object.hasOwn(PROFILE_SOURCE_PREDICATES, source)) {
    where.push(PROFILE_SOURCE_PREDICATES[source as ProfileSourceValue]);
  }
  // AND semantics on purpose: each selected datatype is its own `= 1`
  // predicate, joined by the shared `where.join(" AND ")` -- the reviewer who
  // ticks Address and People asks for companies that have BOTH. Same
  // key-names-a-predicate rule as `source`: a value not in the catalog record
  // finds no expression and adds nothing, so nothing from the URL ever
  // reaches SQL as text (no params needed).
  for (const datatype of filters.datatypes ?? []) {
    if (Object.hasOwn(DATATYPE_PRESENCE_EXPR, datatype)) {
      where.push(`${DATATYPE_PRESENCE_EXPR[datatype as ProfileDatatypeKey]} = 1`);
    }
  }
  return { where, params };
}

/** No description TEXT crosses the wire for this list (the serving view does
 * not even carry it): the page shows whether there is one, not what it says,
 * so 3.5M descriptions stay in ClickHouse. entity_type is derived from the id
 * length -- 000299 admitted 12-digit personnummer-based sole-trader ids
 * beside the 10-digit org numbers.
 *
 * The presence columns are projected FROM the catalog, in its order, so the
 * SELECT cannot offer a column the header row does not name (or vice versa);
 * each is the view's own precomputed UInt8 flag -- a plain column read, NO
 * FINAL, no IN-subqueries (see SE_COMPANIES_SERVING_TABLE). */
export const INFO_LIST_SELECT_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  i.status AS status,
  i.legal_form_code AS legal_form_code,
  i.legal_form_label_en AS legal_form_label_en,
  i.legal_form_label_sv AS legal_form_label_sv,
  if(length(i.company_id) = 12, 'sole', 'legal') AS entity_type,
  i.has_description AS has_description,
${PROFILE_DATATYPES.map(
  (datatype) =>
    `  ${DATATYPE_PRESENCE_EXPR[datatype.key]} AS ${datatype.key},`,
).join("\n")}
  ${PROFILE_SOURCES_EXPR} AS profile_sources
FROM ${SE_COMPANIES_SERVING_TABLE} AS i`;

/** One scan for all three numbers: the strip's totals and the table's own
 * pagination total (see `listSeCompanyInfoPage`). Same table, same NO FINAL:
 * countIf over the precomputed flag instead of a Nullable-text check. */
export const INFO_COUNTS_SQL = `SELECT
  toString(count()) AS total,
  toString(countIf(i.has_description = 1)) AS with_description,
  toString(countIf(i.has_description = 0)) AS without_description
FROM ${SE_COMPANIES_SERVING_TABLE} AS i`;

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
 * the counts strip -- one fewer scan over the 3.5M-row view per page load. */
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
 * scanning the 3.5M-row serving view on every page load would still be a
 * needless per-click read (cheap as the view is, cached beats re-scanned).
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

/** The option list a correction ledger's filter sheet takes. The info ledger
 * that first read one is gone; the ADDRESS ledger's loader
 * (se-company-address-lists.server.ts) fills this same shape. */
export interface SeCompanyInfoCorrectionFilterOptions {
  decidedBy: string[];
}

/**
 * ONE query for the two data-driven option lists of the company list, read
 * off the serving view like the list itself (NO FINAL -- the view is a plain
 * MergeTree, and its refresh already folded legal_form_code's NULL into ''
 * -- the value the "none" option filters on). `groupUniqArray` over a
 * low-cardinality column is cheap; `arraySort` makes the option order stable
 * between loads.
 *
 * The legal-form LABELS come from the curated dictionary rather than from the
 * rows' own copies of them: the view carries one label pair per ROW, and
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
  arraySort(groupUniqArray(i.status)) AS statuses,
  arraySort(groupUniqArray(i.legal_form_code)) AS legal_form_codes,
  (
    SELECT groupArray(CAST(
      (code, label_sv, label_en),
      'Tuple(code String, label_sv String, label_en String)'
    ))
    FROM legal_form_labels
  ) AS legal_form_labels
FROM ${SE_COMPANIES_SERVING_TABLE} AS i`;

interface OptionsCacheEntry<T> {
  value: T;
  at: number;
}

let infoOptionsCache: OptionsCacheEntry<SeCompanyInfoFilterOptions> | null = null;

function isFresh(entry: OptionsCacheEntry<unknown> | null): boolean {
  return entry !== null && Date.now() - entry.at < FILTER_OPTIONS_TTL_MS;
}

/** Drops the cached option list. Used by tests; also the one lever if a
 * reviewer ever needs the list refreshed before the TTL expires. */
export function resetSeCompanyInfoFilterOptionsCache(): void {
  infoOptionsCache = null;
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
