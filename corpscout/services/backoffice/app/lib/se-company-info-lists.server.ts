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
} from "~/lib/se-company-info-filters";
import { INFO_LIST_SOURCES } from "~/lib/se-company-info-sources";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export type { SeInfoCorrectionStatus };

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

/**
 * A data-driven discrete filter's value: absent when unset or when the select
 * says "Any", and the empty string when it says "none" (a company with no
 * legal form code / description language / status recorded). Values reach SQL
 * only as named params, never as text -- the option lists these come from are
 * read from the column itself (see loadSeCompanyInfoFilterOptions), so there
 * is no fixed enum to whitelist them against the way `source` and `kind` have.
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

export interface SeCompanyInfoListRow {
  company_id: string;
  legal_name: string;
  status: string;
  legal_form_code: string;
  description_source: string;
  description_sources: string[];
  description_language: string;
  description_snippet: string;
  has_suggestion: number;
  corrections_count: number;
  resolved_at: string;
}

export interface SeCompanyInfoListFilters {
  companyId?: string;
  name?: string;
  source?: string;
  status?: string;
  legalForm?: string;
  language?: string;
  /** "yes" | "no" -- anything else (including the select's "any") is absent. */
  suggestion?: string;
  multi?: boolean;
  /** "legal" | "sole" -- anything else is absent. Plain `string` so the
   * routes' parsed, already-validated filter object is assignable as-is
   * instead of being re-spelled field by field. */
  entity?: string;
  corrected?: boolean;
}

export interface SeCompanyInfoListQuery extends SeCompanyInfoListFilters {
  page: number;
  pageSize: number;
  sort?: string;
  dir?: string;
}

/**
 * Every column a header may sort by, mapped to the expression that sorts it.
 * The keys are exactly the columns of `SeCompanyInfoListRow` (the component's
 * `sortKey`s are typed against them), and a request naming anything else falls
 * back to the default -- nothing from the request is ever interpolated.
 * `has_suggestion` sorts by the underlying nullable id, `corrections_count` by
 * the array length the row shows.
 */
export const INFO_SORT_COLUMNS = {
  company_id: "i.company_id",
  legal_name: "i.legal_name",
  status: "i.status",
  legal_form_code: "i.legal_form_code",
  description_source: "i.description_source",
  description_sources: "i.description_sources",
  description_language: "i.description_language",
  description_snippet: "i.description",
  has_suggestion: "i.suggestion_id",
  corrections_count: "length(i.correction_ids)",
  resolved_at: "i.resolved_at",
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

/** No `total` here on purpose: the table's pagination total is derived from
 * `loadSeCompanyInfoCounts`'s by-source breakdown (see the route), which the
 * page already loads for the counts strip -- so listing a page of rows never
 * needs its own separate `count()` scan. */
export interface SeCompanyInfoListPage {
  rows: SeCompanyInfoListRow[];
}

export interface SeCompanyInfoSourceCount {
  source: string;
  count: number;
}

export interface SeCompanyInfoListCounts {
  bySource: SeCompanyInfoSourceCount[];
  multiSourceCount: number;
  pendingModelCount: number;
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
  const source = nonEmpty(filters.source);
  if (source && (INFO_LIST_SOURCES as readonly string[]).includes(source)) {
    where.push("toString(i.description_source) = {source:String}");
    params.source = source === "none" ? "" : source;
  }
  if (filters.multi) {
    where.push("i.description_source_count > 1");
  }
  if (filters.entity === "legal") {
    where.push("length(i.company_id) = 10");
  } else if (filters.entity === "sole") {
    where.push("length(i.company_id) = 12");
  }
  if (filters.corrected) {
    where.push("notEmpty(i.correction_ids)");
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
  const language = discreteValue(filters.language);
  if (language !== null) {
    where.push("toString(i.description_language) = {language:String}");
    params.language = language;
  }
  if (filters.suggestion === "yes") {
    where.push("i.suggestion_id IS NOT NULL");
  } else if (filters.suggestion === "no") {
    where.push("i.suggestion_id IS NULL");
  }
  return { where, params };
}

/** description snippet is truncated in SQL with substringUTF8 (not `substring`,
 * which is byte-based and cuts a multi-byte character -- å/ä/ö -- in half,
 * rendering U+FFFD) and not just CSS, so the full description text of 3.5M
 * rows never crosses the wire for a page that only shows the first 120
 * characters. */
export const INFO_LIST_SELECT_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  toString(i.status) AS status,
  ifNull(i.legal_form_code, '') AS legal_form_code,
  toString(i.description_source) AS description_source,
  i.description_sources AS description_sources,
  toString(i.description_language) AS description_language,
  substringUTF8(ifNull(i.description, ''), 1, 120) AS description_snippet,
  toUInt8(i.suggestion_id IS NOT NULL) AS has_suggestion,
  length(i.correction_ids) AS corrections_count,
  toString(i.resolved_at) AS resolved_at
FROM corpscout.se_company_info AS i FINAL`;

export const INFO_COUNTS_BY_SOURCE_SQL = `SELECT
  toString(i.description_source) AS description_source,
  toString(count()) AS count
FROM corpscout.se_company_info AS i FINAL`;

export const INFO_COUNTS_TOTALS_SQL = `SELECT
  toString(countIf(i.description_source_count > 1)) AS multi_source_count,
  toString(countIf(
    i.description_source_count > 1
    AND i.suggestion_id IS NULL
    AND empty(i.correction_ids)
  )) AS pending_model_count
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

/** No count() query: the page's total is derived by the route from
 * `loadSeCompanyInfoCounts`'s by-source breakdown, which shares this exact
 * WHERE and is loaded alongside this call anyway for the counts strip -- one
 * fewer FINAL scan over 3.5M rows per page load. */
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
 * Rows by description_source, plus the multi-source and pending-model
 * totals, all computed with the exact same WHERE as `listSeCompanyInfoPage`
 * (built once from the same filters) so the strip never drifts from the
 * table it sits above. Summing `bySource[].count` gives the table's
 * pagination total (see `listSeCompanyInfoPage`'s doc comment).
 */
export async function loadSeCompanyInfoCounts(
  filters: SeCompanyInfoListFilters,
): Promise<SeCompanyInfoListCounts> {
  const { filter, params } = infoFilterClause(filters);

  const [bySource, totals] = await Promise.all([
    chQuery<{ description_source: string; count: string }>(
      `${INFO_COUNTS_BY_SOURCE_SQL}
${filter}
GROUP BY i.description_source
ORDER BY i.description_source`,
      params,
    ),
    chQuery<{ multi_source_count: string; pending_model_count: string }>(
      `${INFO_COUNTS_TOTALS_SQL}\n${filter}`,
      params,
    ),
  ]);
  return {
    bySource: bySource.map((r) => ({ source: r.description_source, count: Number(r.count) })),
    multiSourceCount: Number(totals[0]?.multi_source_count ?? 0),
    pendingModelCount: Number(totals[0]?.pending_model_count ?? 0),
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
  legalFormCodes: string[];
  descriptionLanguages: string[];
}

export interface SeCompanyInfoCorrectionFilterOptions {
  decidedBy: string[];
}

/**
 * ONE query for every data-driven option list of se_company_info, read FINAL
 * like every other query against it (a company's status is whatever its newest
 * version says, so the pre-merge parts would offer values no live row has).
 * `groupUniqArray` over a LowCardinality column is a dictionary walk, not a
 * per-row aggregation; `arraySort` makes the option order stable between
 * loads. legal_form_code is Nullable, so its NULL is folded into '' -- the
 * value the "none" option filters on.
 */
export const INFO_FILTER_OPTIONS_SQL = `SELECT
  arraySort(groupUniqArray(toString(i.status))) AS statuses,
  arraySort(groupUniqArray(ifNull(i.legal_form_code, ''))) AS legal_form_codes,
  arraySort(groupUniqArray(toString(i.description_language))) AS description_languages
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
    description_languages: string[];
  }>(INFO_FILTER_OPTIONS_SQL);
  const value: SeCompanyInfoFilterOptions = {
    statuses: row?.statuses ?? [],
    legalFormCodes: row?.legal_form_codes ?? [],
    descriptionLanguages: row?.description_languages ?? [],
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
