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
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export { ZERO_EVIDENCE_HASH };

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

function clampPageSize(pageSize: number): number {
  const n = Math.trunc(pageSize);
  return Math.min(200, Math.max(10, Number.isFinite(n) && n > 0 ? n : 50));
}

function pageParams(query: { page: number; pageSize: number }): {
  limit: number;
  offset: number;
} {
  const limit = clampPageSize(query.pageSize);
  const p = Math.trunc(query.page);
  const page = Number.isFinite(p) && p > 0 ? p : 1;
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
  description_source: string;
  description_sources: string[];
  description_snippet: string;
  has_suggestion: number;
  corrections_count: number;
  resolved_at: string;
}

export interface SeCompanyInfoListFilters {
  companyId?: string;
  name?: string;
  source?: string;
  multi?: boolean;
  entity?: "legal" | "sole";
  corrected?: boolean;
}

export interface SeCompanyInfoListQuery extends SeCompanyInfoListFilters {
  page: number;
  pageSize: number;
}

export interface SeCompanyInfoListPage {
  rows: SeCompanyInfoListRow[];
  total: number;
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

/** description_source values the pipeline writes; 'none' is the UI's name
 * for the empty-string source (a row whose description has no source yet). */
export const INFO_LIST_SOURCES = [
  "scb",
  "wikidata",
  "esef",
  "llm",
  "reviewed",
  "none",
] as const;

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
  return { where, params };
}

/** description snippet is truncated in SQL (not just CSS) so 3.5M rows of
 * full description text never cross the wire for a page that only shows the
 * first 120 characters. */
export const INFO_LIST_SELECT_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  toString(i.status) AS status,
  toString(i.description_source) AS description_source,
  i.description_sources AS description_sources,
  substring(ifNull(i.description, ''), 1, 120) AS description_snippet,
  toUInt8(i.suggestion_id IS NOT NULL) AS has_suggestion,
  length(i.correction_ids) AS corrections_count,
  toString(i.resolved_at) AS resolved_at
FROM corpscout.se_company_info AS i FINAL`;

export const INFO_LIST_COUNT_SQL = `SELECT toString(count()) AS total
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

export async function listSeCompanyInfoPage(
  query: SeCompanyInfoListQuery,
): Promise<SeCompanyInfoListPage> {
  const { filter, params } = infoFilterClause(query);
  const { limit, offset } = pageParams(query);

  const [rows, counted] = await Promise.all([
    chQuery<SeCompanyInfoListRow>(
      `${INFO_LIST_SELECT_SQL}
${filter}
ORDER BY i.company_id
${PAGE_LIMIT_OFFSET_SQL}`,
      { ...params, limit, offset },
    ),
    chQuery<{ total: string }>(`${INFO_LIST_COUNT_SQL}\n${filter}`, params),
  ]);
  return { rows, total: Number(counted[0]?.total ?? 0) };
}

/**
 * Rows by description_source, plus the multi-source and pending-model
 * totals, all computed with the exact same WHERE as `listSeCompanyInfoPage`
 * (built once from the same filters) so the strip never drifts from the
 * table it sits above.
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
/* Page 2: /admin/se/company-info/corrections -- the correction ledger  */
/* -------------------------------------------------------------------- */

export type SeInfoCorrectionStatus = "undone" | "applied" | "stale" | "pending";

export const SE_INFO_CORRECTION_STATUSES: readonly SeInfoCorrectionStatus[] = [
  "pending",
  "applied",
  "stale",
  "undone",
];

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
}

export interface SeCompanyInfoCorrectionListQuery extends SeCompanyInfoCorrectionListFilters {
  page: number;
  pageSize: number;
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
 * LEFT JOIN miss under join_use_nulls); else pending.
 */
export const CORRECTION_STATUS_EXPR = `multiIf(
    c.correction_id IN (SELECT id FROM undone), 'undone',
    has(p.correction_ids, c.correction_id) != 0, 'applied',
    toString(c.evidence_hash) != {zeroHash:String}
      AND toString(c.evidence_hash) != ifNull(toString(p.evidence_set_hash), ''), 'stale',
    'pending'
  )`;

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
LEFT JOIN corpscout.se_company_info AS p FINAL ON p.company_id = c.company_id`;

export const CORRECTIONS_LIST_COUNT_SQL = `${UNDONE_CTE_SQL}
SELECT toString(count()) AS total
FROM corpscout.se_company_info_correction AS c
LEFT JOIN corpscout.se_company_info AS p FINAL ON p.company_id = c.company_id`;

/** The status filter reuses CORRECTION_STATUS_EXPR verbatim as a WHERE
 * predicate (not a reference to the SELECT alias) -- ClickHouse does not
 * guarantee a SELECT-list alias is visible to WHERE at the same query
 * level, so the same expression text is evaluated again rather than relied
 * on by name. zeroHash is always included: CORRECTION_STATUS_EXPR sits in
 * the SELECT list of every query built from CORRECTIONS_LIST_SELECT_SQL
 * regardless of which filters are set. */
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
  if (kind) {
    where.push("c.correction_kind = {kind:String}");
    params.kind = kind;
  }
  const status = nonEmpty(filters.status);
  if (status && (SE_INFO_CORRECTION_STATUSES as readonly string[]).includes(status)) {
    where.push(`(${CORRECTION_STATUS_EXPR}) = {status:String}`);
    params.status = status;
  }
  return { where, params };
}

export async function listSeCompanyInfoCorrectionsPage(
  query: SeCompanyInfoCorrectionListQuery,
): Promise<SeCompanyInfoCorrectionListPage> {
  const { where, params } = buildCorrectionsListFilter(query);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const { limit, offset } = pageParams(query);

  const [rows, counted] = await Promise.all([
    chQuery<SeCompanyInfoCorrectionListRow>(
      `${CORRECTIONS_LIST_SELECT_SQL}
${filter}
ORDER BY c.created_at DESC, c.correction_id DESC
${PAGE_LIMIT_OFFSET_SQL}`,
      { ...params, limit, offset },
    ),
    chQuery<{ total: string }>(`${CORRECTIONS_LIST_COUNT_SQL}\n${filter}`, params),
  ]);
  return { rows, total: Number(counted[0]?.total ?? 0) };
}
