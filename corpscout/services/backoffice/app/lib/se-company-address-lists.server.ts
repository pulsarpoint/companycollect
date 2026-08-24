/**
 * The `/admin/se/company-address/corrections` list: every row of
 * `corpscout.se_company_address_correction`, with the same four statuses the
 * Address tab computes for one company. Mirrors se-company-info-lists.server.ts
 * clause for clause -- explicit column aliasing, hash/UUID columns wrapped in
 * toString(), a dynamically built WHERE, and its PAGE_LIMIT_OFFSET_SQL, which is
 * imported rather than re-spelled so the two lists can never page differently.
 *
 * The published side is joined AGGREGATED PER COMPANY, which is the one real
 * difference from the info ledger: a company has several address rows, so
 * "applied" and "stale" are answered against sets rather than against a single
 * row. Applied ids come from every row (a reject's id lives on the tombstone it
 * wrote); the key and hash sets come from the live rows only.
 */
import { chQuery } from "~/lib/clickhouse.server";
import type { SortDir } from "~/lib/countries";
import { clampPage, clampPageSize } from "~/lib/paging";
import {
  SE_ADDRESS_CORRECTION_KINDS,
  SE_ADDRESS_CORRECTION_STATUSES,
  ZERO_EVIDENCE_HASH,
  type SeAddressCorrectionStatus,
} from "~/lib/se-address-corrections";
import {
  ANY_FILTER_VALUE,
  NONE_FILTER_VALUE,
} from "~/lib/se-company-info-filters";
import {
  FILTER_OPTIONS_TTL_MS,
  PAGE_LIMIT_OFFSET_SQL,
} from "~/lib/se-company-info-lists.server";

export type { SeAddressCorrectionStatus };

function nonEmpty(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed === "" ? null : trimmed;
}

/** A data-driven discrete filter's value: absent when unset or when the select
 * says "Any", and the empty string when it says "none". */
function discreteValue(value: string | undefined): string | null {
  const trimmed = nonEmpty(value);
  if (trimmed === null || trimmed === ANY_FILTER_VALUE) return null;
  return trimmed === NONE_FILTER_VALUE ? "" : trimmed;
}

/* -------------------------------------------------------------------- */
/* Server-side sorting and paging                                        */
/* -------------------------------------------------------------------- */

interface SortTerm {
  expr: string;
  dir: SortDir;
}

/** ORDER BY for one whitelisted column plus this list's stable tiebreak, with
 * any tiebreak that IS the sorted column dropped (so the default sort reads
 * `ORDER BY c.created_at DESC, c.correction_id DESC`, not the same column
 * twice). Kept here rather than imported: it is four lines of pure text
 * assembly, and the info list's copy is free to change with its own columns. */
function orderBySql(primary: SortTerm, tiebreaks: readonly SortTerm[]): string {
  const terms = [primary, ...tiebreaks.filter((term) => term.expr !== primary.expr)];
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

/* -------------------------------------------------------------------- */
/* The ledger list                                                       */
/* -------------------------------------------------------------------- */

export interface SeCompanyAddressCorrectionListRow {
  correction_id: string;
  company_id: string;
  created_at: string;
  correction_kind: string;
  /** The address this correction decides, lifted out of the payload -- the one
   * column the info ledger has no equivalent of. "" on an undo (and on a
   * malformed row that named none). */
  address_key: string;
  payload: string;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  status: SeAddressCorrectionStatus;
}

export interface SeCompanyAddressCorrectionListFilters {
  companyId?: string;
  kind?: string;
  status?: string;
  decidedBy?: string;
}

export interface SeCompanyAddressCorrectionListQuery
  extends SeCompanyAddressCorrectionListFilters {
  page: number;
  pageSize: number;
  sort?: string;
  dir?: string;
}

export interface SeCompanyAddressCorrectionListPage {
  rows: SeCompanyAddressCorrectionListRow[];
  total: number;
}

export interface SeCompanyAddressCorrectionFilterOptions {
  decidedBy: string[];
}

/** The subject of every non-undo correction. One expression, used by the SELECT
 * list, the status branches and the sort column, so they can never read the
 * payload differently. */
const ADDRESS_KEY_EXPR = "JSONExtractString(c.payload, 'address_key')";

/** A later undo's supersedes_correction_id names the id it cancels, across
 * every company -- unlike se-company-address.server.ts's per-company CTE, since
 * this list is not scoped to one. */
export const ADDRESS_UNDONE_CTE_SQL = `WITH undone AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_address_correction
  WHERE supersedes_correction_id IS NOT NULL
)`;

/**
 * The published side, one row per company: the ids Dagster stamped on ANY of
 * that company's address rows, and the key/evidence sets of its LIVE ones.
 *
 * Scoped to the companies actually present in the ledger before FINAL collapses
 * anything, for the same reason the info list scopes its own join: an unscoped
 * `FROM corpscout.se_company_address FINAL` re-merges the whole table to
 * decorate a handful of ledger rows. Every projected column is an Array, which
 * a LEFT JOIN miss fills with [] -- a company whose corrections have no
 * published row at all reads as "nothing applied, nothing live", which is the
 * truth.
 */
export const ADDRESS_SCOPED_PUBLISHED_JOIN_SQL = `LEFT JOIN (
  SELECT
    company_id,
    groupUniqArrayArray(arrayMap(x -> toString(x), correction_ids)) AS applied_correction_ids,
    groupUniqArrayIf(toString(address_key), is_current) AS live_address_keys,
    groupUniqArrayIf(toString(evidence_set_hash), is_current) AS live_evidence_hashes
  FROM corpscout.se_company_address FINAL
  WHERE company_id IN (SELECT company_id FROM corpscout.se_company_address_correction)
  GROUP BY company_id
) AS p ON p.company_id = c.company_id`;

/**
 * Status precedence mirrors the Address tab's own (see
 * se-company-address.server.ts): an undo always wins; then applied, which has
 * the second branch ruling A11 forces -- a `reject_address` naming a key the
 * resolution did not produce has NO row for Dagster to stamp, so the absence of
 * its key from the live set IS the applied signal; then stale, a live,
 * not-applied correction whose evidence matches no live row of the company (the
 * zero hash is undo's own marker and is never stale); else pending.
 *
 * Branch ORDER matters: multiIf returns the first match, and applied must
 * precede stale so a decision that landed is never reported as waiting.
 */
export const ADDRESS_CORRECTION_STATUS_EXPR = `multiIf(
    c.correction_id IN (SELECT id FROM undone), 'undone',
    has(p.applied_correction_ids, toString(c.correction_id))
      OR (
        c.correction_kind = 'reject_address'
        AND ${ADDRESS_KEY_EXPR} != ''
        AND NOT has(p.live_address_keys, ${ADDRESS_KEY_EXPR})
      ), 'applied',
    toString(c.evidence_hash) != {zeroHash:String}
      AND NOT has(p.live_evidence_hashes, toString(c.evidence_hash)), 'stale',
    'pending'
  )`;

export const ADDRESS_CORRECTION_LIST_SQL = `${ADDRESS_UNDONE_CTE_SQL}
SELECT
  toString(c.correction_id) AS correction_id,
  c.company_id AS company_id,
  toString(c.created_at) AS created_at,
  c.correction_kind AS correction_kind,
  ${ADDRESS_KEY_EXPR} AS address_key,
  c.payload AS payload,
  c.reason AS reason,
  c.decided_by AS decided_by,
  toString(c.supersedes_correction_id) AS supersedes_correction_id,
  ${ADDRESS_CORRECTION_STATUS_EXPR} AS status
FROM corpscout.se_company_address_correction AS c
${ADDRESS_SCOPED_PUBLISHED_JOIN_SQL}`;

/** The paging total for the same filters. Kept as its own statement (rather
 * than a window function on the row query) so the count is exact whatever page
 * is being read. */
export const ADDRESS_CORRECTION_COUNTS_SQL = `${ADDRESS_UNDONE_CTE_SQL}
SELECT toString(count()) AS total
FROM corpscout.se_company_address_correction AS c
${ADDRESS_SCOPED_PUBLISHED_JOIN_SQL}`;

/** The ledger's only data-driven discrete column. No FINAL --
 * se_company_address_correction is an append-only MergeTree, not Replacing. */
export const ADDRESS_CORRECTION_FILTER_OPTIONS_SQL = `SELECT
  arraySort(groupUniqArray(c.decided_by)) AS decided_by
FROM corpscout.se_company_address_correction AS c`;

/**
 * The status filter reuses ADDRESS_CORRECTION_STATUS_EXPR verbatim as a WHERE
 * predicate rather than naming the SELECT alias -- ClickHouse does not
 * guarantee an alias is visible to WHERE at the same query level. zeroHash is
 * always included, because the status expression sits in the SELECT list of
 * every query built from ADDRESS_CORRECTION_LIST_SQL whatever is filtered.
 *
 * `kind` and `status` are whitelisted against the ADDRESS ledger's own enums
 * (an `approve_suggestion` means nothing here), so an unrecognized value --
 * including the filter form's "any" sentinel -- is treated as absent rather
 * than filtering on literal garbage.
 */
export function buildAddressCorrectionsListFilter(
  filters: SeCompanyAddressCorrectionListFilters,
): { where: string[]; params: Record<string, unknown> } {
  const where: string[] = [];
  const params: Record<string, unknown> = { zeroHash: ZERO_EVIDENCE_HASH };

  const companyId = nonEmpty(filters.companyId);
  if (companyId) {
    where.push("c.company_id = {companyId:String}");
    params.companyId = companyId;
  }
  const kind = nonEmpty(filters.kind);
  if (kind && (SE_ADDRESS_CORRECTION_KINDS as readonly string[]).includes(kind)) {
    where.push("c.correction_kind = {kind:String}");
    params.kind = kind;
  }
  const status = nonEmpty(filters.status);
  if (status && (SE_ADDRESS_CORRECTION_STATUSES as readonly string[]).includes(status)) {
    where.push(`(${ADDRESS_CORRECTION_STATUS_EXPR}) = {status:String}`);
    params.status = status;
  }
  // decided_by has no enum to whitelist against (it is whoever wrote the row),
  // so its options come from the ledger itself and the chosen one travels as a
  // named param.
  const decidedBy = discreteValue(filters.decidedBy);
  if (decidedBy !== null) {
    where.push("c.decided_by = {decidedBy:String}");
    params.decidedBy = decidedBy;
  }
  return { where, params };
}

/**
 * Every column a header may sort by. The two computed ones sort by their own
 * expression rather than by the SELECT alias, for the same reason the WHERE
 * clause repeats it.
 */
export const ADDRESS_CORRECTION_SORT_COLUMNS = {
  created_at: "c.created_at",
  company_id: "c.company_id",
  correction_id: "c.correction_id",
  correction_kind: "c.correction_kind",
  address_key: ADDRESS_KEY_EXPR,
  payload: "c.payload",
  reason: "c.reason",
  decided_by: "c.decided_by",
  status: `(${ADDRESS_CORRECTION_STATUS_EXPR})`,
} as const;

export type SeCompanyAddressCorrectionSortKey =
  keyof typeof ADDRESS_CORRECTION_SORT_COLUMNS;

export const DEFAULT_ADDRESS_CORRECTION_SORT: SeCompanyAddressCorrectionSortKey =
  "created_at";
export const DEFAULT_ADDRESS_CORRECTION_DIR: SortDir = "desc";

export function resolveCorrectionsSort(
  sort: string | undefined,
  dir: string | undefined,
): { sort: SeCompanyAddressCorrectionSortKey; dir: SortDir } {
  return {
    sort: resolveSortKey(
      ADDRESS_CORRECTION_SORT_COLUMNS,
      sort,
      DEFAULT_ADDRESS_CORRECTION_SORT,
    ),
    dir: resolveDir(dir, DEFAULT_ADDRESS_CORRECTION_DIR),
  };
}

/** (created_at DESC, correction_id DESC) stays the tiebreak -- and, when
 * nothing else is chosen, IS the sort. */
export function correctionsOrderBySql(
  sort: SeCompanyAddressCorrectionSortKey,
  dir: SortDir,
): string {
  return orderBySql({ expr: ADDRESS_CORRECTION_SORT_COLUMNS[sort], dir }, [
    { expr: ADDRESS_CORRECTION_SORT_COLUMNS.created_at, dir: "desc" },
    { expr: ADDRESS_CORRECTION_SORT_COLUMNS.correction_id, dir: "desc" },
  ]);
}

export async function listSeCompanyAddressCorrectionsPage(
  query: SeCompanyAddressCorrectionListQuery,
): Promise<SeCompanyAddressCorrectionListPage> {
  const { where, params } = buildAddressCorrectionsListFilter(query);
  const filter = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const { limit, offset } = pageParams(query);
  const sort = resolveCorrectionsSort(query.sort, query.dir);

  const [rows, counted] = await Promise.all([
    chQuery<SeCompanyAddressCorrectionListRow>(
      `${ADDRESS_CORRECTION_LIST_SQL}
${filter}
${correctionsOrderBySql(sort.sort, sort.dir)}
${PAGE_LIMIT_OFFSET_SQL}`,
      { ...params, limit, offset },
    ),
    chQuery<{ total: string }>(`${ADDRESS_CORRECTION_COUNTS_SQL}\n${filter}`, params),
  ]);
  return { rows, total: Number(counted[0]?.total ?? 0) };
}

interface OptionsCacheEntry<T> {
  value: T;
  at: number;
}

let correctionOptionsCache: OptionsCacheEntry<SeCompanyAddressCorrectionFilterOptions> | null =
  null;

/** Drops the cached option list. Used by tests; also the one lever if a
 * reviewer needs it refreshed before the TTL expires. */
export function resetSeCompanyAddressCorrectionFilterOptionsCache(): void {
  correctionOptionsCache = null;
}

export async function loadSeCompanyAddressCorrectionFilterOptions(): Promise<SeCompanyAddressCorrectionFilterOptions> {
  if (
    correctionOptionsCache !== null &&
    Date.now() - correctionOptionsCache.at < FILTER_OPTIONS_TTL_MS
  ) {
    return correctionOptionsCache.value;
  }
  const [row] = await chQuery<{ decided_by: string[] }>(
    ADDRESS_CORRECTION_FILTER_OPTIONS_SQL,
  );
  const value: SeCompanyAddressCorrectionFilterOptions = {
    decidedBy: row?.decided_by ?? [],
  };
  correctionOptionsCache = { value, at: Date.now() };
  return value;
}
