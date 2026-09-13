/**
 * The Address tab's window onto the SE address entity (spec 2026-09-06,
 * sections 3.1 to 3.6, 5.5 and 8): one read over the six tables into a single
 * detail object, the reviewer's five writes, and the targeted fold launch.
 *
 * The shape follows `se-basic-info.server.ts`. Every read collapses a nullable
 * column to `''` so a component never has to tell `""` from `null`, reads a
 * `FixedString(64)` key through `toString` and takes the current version
 * through `FINAL`; stamps and dates arrive as ClickHouse's own strings. Every
 * write is append-only: one new version row per action, stamped with a single
 * `clickhouseStamp(now)` that also seeds the row's `suggestion_id`, carrying
 * `decided_by = 'backoffice'`, `source_run_id = 'backoffice'` and
 * `extractor_version = 'backoffice-v1'`. Nothing here touches a published row:
 * the next fold applies what the reviewer decided.
 */
import { createHash } from "node:crypto";

import {
  chInsertSeCompanyAddressRules,
  chInsertSeCompanyAddressSuggestions,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  ASSET_JOB_NAME,
  dagsterRunUrl,
  launchRun,
  SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET,
} from "~/lib/dagster.server";
import type { SeAddressDecision } from "~/lib/se-address-decision-form";
import { addressFoldPending, isAddressKey, WORKPLACE_PAGE_SIZE } from "~/lib/se-address-fields";
import { SE_COMPANY_ADDRESS_TABLE } from "~/lib/se-address-tables";
import { clickhouseStamp } from "~/lib/se-basic-info.server";

/** The eight normalized components a published row and a normalized row share. */
export interface SeAddressComponents {
  care_of: string;
  box: string;
  street_name: string;
  house_number: string;
  unit: string;
  postal_code: string;
  city: string;
  country_code: string;
}

/** One published address (`se_company_address`, 31 columns). `sources`,
 * `slots` and `normalized_ids` are index-parallel -- one entry per member;
 * `kinds` is the DISTINCT member kinds and is never zipped with them. */
export interface SeAddressRow extends SeAddressComponents {
  company_id: string;
  address_key: string;
  normalized_address: string;
  kinds: string[];
  sources: string[];
  slots: string[];
  normalized_ids: string[];
  text_source: string;
  active: number;
  inactive_reason: string;
  latitude: number | null;
  longitude: number | null;
  geocode_status: string;
  geocode_method: string;
  geocode_confidence: number | null;
  geocode_precision: string;
  geocode_policy: string;
  geocode_reference: string;
  /** `''` when the row was never geocoded. */
  geocoded_at: string;
  normalizer_version: string;
  /** `YYYY-MM-DD HH:MM:SS.mmm` UTC. */
  folded_at: string;
  fold_version: string;
  source_run_id: string;
}

/** A published row as it was at some fold (`se_company_address_history`). */
export type SeAddressHistoryRow = SeAddressRow;

/** One normalized suggestion (`se_company_address_normalized`, 21 columns). */
export interface SeAddressNormalizedRow extends SeAddressComponents {
  company_id: string;
  source: string;
  slot: string;
  normalized_id: string;
  suggestion_id: string;
  suggested_at: string;
  kind: string;
  normalized_address: string;
  address_key: string;
  parse_status: string;
  parse_notes: string;
  normalizer_version: string;
  normalized_at: string;
}

/** One raw suggestion (`se_company_address_suggestion`, 20 columns) -- what the
 * source (or the reviewer) delivered, never normalized. */
export interface SeAddressRawRow {
  company_id: string;
  source: string;
  slot: string;
  suggestion_id: string;
  source_record_uid: string;
  observed_at: string;
  kind: string;
  raw_address: string;
  care_of: string;
  street_address: string;
  postal_code: string;
  post_town: string;
  county: string;
  country_code: string;
  decided_by: string;
  note: string;
  /** `''` when this row replaces no published address. */
  replaces_key: string;
  suggested_at: string;
  source_run_id: string;
  extractor_version: string;
}

/** One version of a per-company address decision (`se_company_address_rule`);
 * `removed = 1` releases the rule. */
export interface SeAddressRuleRow {
  company_id: string;
  address_key: string;
  action: string;
  removed: number;
  decided_by: string;
  note: string;
  decided_at: string;
}

/** One contributing source of a published address, resolved through the row's
 * `normalized_ids` (spec 5.4's lineage). */
export interface SeAddressMember {
  source: string;
  slot: string;
  /** The normalized version this published row was folded from. */
  normalizedId: string;
  /** That slot's CURRENT normalized version, `null` when it has none. */
  current: SeAddressNormalizedRow | null;
  raw: SeAddressRawRow | null;
  /** The current normalized version is not the one the row was folded from. */
  refoldPending: boolean;
  /** How many of the seven components the current version fills. */
  completeness: number;
}

/** A reviewer draft: a `reviewer_draft` raw row that still holds an address. */
export interface SeAddressDraft {
  slot: string;
  raw: SeAddressRawRow;
  /** Present once a normalize run (or Fold now) has parsed the draft. */
  normalized: SeAddressNormalizedRow | null;
  /** The published key this draft corrects, `''` for a plain Add. */
  replacesKey: string;
}

/**
 * One row of a list -- the Addresses card or the Workplaces card -- WITHOUT its
 * members (spec 8, amended 2026-09-13, rule 1). A kommun publishes 1,503 of
 * these; shipping each one's normalized and raw rows is what made the page 7.8
 * MB. The panel asks for the selected row's members on its own.
 */
export interface SeAddressListEntry {
  row: SeAddressRow;
  /** Some member's slot has a current normalized version that is not the one
   * this row was folded from -- computed in ClickHouse, per row. */
  refoldPending: boolean;
  /** The hide rule in force for this key, `null` when there is none. */
  hideRule: SeAddressRuleRow | null;
}

/** The `?address=<key>` row with everything the right-hand panel shows. */
export interface SeAddressPublishedDetail {
  row: SeAddressRow;
  members: SeAddressMember[];
  /** Why the published text came from `row.text_source` (spec 5.2's sort). */
  textSourceReason: "most complete" | "tie-break" | "single source";
  /** The hide rule in force for this key, `null` when there is none. */
  hideRule: SeAddressRuleRow | null;
}

/** One page of the company's workplace-only addresses (spec 8, rule 2). */
export interface SeAddressWorkplacePage {
  rows: SeAddressListEntry[];
  /** Matching rows in the whole company, not on this page. */
  total: number;
  /** 1-based, as asked for -- echoed back so the card can build its links. */
  page: number;
  pageSize: number;
  /** The filter in force, `''` when there is none. */
  query: string;
}

/** What the tab's URL asks the loader for. */
export interface SeAddressDetailOptions {
  /** The `?address=` key; a malformed one is treated as no selection. */
  selectedKey: string | null;
  /** `?workplaces=`, 1-based. */
  workplacePage: number;
  /** `?workplace_q=`, already trimmed and capped. */
  workplaceQuery: string;
}

export interface SeAddressDetail {
  /** The company's own addresses: every row except an ACTIVE workplace-only one. */
  published: SeAddressListEntry[];
  /** The row the panel describes, with its members: the `?address=` row when
   * the key names one of this company's, else the FIRST ACTIVE row of
   * `published` (owner ruling 2026-09-13 -- the tab keeps the default it has
   * always had). Null only when the company has no active company address at
   * all: a workplace-only company, or one whose every address is withdrawn. */
  selected: SeAddressPublishedDetail | null;
  workplaces: SeAddressWorkplacePage;
  drafts: SeAddressDraft[];
  history: SeAddressHistoryRow[];
  /** Every current rule version of this company, released ones included. */
  rules: SeAddressRuleRow[];
  foldPending: boolean;
}

/** A list row as ClickHouse returns it: the published columns plus the flag. */
interface SeAddressListRow extends SeAddressRow {
  refold_pending: number;
}

/** The one row `ADDRESS_FOLD_STATE_SQL` returns. Stamps are `''` when the
 * aggregate had no row to take a maximum of. */
interface SeAddressFoldStateRow {
  folded_at: string;
  newest_normalized_at: string;
  has_publishable: number;
  newest_suggested_at: string;
  normalized_rows: number;
}

const COMPONENTS_SQL = (a: string) => `  ifNull(${a}.care_of, '') AS care_of, ifNull(${a}.box, '') AS box, ifNull(${a}.street_name, '') AS street_name,
  ifNull(${a}.house_number, '') AS house_number, ifNull(${a}.unit, '') AS unit, ifNull(${a}.postal_code, '') AS postal_code,
  ifNull(${a}.city, '') AS city, toString(${a}.country_code) AS country_code`;

const MAIN_COLUMNS_SQL = (a: string) => `  ${a}.company_id AS company_id, toString(${a}.address_key) AS address_key,
${COMPONENTS_SQL(a)},
  ${a}.normalized_address AS normalized_address,
  arrayMap(x -> toString(x), ${a}.kinds) AS kinds, arrayMap(x -> toString(x), ${a}.sources) AS sources, ${a}.slots AS slots,
  arrayMap(x -> toString(x), ${a}.normalized_ids) AS normalized_ids, toString(${a}.text_source) AS text_source,
  toUInt8(${a}.active) AS active, toString(${a}.inactive_reason) AS inactive_reason,
  ${a}.latitude AS latitude, ${a}.longitude AS longitude, toString(${a}.geocode_status) AS geocode_status,
  toString(${a}.geocode_method) AS geocode_method, ${a}.geocode_confidence AS geocode_confidence,
  toString(${a}.geocode_precision) AS geocode_precision, toString(${a}.geocode_policy) AS geocode_policy,
  ${a}.geocode_reference AS geocode_reference, ifNull(toString(${a}.geocoded_at), '') AS geocoded_at,
  toString(${a}.normalizer_version) AS normalizer_version, toString(${a}.folded_at) AS folded_at,
  toString(${a}.fold_version) AS fold_version, ${a}.source_run_id AS source_run_id`;

export const ADDRESS_MAIN_SQL = `SELECT
${MAIN_COLUMNS_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String}
ORDER BY m.active DESC, m.inactive_reason, m.normalized_address`;

/** History is append-only: no FINAL, newest first, capped like the Info tab's. */
export const ADDRESS_HISTORY_SQL = `SELECT
${MAIN_COLUMNS_SQL("h")}
FROM corpscout.se_company_address_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.folded_at DESC
LIMIT 200`;

const NORMALIZED_COLUMNS_SQL = `  n.company_id AS company_id, toString(n.source) AS source, n.slot AS slot, toString(n.normalized_id) AS normalized_id,
  toString(n.suggestion_id) AS suggestion_id, toString(n.suggested_at) AS suggested_at, toString(n.kind) AS kind,
${COMPONENTS_SQL("n")},
  n.normalized_address AS normalized_address, toString(n.address_key) AS address_key, toString(n.parse_status) AS parse_status,
  n.parse_notes AS parse_notes, toString(n.normalizer_version) AS normalizer_version, toString(n.normalized_at) AS normalized_at`;

export const ADDRESS_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
ORDER BY n.source, n.slot`;

const RAW_COLUMNS_SQL = `  s.company_id AS company_id, toString(s.source) AS source, s.slot AS slot, toString(s.suggestion_id) AS suggestion_id,
  s.source_record_uid AS source_record_uid, toString(s.observed_at) AS observed_at, toString(s.kind) AS kind,
  ifNull(s.raw_address, '') AS raw_address, ifNull(s.care_of, '') AS care_of, ifNull(s.street_address, '') AS street_address,
  ifNull(s.postal_code, '') AS postal_code, ifNull(s.post_town, '') AS post_town, ifNull(s.county, '') AS county,
  ifNull(s.country_code, '') AS country_code, ifNull(s.decided_by, '') AS decided_by, ifNull(s.note, '') AS note,
  ifNull(toString(s.replaces_key), '') AS replaces_key, toString(s.suggested_at) AS suggested_at,
  s.source_run_id AS source_run_id, toString(s.extractor_version) AS extractor_version`;

export const ADDRESS_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source, s.slot`;

export const ADDRESS_RULES_SQL = `SELECT
  r.company_id AS company_id, toString(r.address_key) AS address_key, toString(r.action) AS action, toUInt8(r.removed) AS removed,
  toString(r.decided_by) AS decided_by, r.note AS note, toString(r.decided_at) AS decided_at
FROM corpscout.se_company_address_rule AS r FINAL
WHERE r.company_id = {companyId:String}
ORDER BY r.decided_at DESC`;

/**
 * The company's CURRENT normalized versions as two arrays of join keys: one of
 * `source \n slot`, one of `source \n slot \n normalized_id`. A published row is
 * re-fold pending when one of its members' slots HAS a current version (the
 * first array) that is not the version the row was folded from (the second) --
 * exactly what the detail used to decide in TypeScript after reading every
 * normalized row of the company.
 *
 * Concatenated keys rather than tuples: `concat` over a `LowCardinality(String)`
 * column yields a plain `String`, so `has` never has to reconcile a
 * LowCardinality element type with a String one. No source (a catalogue value)
 * and no slot (`''`, `company`, `est:<id>`, `r<17 digits>`) holds a newline, so
 * the keys cannot collide.
 */
const CURRENT_PAIRS_SQL = `  (SELECT groupArray(concat(toString(n.source), '\\n', n.slot))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS current_pairs`;

const CURRENT_TRIPLES_SQL = `  (SELECT groupArray(concat(toString(n.source), '\\n', n.slot, '\\n', toString(n.normalized_id)))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS current_triples`;

/** `sources`, `slots` and `normalized_ids` are index-parallel, so the flag is
 * an `arrayExists` over the row's own member indexes. An index past the end of
 * a shorter array yields that type's default, which matches no key. */
const REFOLD_PENDING_SQL = (a: string) => `  toUInt8(arrayExists(
    i -> has(current_pairs, concat(toString(${a}.sources[i]), '\\n', ${a}.slots[i]))
      AND NOT has(current_triples, concat(toString(${a}.sources[i]), '\\n', ${a}.slots[i], '\\n', toString(${a}.normalized_ids[i]))),
    range(1, length(${a}.sources) + 1))) AS refold_pending`;

/**
 * The Addresses card's rows: everything this company has published EXCEPT an
 * active row whose `kinds` are exactly `['workplace']` -- the split migration
 * 000403 gave the serving view (`a.kinds = ['workplace']`). A merged row that
 * carries `workplace` beside another kind is the company's address and stays.
 * A withdrawn or hidden workplace row stays too, in the collapsed group, so a
 * reviewer can still see what a Remove did. No members: the panel loads those
 * for the selected row alone. `active DESC` first, so the first entry of this
 * list is also the row the panel falls back to with no `?address=`.
 */
export const ADDRESS_LIST_SQL = `WITH
${CURRENT_PAIRS_SQL},
${CURRENT_TRIPLES_SQL}
SELECT
${MAIN_COLUMNS_SQL("m")},
${REFOLD_PENDING_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String}
  AND NOT (m.active = 1 AND m.kinds = ['workplace'])
ORDER BY m.active DESC, m.inactive_reason, m.normalized_address`;

/** The other half of the split, filtered. `positionCaseInsensitiveUTF8` takes
 * its needle as a LITERAL, so `%` and `_` are ordinary characters and there is
 * nothing to escape -- an `ilike` would have to escape both before binding. */
const WORKPLACE_WHERE_SQL = `WHERE m.company_id = {companyId:String}
  AND m.active = 1
  AND m.kinds = ['workplace']
  AND ({workplaceQuery:String} = '' OR positionCaseInsensitiveUTF8(m.normalized_address, {workplaceQuery:String}) > 0)`;

export const ADDRESS_WORKPLACES_SQL = `WITH
${CURRENT_PAIRS_SQL},
${CURRENT_TRIPLES_SQL}
SELECT
${MAIN_COLUMNS_SQL("m")},
${REFOLD_PENDING_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
${WORKPLACE_WHERE_SQL}
ORDER BY m.normalized_address, m.address_key
LIMIT {limit:UInt32} OFFSET {offset:UInt32}`;

/** `toUInt32`, not a bare `count()`: ClickHouse quotes 64-bit integers in
 * JSONEachRow, so an unwrapped count would reach the page as the string
 * "1502" and every arithmetic on it would be string arithmetic. */
export const ADDRESS_WORKPLACES_COUNT_SQL = `SELECT toUInt32(count()) AS total
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
${WORKPLACE_WHERE_SQL}`;

/** The selected row itself, whichever card it belongs to. `toString` on the key
 * rather than a FixedString comparison: `company_id` has already narrowed the
 * read to this company's few rows, so nothing is lost by not matching the key
 * against the index. */
export const ADDRESS_SELECTED_SQL = `SELECT
${MAIN_COLUMNS_SQL("m")}
FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String} AND toString(m.address_key) = {addressKey:String}
LIMIT 1`;

/** The selected row's members, by the `(source, slot)` pairs the row carries:
 * two parallel `Array(String)` parameters zipped back into pairs in ClickHouse.
 * `CAST(... AS String)` drops the column's LowCardinality wrapper so both sides
 * of `has` are the same tuple type. */
const MEMBER_PAIRS_PREDICATE_SQL = (a: string) =>
  `  AND has(arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)}), (CAST(${a}.source AS String), ${a}.slot))`;

export const ADDRESS_MEMBER_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
${MEMBER_PAIRS_PREDICATE_SQL("n")}
ORDER BY n.source, n.slot`;

export const ADDRESS_MEMBER_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
${MEMBER_PAIRS_PREDICATE_SQL("s")}
ORDER BY s.source, s.slot`;

/** The reviewer's drafts, filtered in SQL rather than in the page: a company
 * has at most a handful of them, however many addresses it publishes. */
export const ADDRESS_DRAFT_RAW_SQL = `SELECT
${RAW_COLUMNS_SQL}
FROM corpscout.se_company_address_suggestion AS s FINAL
WHERE s.company_id = {companyId:String} AND s.source = 'reviewer_draft'
ORDER BY s.slot`;

export const ADDRESS_DRAFT_NORMALIZED_SQL = `SELECT
${NORMALIZED_COLUMNS_SQL}
FROM corpscout.se_company_address_normalized AS n FINAL
WHERE n.company_id = {companyId:String} AND n.source = 'reviewer_draft'
ORDER BY n.slot`;

/**
 * Everything "Fold pending" needs, in one row of five scalar aggregates, so the
 * page never has to read a company's normalized and raw rows to decide it: the
 * newest fold, the newest non-draft normalized stamp, whether any non-draft
 * normalized row is publishable (spec 5.5), the newest non-draft raw stamp (an
 * activated reviewer address or a Remove tombstone the normalize step has not
 * seen yet), and how many normalized rows the company has at all -- which is
 * what tells an empty tab from a company that exists at no layer.
 *
 * Drafts are excluded from the stamps because they are never folded (spec 5.5),
 * and each `max` is guarded by its own `count()`: a maximum over no rows is the
 * type's zero value, not NULL, and `1970-01-01` would read as a real stamp.
 */
export const ADDRESS_FOLD_STATE_SQL = `SELECT
  (SELECT if(count() = 0, '', toString(max(m.folded_at)))
   FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL
   WHERE m.company_id = {companyId:String}) AS folded_at,
  (SELECT if(count() = 0, '', toString(max(n.normalized_at)))
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String} AND n.source != 'reviewer_draft') AS newest_normalized_at,
  (SELECT toUInt8(countIf(n.parse_status IN ('ok', 'partial', 'foreign')) > 0)
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String} AND n.source != 'reviewer_draft') AS has_publishable,
  (SELECT if(count() = 0, '', toString(max(s.suggested_at)))
   FROM corpscout.se_company_address_suggestion AS s FINAL
   WHERE s.company_id = {companyId:String} AND s.source != 'reviewer_draft') AS newest_suggested_at,
  (SELECT toUInt32(count())
   FROM corpscout.se_company_address_normalized AS n FINAL
   WHERE n.company_id = {companyId:String}) AS normalized_rows`;

const DRAFT_SOURCE = "reviewer_draft";
const REVIEWER_SOURCE = "reviewer";
const HIDE_ACTION = "hide";
/** The seven components completeness counts; `country_code` is always set. */
const COMPONENT_FIELDS = [
  "care_of",
  "box",
  "street_name",
  "house_number",
  "unit",
  "postal_code",
  "city",
] as const;
/** A `reviewer_draft` row still holding one of these is a live draft; a row
 * with all four empty is the tombstone an Activate or a Discard left behind. */
const DRAFT_TEXT_FIELDS = ["street_address", "care_of", "postal_code", "post_town"] as const;
// Deleted -- the fold state answers `has_publishable` in ClickHouse now:
// const PUBLISHABLE_PARSE_STATUS = new Set(["ok", "partial", "foreign"]);

function slotKey(source: string, slot: string): string {
  return `${source}|${slot}`;
}

function completenessOf(row: SeAddressNormalizedRow | null): number {
  return row === null ? 0 : COMPONENT_FIELDS.filter((field) => row[field] !== "").length;
}

/** The hide rule in force for a key: the current version (FINAL already
 * collapsed the older ones) that has not been released. */
function activeHideRule(rules: readonly SeAddressRuleRow[], addressKey: string): SeAddressRuleRow | null {
  return (
    rules.find(
      (rule) => rule.address_key === addressKey && rule.action === HIDE_ACTION && rule.removed === 0,
    ) ?? null
  );
}

/**
 * Why the published text came from `row.text_source`. The fold sorts members by
 * completeness first (spec 5.2), so the text source is the most complete member
 * when it beats every other one outright; when it ties, precedence or recency
 * decided. A source can hold two slots (SCB and Bolagsverket both write slot
 * `''`, Ratsit `'company'`), so the attributed member is the most complete of
 * that source's members -- the one the fold would have sorted first.
 */
function textSourceReason(
  row: SeAddressRow,
  members: readonly SeAddressMember[],
): SeAddressPublishedDetail["textSourceReason"] {
  if (members.length === 1) return "single source";
  const fromTextSource = members.filter((member) => member.source === row.text_source);
  const chosen = fromTextSource.reduce<SeAddressMember | undefined>(
    (best, member) => (best === undefined || member.completeness > best.completeness ? member : best),
    undefined,
  );
  if (!chosen) return "tie-break";
  const outright = members.every(
    (member) => member === chosen || member.completeness < chosen.completeness,
  );
  return outright ? "most complete" : "tie-break";
}

const EMPTY_FOLD_STATE: SeAddressFoldStateRow = {
  folded_at: "",
  newest_normalized_at: "",
  has_publishable: 0,
  newest_suggested_at: "",
  normalized_rows: 0,
};

/** A list row as the page wants it: the published columns, the ClickHouse flag
 * as a boolean, and the rule in force for its key. */
function listEntry(
  queryRow: SeAddressListRow,
  rules: readonly SeAddressRuleRow[],
): SeAddressListEntry {
  const { refold_pending, ...row } = queryRow;
  return {
    row,
    refoldPending: refold_pending === 1,
    hideRule: activeHideRule(rules, row.address_key),
  };
}

/** One published row's `(source, slot)` pairs, as the two parallel array
 * parameters the member reads bind. */
function memberPairParams(
  companyId: string,
  row: SeAddressRow,
): { companyId: string; memberSources: string[]; memberSlots: string[] } {
  return {
    companyId,
    memberSources: [...row.sources],
    memberSlots: row.sources.map((_, index) => row.slots[index] ?? ""),
  };
}

/** The selected address's panel: two reads, both bound to that row's own pairs,
 * then the members assembled through `normalized_ids` exactly as before. */
async function loadSelectedDetail(
  companyId: string,
  row: SeAddressRow,
  rules: readonly SeAddressRuleRow[],
): Promise<SeAddressPublishedDetail> {
  const params = memberPairParams(companyId, row);
  const [normalizedRows, rawRows] = await Promise.all([
    chQuery<SeAddressNormalizedRow>(ADDRESS_MEMBER_NORMALIZED_SQL, params),
    chQuery<SeAddressRawRow>(ADDRESS_MEMBER_RAW_SQL, params),
  ]);
  const normalizedBySlot = new Map(normalizedRows.map((entry) => [slotKey(entry.source, entry.slot), entry]));
  const rawBySlot = new Map(rawRows.map((entry) => [slotKey(entry.source, entry.slot), entry]));
  const members = row.sources.map((source, index) => {
    const slot = row.slots[index] ?? "";
    const normalizedId = row.normalized_ids[index] ?? "";
    const current = normalizedBySlot.get(slotKey(source, slot)) ?? null;
    return {
      source,
      slot,
      normalizedId,
      current,
      raw: rawBySlot.get(slotKey(source, slot)) ?? null,
      refoldPending: current !== null && current.normalized_id !== normalizedId,
      completeness: completenessOf(current),
    };
  });
  return {
    row,
    members,
    textSourceReason: textSourceReason(row, members),
    hideRule: activeHideRule(rules, row.address_key),
  };
}

/**
 * The tab in two round trips (spec 8, amended 2026-09-13). The first runs nine
 * small reads in parallel: the Addresses list without members, one page of the
 * workplaces with its count, history, rules, the drafts, the fold state, and --
 * only when `?address=` carried a well-formed key -- that one row. The second
 * resolves the selected row's members from its own `(source, slot)` pairs: the
 * `?address=` row when it is this company's, else the first active row of the
 * list, which is already in hand and needs no read of its own (owner ruling
 * 2026-09-13: the panel keeps the default selection it has always had).
 *
 * Null when the company has no address at any layer -- no list row, no
 * workplace, no draft and no normalized row -- which the route turns into the
 * workspace's empty state.
 */
export async function loadSeAddressDetail(
  companyId: string,
  options: SeAddressDetailOptions,
): Promise<SeAddressDetail | null> {
  const { workplacePage, workplaceQuery } = options;
  // A hand-typed key never reaches ClickHouse: the route already filters one,
  // and this is the store's own guard.
  const selectedKey =
    options.selectedKey !== null && isAddressKey(options.selectedKey) ? options.selectedKey : null;
  const offset = (workplacePage - 1) * WORKPLACE_PAGE_SIZE;
  const [
    listRows,
    workplaceRows,
    workplaceCount,
    history,
    rules,
    draftRawRows,
    draftNormalizedRows,
    foldStateRows,
    selectedRows,
  ] = await Promise.all([
    chQuery<SeAddressListRow>(ADDRESS_LIST_SQL, { companyId }),
    chQuery<SeAddressListRow>(ADDRESS_WORKPLACES_SQL, {
      companyId,
      workplaceQuery,
      limit: WORKPLACE_PAGE_SIZE,
      offset,
    }),
    chQuery<{ total: number }>(ADDRESS_WORKPLACES_COUNT_SQL, { companyId, workplaceQuery }),
    chQuery<SeAddressHistoryRow>(ADDRESS_HISTORY_SQL, { companyId }),
    chQuery<SeAddressRuleRow>(ADDRESS_RULES_SQL, { companyId }),
    chQuery<SeAddressRawRow>(ADDRESS_DRAFT_RAW_SQL, { companyId }),
    chQuery<SeAddressNormalizedRow>(ADDRESS_DRAFT_NORMALIZED_SQL, { companyId }),
    chQuery<SeAddressFoldStateRow>(ADDRESS_FOLD_STATE_SQL, { companyId }),
    selectedKey === null
      ? Promise.resolve([] as SeAddressRow[])
      : chQuery<SeAddressRow>(ADDRESS_SELECTED_SQL, { companyId, addressKey: selectedKey }),
  ]);
  const published = listRows.map((row) => listEntry(row, rules));
  const workplaces: SeAddressWorkplacePage = {
    rows: workplaceRows.map((row) => listEntry(row, rules)),
    total: workplaceCount[0]?.total ?? 0,
    page: workplacePage,
    pageSize: WORKPLACE_PAGE_SIZE,
    query: workplaceQuery,
  };
  const draftNormalizedBySlot = new Map(draftNormalizedRows.map((row) => [row.slot, row]));
  const drafts = draftRawRows
    .filter((row) => DRAFT_TEXT_FIELDS.some((field) => row[field] !== ""))
    .map((raw) => ({
      slot: raw.slot,
      raw,
      normalized: draftNormalizedBySlot.get(raw.slot) ?? null,
      replacesKey: raw.replaces_key,
    }));
  const state = foldStateRows[0] ?? EMPTY_FOLD_STATE;
  if (
    published.length === 0 &&
    workplaces.total === 0 &&
    drafts.length === 0 &&
    state.normalized_rows === 0
  ) {
    return null;
  }
  // The `?address=` row, else the first active company address (the list is
  // sorted `active DESC` first). A key that is absent, malformed or not this
  // company's lands on the same fallback, and only a company with no active
  // company address at all -- workplace-only, or every address withdrawn --
  // leaves the panel with nothing to describe.
  const selectedRow =
    selectedRows[0] ?? published.find((entry) => entry.row.active === 1)?.row ?? null;
  return {
    published,
    selected: selectedRow === null ? null : await loadSelectedDetail(companyId, selectedRow, rules),
    workplaces,
    drafts,
    history,
    rules,
    foldPending: addressFoldPending(
      state.folded_at === "" ? null : state.folded_at,
      // A released rule counts too: the release is not applied until the fold.
      [state.newest_normalized_at, state.newest_suggested_at, ...rules.map((rule) => rule.decided_at)].filter(
        (stamp) => stamp !== "",
      ),
      state.has_publishable === 1,
    ),
  };
}

/** A reviewer decision this module refuses; the route renders the message. */
export class SeAddressDecisionError extends Error {}

/** The address columns a backoffice raw row carries a value in (`county` and
 * `raw_address` are always NULL from here). The four text columns are `null`,
 * never `''`; `country` is the reviewer's own (Task 2 validates it to `SE`) and
 * only reaches the row while the row holds an address. */
export interface SeAddressRawColumns {
  kind: string;
  country: string;
  care_of: string | null;
  street_address: string | null;
  postal_code: string | null;
  post_town: string | null;
}

/** One row for `chInsertSeCompanyAddressSuggestions`: the raw table's twenty
 * columns, every Nullable one typed `string | null` so a cleared value is NULL
 * rather than `''` (spec 3.1's "a source that stops delivering writes NULL"). */
export interface SeAddressRawInsertRow {
  company_id: string;
  source: string;
  slot: string;
  suggestion_id: string;
  source_record_uid: string;
  observed_at: string;
  kind: string;
  raw_address: string | null;
  care_of: string | null;
  street_address: string | null;
  postal_code: string | null;
  post_town: string | null;
  county: string | null;
  country_code: string | null;
  decided_by: string;
  note: string | null;
  replaces_key: string | null;
  suggested_at: string;
  source_run_id: string;
  extractor_version: string;
}

/** One row for `chInsertSeCompanyAddressRules`; `note` is a plain String on the
 * table (DEFAULT ''), so it is never null here. */
export interface SeAddressRuleInsertRow {
  company_id: string;
  address_key: string;
  action: string;
  removed: number;
  decided_by: string;
  note: string;
  decided_at: string;
}

function textOrNull(value: string): string | null {
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

/** Ruling 2: a reviewer address lives in slot `r<the stamp's 17 digits>`, so
 * the `reviewer` row and its `reviewer_draft` share one lineage. */
function stampSlot(stamp: string): string {
  return `r${stamp.replace(/\D/g, "")}`;
}

/**
 * The id the extractors compute in SQL for the same row
 * (`address/suggestions.py`): sha256 over company, source, slot and the stamp,
 * newline-joined, with the stamp spelled exactly as `suggested_at`.
 */
function suggestionId(companyId: string, source: string, slot: string, stamp: string): string {
  return createHash("sha256").update(`${companyId}\n${source}\n${slot}\n${stamp}`).digest("hex");
}

/** One version of a raw row written by the backoffice. `country_code` follows
 * the address: the reviewer's country while the row holds one, NULL once it is
 * cleared. */
function rawRowVersion(
  companyId: string,
  source: string,
  slot: string,
  stamp: string,
  columns: SeAddressRawColumns,
  note: string,
  replacesKey: string | null,
): SeAddressRawInsertRow {
  const holdsAddress =
    columns.care_of !== null ||
    columns.street_address !== null ||
    columns.postal_code !== null ||
    columns.post_town !== null;
  return {
    company_id: companyId,
    source,
    slot,
    suggestion_id: suggestionId(companyId, source, slot, stamp),
    source_record_uid: "",
    observed_at: stamp,
    // LowCardinality(String), not nullable: a cleared row keeps the kind it had.
    kind: columns.kind === "" ? "unknown" : columns.kind,
    raw_address: null,
    care_of: columns.care_of,
    street_address: columns.street_address,
    postal_code: columns.postal_code,
    post_town: columns.post_town,
    county: null,
    country_code: holdsAddress ? (textOrNull(columns.country) ?? "SE") : null,
    decided_by: "backoffice",
    note: textOrNull(note),
    replaces_key: replacesKey,
    suggested_at: stamp,
    source_run_id: "backoffice",
    extractor_version: "backoffice-v1",
  };
}

/** The tombstone version: every address column NULL, the kind carried over. */
function clearedRowVersion(
  companyId: string,
  source: string,
  slot: string,
  stamp: string,
  kind: string,
  note: string,
): SeAddressRawInsertRow {
  return rawRowVersion(
    companyId,
    source,
    slot,
    stamp,
    { kind, country: "", care_of: null, street_address: null, postal_code: null, post_town: null },
    note,
    null,
  );
}

function ruleVersion(
  companyId: string,
  addressKey: string,
  removed: number,
  note: string,
  stamp: string,
): SeAddressRuleInsertRow {
  return {
    company_id: companyId,
    address_key: addressKey,
    action: HIDE_ACTION,
    removed,
    decided_by: "backoffice",
    note,
    decided_at: stamp,
  };
}

function findDraft(rows: readonly SeAddressRawRow[], slot: string): SeAddressRawRow | undefined {
  return rows.find(
    (row) =>
      row.source === DRAFT_SOURCE &&
      row.slot === slot &&
      DRAFT_TEXT_FIELDS.some((field) => row[field] !== ""),
  );
}

/**
 * Add address / Correct / edit a draft: one `reviewer_draft` raw row version
 * under a new slot (Ruling 2) or the slot being edited. Never refuses -- a
 * draft is not published, so it may hold anything the validation accepted --
 * and never touches the `reviewer` row: Activate does that.
 */
export async function saveSeAddressDraft(
  companyId: string,
  decision: Extract<SeAddressDecision, { intent: "save-draft" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string; slot: string }> {
  const stamp = clickhouseStamp(now);
  const slot = decision.slot ?? stampSlot(stamp);
  const { input } = decision;
  await chInsertSeCompanyAddressSuggestions([
    rawRowVersion(
      companyId,
      DRAFT_SOURCE,
      slot,
      stamp,
      {
        kind: input.kind,
        country: input.country,
        care_of: textOrNull(input.careOf),
        street_address: textOrNull(input.streetLine),
        postal_code: textOrNull(input.postalCode),
        post_town: textOrNull(input.city),
      },
      input.note,
      decision.replacesKey,
    ),
  ]);
  return { decidedAt: stamp, slot };
}

/**
 * Activate: the draft becomes the company's `reviewer` address under the same
 * slot, and the draft is cleared, in ONE insert so a reader never sees the pair
 * half-applied. A Correct (the draft carries `replaces_key`) also hides the key
 * it replaces -- without that rule the source members would republish it --
 * unless the draft parses back to that very key: the reviewer only retyped the
 * address the sources already deliver, and hiding it would hide the reviewer's
 * own row too. An unparsed draft still gets the rule: the fold decides what it
 * becomes, and Reset to default is there if the reviewer wants it back.
 */
export async function activateSeAddressDraft(
  companyId: string,
  decision: Extract<SeAddressDecision, { intent: "activate" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const [rawRows, normalizedRows] = await Promise.all([
    chQuery<SeAddressRawRow>(ADDRESS_RAW_SQL, { companyId }),
    chQuery<SeAddressNormalizedRow>(ADDRESS_NORMALIZED_SQL, { companyId }),
  ]);
  const draft = findDraft(rawRows, decision.slot);
  if (!draft || draft.street_address === "") {
    throw new SeAddressDecisionError("No draft to activate.");
  }
  await chInsertSeCompanyAddressSuggestions([
    rawRowVersion(
      companyId,
      REVIEWER_SOURCE,
      decision.slot,
      stamp,
      {
        kind: draft.kind,
        country: draft.country_code,
        care_of: textOrNull(draft.care_of),
        street_address: textOrNull(draft.street_address),
        postal_code: textOrNull(draft.postal_code),
        post_town: textOrNull(draft.post_town),
      },
      decision.note,
      null,
    ),
    clearedRowVersion(companyId, DRAFT_SOURCE, decision.slot, stamp, draft.kind, "activated"),
  ]);
  const draftNormalized =
    normalizedRows.find((row) => row.source === DRAFT_SOURCE && row.slot === decision.slot) ?? null;
  const foldsBackToTheSameKey =
    draftNormalized !== null && draftNormalized.address_key === draft.replaces_key;
  if (draft.replaces_key !== "" && !foldsBackToTheSameKey) {
    await chInsertSeCompanyAddressRules([
      ruleVersion(companyId, draft.replaces_key, 0, "corrected by reviewer", stamp),
    ]);
  }
  return { decidedAt: stamp };
}

/** Discard: the draft's slot keeps its lineage but holds no address again. */
export async function discardSeAddressDraft(
  companyId: string,
  decision: Extract<SeAddressDecision, { intent: "discard" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const rawRows = await chQuery<SeAddressRawRow>(ADDRESS_RAW_SQL, { companyId });
  const draft = findDraft(rawRows, decision.slot);
  if (!draft) throw new SeAddressDecisionError("No draft to discard.");
  await chInsertSeCompanyAddressSuggestions([
    clearedRowVersion(companyId, DRAFT_SOURCE, decision.slot, stamp, draft.kind, "discarded"),
  ]);
  return { decidedAt: stamp };
}

/**
 * Remove (Ruling 1, amended): the published key is computed over the UNION of
 * the members' components (`fold.py::_published_from`), so retiring one member
 * of a mixed row would shrink that union, re-key the address and orphan the
 * hide rule -- the row would come back under a new key with the old one merely
 * withdrawn. So a row with any non-reviewer member is hidden by the rule alone,
 * every member left in place; only a reviewer-only row is removed by
 * tombstoning its slots, and then no rule is needed. The raw rows are read for
 * one reason: a tombstone must carry the slot's own `kind`, which the published
 * row's `kinds` (DISTINCT, not member-parallel) cannot give.
 */
export async function removeSeAddress(
  companyId: string,
  decision: Extract<SeAddressDecision, { intent: "remove" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const [mainRows, rules, rawRows] = await Promise.all([
    chQuery<SeAddressRow>(ADDRESS_MAIN_SQL, { companyId }),
    chQuery<SeAddressRuleRow>(ADDRESS_RULES_SQL, { companyId }),
    chQuery<SeAddressRawRow>(ADDRESS_RAW_SQL, { companyId }),
  ]);
  const row = isAddressKey(decision.addressKey)
    ? mainRows.find((candidate) => candidate.address_key === decision.addressKey)
    : undefined;
  if (!row) throw new SeAddressDecisionError("Unknown address.");
  // Hidden already, or hidden by a rule the next fold has yet to apply.
  if (row.inactive_reason === "hidden" || activeHideRule(rules, row.address_key) !== null) {
    throw new SeAddressDecisionError("Already hidden.");
  }
  if (row.sources.some((source) => source !== REVIEWER_SOURCE)) {
    await chInsertSeCompanyAddressRules([
      ruleVersion(
        companyId,
        row.address_key,
        0,
        decision.note === "" ? "removed by reviewer" : decision.note,
        stamp,
      ),
    ]);
    return { decidedAt: stamp };
  }
  const rawBySlot = new Map(rawRows.map((raw) => [slotKey(raw.source, raw.slot), raw]));
  await chInsertSeCompanyAddressSuggestions(
    row.slots.map((slot) =>
      clearedRowVersion(
        companyId,
        REVIEWER_SOURCE,
        slot,
        stamp,
        rawBySlot.get(slotKey(REVIEWER_SOURCE, slot))?.kind ?? "unknown",
        "removed by reviewer",
      ),
    ),
  );
  return { decidedAt: stamp };
}

/** Reset to default: the hide rule is released (`removed = 1`) and the address
 * comes back at the next fold. Refuses when no rule is in force. */
export async function resetSeAddress(
  companyId: string,
  decision: Extract<SeAddressDecision, { intent: "reset" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const rules = await chQuery<SeAddressRuleRow>(ADDRESS_RULES_SQL, { companyId });
  if (activeHideRule(rules, decision.addressKey) === null) {
    throw new SeAddressDecisionError("No rule to reset.");
  }
  const note = decision.note === "" ? "reset to default" : `reset to default: ${decision.note}`;
  await chInsertSeCompanyAddressRules([
    ruleVersion(companyId, decision.addressKey, 1, note, stamp),
  ]);
  return { decidedAt: stamp };
}

const FOLD_NOW_TAG = { "backoffice/address": "fold-now" } as const;

/** Fold now (spec 8): one run of the targeted fold -- which normalizes the
 * company's raw rows first, so a draft saved a moment ago parses -- for this
 * company alone. */
export async function launchSeAddressFold(
  companyId: string,
): Promise<{ runId: string; url: string | null }> {
  const run = await launchRun({
    job: ASSET_JOB_NAME,
    assetSelection: [SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET],
    runConfig: {
      ops: { [SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET]: { config: { company_ids: [companyId] } } },
    },
    tags: { ...FOLD_NOW_TAG },
  });
  return { runId: run.runId, url: dagsterRunUrl(run.runId) };
}
