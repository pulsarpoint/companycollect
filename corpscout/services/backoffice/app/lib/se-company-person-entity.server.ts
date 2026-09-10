/**
 * The People tab's window onto the SE person entity (spec 2026-09-09, sections 3.1 to
 * 3.6, 5 and 7): one read over the six tables into a single detail object, the
 * reviewer's seven writes, and the targeted fold launch.
 *
 * The shape follows `se-company-address-entity.server.ts`. Every read collapses a
 * nullable column to `''` so a component never has to tell `""` from `null`, reads a
 * `FixedString(64)` key through `toString` and takes the current version through
 * `FINAL` -- except the history table, which is append-only and never gets it. Stamps
 * and dates arrive as ClickHouse's own strings.
 *
 * Every write is append-only: one new version row per action, stamped with a single
 * `clickhouseStamp(now)` that also seeds the row's `suggestion_id` -- sha256 over
 * company, source, slot and that stamp, exactly the preimage `person/suggestions.py`
 * computes in SQL -- and a rule's `rule_id`. Nothing here touches a published row: the
 * next fold applies what the reviewer decided.
 *
 * Ruling 4: the suggestion table has no `decided_by`, `note` or `replaces_key` column,
 * so those three live inside the row's `data` object, which is otherwise the reviewer's
 * own. A `reviewer` row never carries `replaces_key`: the hide rule Activate writes is
 * what retires the person the correction replaces.
 */
import { createHash } from "node:crypto";

import {
  chInsertSeCompanyPersonRules,
  chInsertSeCompanyPersonSuggestions,
  chQuery,
} from "~/lib/clickhouse.server";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";
import {
  ASSET_JOB_NAME,
  dagsterRunUrl,
  launchRun,
  SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET,
} from "~/lib/dagster.server";
import { clickhouseStamp } from "~/lib/se-basic-info.server";
import type { SePersonDecision } from "~/lib/se-person-decision-form";
import {
  DRAFT_SOURCE,
  foldsIntoPerson,
  groupOfRowSlot,
  isPersonKey,
  normalizeSePersonName,
  personFoldPending,
  personGroupSlot,
  personRowSlot,
  RESERVED_DATA_KEYS,
  REVIEWER_SOURCE,
  type SePersonIdentity,
  type SePersonInput,
  type SePersonRoleInput,
  type SePersonRoleOption,
} from "~/lib/se-person-fields";
import { SE_COMPANY_PERSON_TABLE } from "~/lib/se-person-tables";

/* ------------------------------------------------------------------ */
/* The rows, as the six reads deliver them                              */
/* ------------------------------------------------------------------ */

/** One published person (`se_company_person_v2`, the 29 columns of
 * `tables.MAIN_COLUMNS`). `member_*` and `normalized_ids` are index-parallel -- one
 * entry per member; `sources` is the DISTINCT member sources and is never zipped with
 * them. `role_codes`, `role_years` and `role_sources` are a parallel triple of their
 * own (spec 5.4). */
export interface SePersonRow {
  company_id: string;
  person_key: string;
  display_name: string;
  first_name: string;
  last_name: string;
  /** `''` when no source delivered one. */
  birth_year: string;
  wikidata_id: string;
  sources: string[];
  slots: string[];
  normalized_ids: string[];
  member_sources: string[];
  member_slots: string[];
  member_names: string[];
  member_birth_years: string[];
  member_wikidata_ids: string[];
  member_data: string[];
  role_codes: string[];
  role_years: number[];
  role_sources: string[][];
  current_roles: string[];
  first_year: string;
  last_year: string;
  text_source: string;
  data: string;
  active: number;
  inactive_reason: string;
  /** `YYYY-MM-DD HH:MM:SS.mmm` UTC. */
  folded_at: string;
  fold_version: string;
  source_run_id: string;
}

/** A published row as it was at some fold (`se_company_person_history`, the main
 * table's 29 columns plus its own three). */
export interface SePersonHistoryRow extends SePersonRow {
  changed_at: string;
  change_kind: string;
  fold_run_id: string;
}

/** One normalized suggestion (`se_company_person_normalized`, 23 columns). */
export interface SePersonNormalizedRow {
  company_id: string;
  source: string;
  slot: string;
  suggestion_id: string;
  normalized_id: string;
  normalizer_version: string;
  parse_status: string;
  parse_notes: string[];
  first_tokens: string[];
  middle_tokens: string[];
  last_tokens: string[];
  display_first: string;
  display_last: string;
  display_name: string;
  birth_year: string;
  wikidata_id: string;
  role_code: string;
  role_key: string;
  role_year: string;
  role_from: string;
  role_to: string;
  data: string;
  normalized_at: string;
}

/** One raw suggestion (`se_company_person_suggestion`, 18 columns) -- what the source
 * (or the reviewer) delivered, never normalized. */
export interface SePersonRawRow {
  company_id: string;
  source: string;
  slot: string;
  suggestion_id: string;
  suggested_at: string;
  source_record_id: string;
  full_name: string;
  first_name: string;
  last_name: string;
  birth_year: string;
  wikidata_id: string;
  role_original: string;
  role_key: string;
  fiscal_year: string;
  role_from: string;
  role_to: string;
  document_ref: string;
  data: string;
}

/** One version of a per-company person rule (`se_company_person_rule`, 9 columns);
 * `active = 0` releases it. */
export interface SePersonRuleRow {
  company_id: string;
  rule_id: string;
  kind: string;
  person_keys: string[];
  slots: string[];
  active: number;
  note: string;
  created_at: string;
  created_by: string;
}

/** One spelling-precedence row (`se_company_person_precedence`, 8 columns); the global
 * export carries `company_id = ''`. */
export interface SePersonPrecedenceRow {
  company_id: string;
  field: string;
  source: string;
  precedence: number;
  removed: number;
  decided_by: string;
  note: string;
  decided_at: string;
}

/* ------------------------------------------------------------------ */
/* The detail object                                                    */
/* ------------------------------------------------------------------ */

/** One contributing observation of a published person, resolved through the row's
 * member arrays (spec 5.4's lineage). */
export interface SePersonMember {
  source: string;
  slot: string;
  /** The normalized version this published row was folded from. */
  normalizedId: string;
  /** The spelling, birth year, QID and data THIS member contributed. */
  name: string;
  birthYear: string;
  wikidataId: string;
  data: string;
  /** That slot's CURRENT normalized version, `null` when it has none. */
  current: SePersonNormalizedRow | null;
  raw: SePersonRawRow | null;
  /** The current normalized version is not the one the row was folded from. */
  refoldPending: boolean;
  /** This source's `name` precedence for this company (spec 3.6). */
  precedence: number;
}

/** One entry of the published role block (spec 5.4's parallel triple, zipped). */
export interface SePersonRoleEntry {
  code: string;
  year: number;
  sources: string[];
}

export interface SePersonPublished {
  row: SePersonRow;
  members: SePersonMember[];
  roles: SePersonRoleEntry[];
  /** Why the published spelling came from `row.text_source` (spec 5.3's sort). */
  spellingReason: "precedence" | "most complete" | "tie-break" | "single source";
  /** The active rules that name this person, by key or by one of its slots. */
  rules: SePersonRuleRow[];
}

/** A reviewer draft: the `reviewer_draft` rows of one group slot that still hold a
 * name. One draft is one person; one row inside it is one role entry (Ruling 2). */
export interface SePersonDraft {
  /** The group slot, `r` + the stamp's 17 digits. */
  slot: string;
  rows: SePersonRawRow[];
  /** Present once a normalize run (or Fold now) has parsed the draft. */
  normalized: SePersonNormalizedRow[];
  name: string;
  note: string;
  /** The published key this draft corrects, `''` for a plain Add. */
  replacesKey: string;
}

export interface SePersonDetail {
  published: SePersonPublished[];
  drafts: SePersonDraft[];
  history: SePersonHistoryRow[];
  /** Every current rule version of this company, released ones included. */
  rules: SePersonRuleRow[];
  /** The global spelling order and this company's own overrides. */
  precedence: SePersonPrecedenceRow[];
  foldPending: boolean;
}

/* ------------------------------------------------------------------ */
/* The reads                                                            */
/* ------------------------------------------------------------------ */

const MAIN_COLUMNS_SQL = (a: string) => `  ${a}.company_id AS company_id, toString(${a}.person_key) AS person_key,
  ${a}.display_name AS display_name, ${a}.first_name AS first_name, ${a}.last_name AS last_name,
  ifNull(toString(${a}.birth_year), '') AS birth_year, ifNull(${a}.wikidata_id, '') AS wikidata_id,
  arrayMap(x -> toString(x), ${a}.sources) AS sources, ${a}.slots AS slots,
  arrayMap(x -> toString(x), ${a}.normalized_ids) AS normalized_ids,
  arrayMap(x -> toString(x), ${a}.member_sources) AS member_sources, ${a}.member_slots AS member_slots,
  ${a}.member_names AS member_names,
  arrayMap(x -> ifNull(toString(x), ''), ${a}.member_birth_years) AS member_birth_years,
  ${a}.member_wikidata_ids AS member_wikidata_ids, ${a}.member_data AS member_data,
  ${a}.role_codes AS role_codes, ${a}.role_years AS role_years, ${a}.role_sources AS role_sources,
  ${a}.current_roles AS current_roles,
  ifNull(toString(${a}.first_year), '') AS first_year, ifNull(toString(${a}.last_year), '') AS last_year,
  toString(${a}.text_source) AS text_source, ${a}.data AS data,
  toUInt8(${a}.active) AS active, toString(${a}.inactive_reason) AS inactive_reason,
  toString(${a}.folded_at) AS folded_at, toString(${a}.fold_version) AS fold_version,
  ${a}.source_run_id AS source_run_id`;

export const PERSON_MAIN_SQL = `SELECT
${MAIN_COLUMNS_SQL("m")}
FROM ${SE_COMPANY_PERSON_TABLE} AS m FINAL
WHERE m.company_id = {companyId:String}
ORDER BY m.active DESC, m.inactive_reason, m.display_name`;

/** History is append-only: no FINAL, newest first, capped like the Address tab's. Its
 * own key is `changed_at` -- `folded_at` on a history row is when the PREVIOUS image was
 * published, which is not the order the reviewer reads it in. */
export const PERSON_HISTORY_SQL = `SELECT
${MAIN_COLUMNS_SQL("h")},
  toString(h.changed_at) AS changed_at, toString(h.change_kind) AS change_kind,
  h.fold_run_id AS fold_run_id
FROM corpscout.se_company_person_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.changed_at DESC
LIMIT 200`;

export const PERSON_NORMALIZED_SQL = `SELECT
  n.company_id AS company_id, toString(n.source) AS source, n.slot AS slot,
  toString(n.suggestion_id) AS suggestion_id, toString(n.normalized_id) AS normalized_id,
  toString(n.normalizer_version) AS normalizer_version, toString(n.parse_status) AS parse_status,
  n.parse_notes AS parse_notes, n.first_tokens AS first_tokens, n.middle_tokens AS middle_tokens,
  n.last_tokens AS last_tokens, n.display_first AS display_first, n.display_last AS display_last,
  n.display_name AS display_name, ifNull(toString(n.birth_year), '') AS birth_year,
  ifNull(n.wikidata_id, '') AS wikidata_id, ifNull(n.role_code, '') AS role_code,
  ifNull(n.role_key, '') AS role_key, ifNull(toString(n.role_year), '') AS role_year,
  ifNull(toString(n.role_from), '') AS role_from, ifNull(toString(n.role_to), '') AS role_to,
  n.data AS data, toString(n.normalized_at) AS normalized_at
FROM corpscout.se_company_person_normalized AS n FINAL
WHERE n.company_id = {companyId:String}
ORDER BY n.source, n.slot`;

export const PERSON_RAW_SQL = `SELECT
  s.company_id AS company_id, toString(s.source) AS source, s.slot AS slot,
  toString(s.suggestion_id) AS suggestion_id, toString(s.suggested_at) AS suggested_at,
  s.source_record_id AS source_record_id, ifNull(s.full_name, '') AS full_name,
  ifNull(s.first_name, '') AS first_name, ifNull(s.last_name, '') AS last_name,
  ifNull(toString(s.birth_year), '') AS birth_year, ifNull(s.wikidata_id, '') AS wikidata_id,
  ifNull(s.role_original, '') AS role_original, ifNull(s.role_key, '') AS role_key,
  ifNull(toString(s.fiscal_year), '') AS fiscal_year, ifNull(toString(s.role_from), '') AS role_from,
  ifNull(toString(s.role_to), '') AS role_to, ifNull(s.document_ref, '') AS document_ref,
  s.data AS data
FROM corpscout.se_company_person_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source, s.slot`;

export const PERSON_RULES_SQL = `SELECT
  r.company_id AS company_id, toString(r.rule_id) AS rule_id, toString(r.kind) AS kind,
  arrayMap(x -> toString(x), r.person_keys) AS person_keys, r.slots AS slots,
  toUInt8(r.active) AS active, r.note AS note, toString(r.created_at) AS created_at,
  r.created_by AS created_by
FROM corpscout.se_company_person_rule AS r FINAL
WHERE r.company_id = {companyId:String}
ORDER BY r.created_at DESC`;

/** The spelling order (spec 3.6): the global export (`company_id = ''`) and this
 * company's own overrides in one read. Only the `name` field has a precedence. */
export const PERSON_PRECEDENCE_SQL = `SELECT
  p.company_id AS company_id, toString(p.field) AS field, toString(p.source) AS source,
  toUInt32(p.precedence) AS precedence, toUInt8(p.removed) AS removed,
  toString(p.decided_by) AS decided_by, p.note AS note, toString(p.decided_at) AS decided_at
FROM corpscout.se_company_person_precedence AS p FINAL
WHERE p.company_id IN ('', {companyId:String}) AND p.field = 'name'
ORDER BY p.precedence DESC`;

/** The parse status the fold publishes (`fold.py::FOLDABLE_STATUS`); a row that scores
 * `partial` or `no_person` never becomes a person, so it never owes a fold. */
const FOLDABLE_PARSE_STATUS = "ok";

function slotKey(source: string, slot: string): string {
  return `${source}|${slot}`;
}

/** A `data` string as an object; the tables constrain it to one, and a hand-written row
 * that slipped through reads as empty rather than throwing the whole tab. */
function parseObject(data: string): Record<string, unknown> {
  try {
    const parsed: unknown = JSON.parse(data === "" ? "{}" : data);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
}

function stringAt(data: Record<string, unknown>, key: string): string {
  const value = data[key];
  return typeof value === "string" ? value : "";
}

/** `precedence.py::precedence_for` in TypeScript: the company's own row for a source
 * when one is in force, else the global number, else 0. */
function precedenceReader(rows: readonly SePersonPrecedenceRow[]): (source: string) => number {
  const own = new Map<string, number>();
  const global = new Map<string, number>();
  for (const row of rows) {
    if (row.removed !== 0) continue;
    (row.company_id === "" ? global : own).set(row.source, row.precedence);
  }
  return (source) => own.get(source) ?? global.get(source) ?? 0;
}

function wordCount(text: string): number {
  const trimmed = text.trim();
  return trimmed === "" ? 0 : trimmed.split(/\s+/).length;
}

/**
 * Why the published spelling came from `row.text_source`. The fold sorts members by
 * `name` precedence first, then token count, then string length, then alphabetically
 * (`fold.py::_text_member`): the first two keys are what the reviewer can be told
 * about, the last two are a tie-break nothing meaningful decided.
 */
function spellingReason(
  row: SePersonRow,
  members: readonly SePersonMember[],
): SePersonPublished["spellingReason"] {
  if (members.length === 1) return "single source";
  const own = members.filter((member) => member.source === row.text_source);
  if (own.length === 0) return "tie-break";
  const top = own.reduce((best, member) => Math.max(best, member.precedence), 0);
  const others = members.filter((member) => member.source !== row.text_source);
  // Beaten outright on precedence alone -- and only when there IS another source to
  // beat: two members of the same source tie by definition.
  if (others.length > 0 && others.every((member) => member.precedence < top)) return "precedence";
  const atTop = members.filter((member) => member.precedence === top);
  const differing = atTop.filter((member) => member.name !== row.display_name);
  const published = wordCount(row.display_name);
  if (differing.length > 0 && differing.every((member) => published > wordCount(member.name))) {
    return "most complete";
  }
  return "tie-break";
}

function membersOf(
  row: SePersonRow,
  normalizedBySlot: Map<string, SePersonNormalizedRow>,
  rawBySlot: Map<string, SePersonRawRow>,
  precedenceOf: (source: string) => number,
): SePersonMember[] {
  return row.member_sources.map((source, index) => {
    const slot = row.member_slots[index] ?? "";
    const normalizedId = row.normalized_ids[index] ?? "";
    const current = normalizedBySlot.get(slotKey(source, slot)) ?? null;
    return {
      source,
      slot,
      normalizedId,
      name: row.member_names[index] ?? "",
      birthYear: row.member_birth_years[index] ?? "",
      wikidataId: row.member_wikidata_ids[index] ?? "",
      data: row.member_data[index] ?? "",
      current,
      raw: rawBySlot.get(slotKey(source, slot)) ?? null,
      refoldPending: current !== null && current.normalized_id !== normalizedId,
      precedence: precedenceOf(source),
    };
  });
}

/** The rules the People tab shows on a person: the ones still in force that name its
 * key, or one of the observations it is built from (a split rule names slots only). */
function rulesFor(rules: readonly SePersonRuleRow[], row: SePersonRow): SePersonRuleRow[] {
  return rules.filter(
    (rule) =>
      rule.active === 1 &&
      (rule.person_keys.includes(row.person_key) ||
        rule.slots.some((slot) => row.member_slots.includes(slot))),
  );
}

/** A `reviewer_draft` row still naming somebody; a row with all three name columns
 * empty is the tombstone an Activate, a Discard or a deleted role left behind. */
function holdsName(row: SePersonRawRow): boolean {
  return row.full_name !== "" || row.first_name !== "" || row.last_name !== "";
}

/** A raw row's spelling: the split name the backoffice writes, else the full name a
 * source delivered. */
function personName(row: SePersonRawRow | undefined): string {
  if (row === undefined) return "";
  const split = [row.first_name, row.last_name].filter((part) => part !== "").join(" ");
  return split === "" ? row.full_name : split;
}

function draftsOf(
  rawRows: readonly SePersonRawRow[],
  normalizedRows: readonly SePersonNormalizedRow[],
): SePersonDraft[] {
  const groups = new Map<string, SePersonRawRow[]>();
  for (const row of rawRows) {
    if (row.source !== DRAFT_SOURCE) continue;
    const group = groupOfRowSlot(row.slot);
    const rows = groups.get(group);
    if (rows === undefined) groups.set(group, [row]);
    else rows.push(row);
  }
  const drafts: SePersonDraft[] = [];
  for (const [group, rows] of groups) {
    const sorted = [...rows].sort((a, b) => (a.slot < b.slot ? -1 : a.slot > b.slot ? 1 : 0));
    // The lowest slot that still names somebody -- the one Activate promotes first, so
    // the tab reads the person and the note Activate will act on.
    const first = sorted.find(holdsName);
    if (first === undefined) continue;
    const data = parseObject(first.data);
    drafts.push({
      slot: group,
      rows: sorted,
      normalized: normalizedRows.filter(
        (row) => row.source === DRAFT_SOURCE && groupOfRowSlot(row.slot) === group,
      ),
      name: personName(first),
      note: stringAt(data, "note"),
      replacesKey: stringAt(data, "replaces_key"),
    });
  }
  return drafts;
}

/**
 * The whole tab in one round trip: the published persons with their members, roles and
 * rules, the drafts, the history, the rules and the spelling order. Null when the
 * company has no person at any layer -- no published row, no normalized row and no
 * draft -- which the route turns into the workspace's empty state.
 */
export async function loadSePersonDetail(companyId: string): Promise<SePersonDetail | null> {
  const [mainRows, history, normalizedRows, rawRows, rules, precedence] = await Promise.all([
    chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
    chQuery<SePersonHistoryRow>(PERSON_HISTORY_SQL, { companyId }),
    chQuery<SePersonNormalizedRow>(PERSON_NORMALIZED_SQL, { companyId }),
    chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId }),
    chQuery<SePersonRuleRow>(PERSON_RULES_SQL, { companyId }),
    chQuery<SePersonPrecedenceRow>(PERSON_PRECEDENCE_SQL, { companyId }),
  ]);
  const normalizedBySlot = new Map(normalizedRows.map((row) => [slotKey(row.source, row.slot), row]));
  const rawBySlot = new Map(rawRows.map((row) => [slotKey(row.source, row.slot), row]));
  const precedenceOf = precedenceReader(precedence);
  const published = mainRows.map((row) => {
    const members = membersOf(row, normalizedBySlot, rawBySlot, precedenceOf);
    return {
      row,
      members,
      roles: row.role_codes.map((code, index) => ({
        code,
        year: row.role_years[index] ?? 0,
        sources: row.role_sources[index] ?? [],
      })),
      spellingReason: spellingReason(row, members),
      rules: rulesFor(rules, row),
    };
  });
  const drafts = draftsOf(rawRows, normalizedRows);
  if (published.length === 0 && drafts.length === 0 && normalizedRows.length === 0) return null;
  // Drafts are never folded and never published, so they must not raise "Fold pending"
  // on their own -- Dagster's own selection (`batch.py::_changed_company_ids`) excludes
  // them, and so does the GLOBAL precedence export, which selects no company at all
  // (Ruling 7). A company's OWN precedence override does select it.
  const foldable = normalizedRows.filter((row) => row.source !== DRAFT_SOURCE);
  // A raw row the normalize step has not seen yet has no normalized version to speak
  // for it -- an activated reviewer person, or a Remove's tombstone -- so its own
  // `suggested_at` is what says a fold is owed.
  const foldableRaw = rawRows.filter((row) => row.source !== DRAFT_SOURCE);
  const foldedAt = mainRows.reduce<string | null>(
    (newest, row) => (newest === null || row.folded_at > newest ? row.folded_at : newest),
    null,
  );
  return {
    published,
    drafts,
    history,
    rules,
    precedence,
    foldPending: personFoldPending(
      foldedAt,
      [
        ...foldable.map((row) => row.normalized_at),
        ...foldableRaw.map((row) => row.suggested_at),
        // A released rule counts too: the release is not applied until the fold.
        ...rules.map((rule) => rule.created_at),
        ...precedence.filter((row) => row.company_id !== "").map((row) => row.decided_at),
      ],
      foldable.some((row) => row.parse_status === FOLDABLE_PARSE_STATUS),
    ),
  };
}

/** The role catalog the sheet offers (Ruling 3): the live table's ACTIVE codes, as the
 * client-safe option the form and the labels want. */
export async function loadSePersonRoleOptions(): Promise<SePersonRoleOption[]> {
  const types = await getCompanyPersonRoleTypes();
  return types
    .filter((type) => type.is_active === 1)
    .map((type) => ({ code: type.role_code, label: type.display_name, group: type.role_group }));
}

/* ------------------------------------------------------------------ */
/* The writes                                                           */
/* ------------------------------------------------------------------ */

/** A reviewer decision this module refuses; the route renders the message. */
export class SePersonDecisionError extends Error {}

// DRAFT_SOURCE and REVIEWER_SOURCE are imported from `~/lib/se-person-fields`, not
// re-declared: one spelling of each source string across the six files.
const DECIDED_BY = "backoffice";
const HIDE_KIND = "hide";
const MERGE_KIND = "merge";
const SPLIT_KIND = "split";

/** The 18 columns of `tables.SUGGESTION_COLUMNS`, in that order. Every Nullable one is
 * `string | null` or `number | null` so a cleared value is NULL, never ''. */
export interface SePersonRawInsertRow {
  company_id: string;
  source: string;
  slot: string;
  suggestion_id: string;
  suggested_at: string;
  source_record_id: string;
  full_name: string | null;
  first_name: string | null;
  last_name: string | null;
  birth_year: number | null;
  wikidata_id: string | null;
  role_original: string | null;
  role_key: string | null;
  fiscal_year: number | null;
  role_from: string | null;
  role_to: string | null;
  document_ref: string | null;
  data: string;
}
/** The 9 columns of `tables.RULE_COLUMNS`. */
export interface SePersonRuleInsertRow {
  company_id: string;
  rule_id: string;
  kind: string;
  person_keys: string[];
  slots: string[];
  active: number;
  note: string;
  created_at: string;
  created_by: string;
}

/** The twelve value columns of a suggestion row -- everything but the key, the stamp
 * and `data`. A cleared column is NULL, which is the tombstone the fold reads. */
type SePersonValueColumns = Pick<
  SePersonRawInsertRow,
  | "source_record_id"
  | "full_name"
  | "first_name"
  | "last_name"
  | "birth_year"
  | "wikidata_id"
  | "role_original"
  | "role_key"
  | "fiscal_year"
  | "role_from"
  | "role_to"
  | "document_ref"
>;

/**
 * The id the extractors compute in SQL for the same row (`person/suggestions.py`):
 * sha256 over company, source, slot and the stamp, newline-joined, with the stamp
 * spelled exactly as `suggested_at`.
 */
function suggestionId(companyId: string, source: string, slot: string, stamp: string): string {
  return createHash("sha256").update(`${companyId}\n${source}\n${slot}\n${stamp}`).digest("hex");
}

/** Minted once, when the rule is created; a Reset writes a new VERSION of this id. */
function ruleId(
  companyId: string,
  kind: string,
  keys: readonly string[],
  slots: readonly string[],
  stamp: string,
): string {
  const preimage = `${companyId}\n${kind}\n${[...keys].sort().join(",")}\n${[...slots].sort().join(",")}\n${stamp}`;
  return createHash("sha256").update(preimage).digest("hex");
}

function numberOrNull(value: string): number | null {
  return value === "" ? null : Number(value);
}
function textOrNull(value: string): string | null {
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

/** Ruling 4: the reviewer's own object plus the keys the backoffice owns. */
function rowData(own: string, note: string, replacesKey: string | null): string {
  return JSON.stringify({
    ...parseObject(own),
    decided_by: DECIDED_BY,
    note,
    ...(replacesKey === null ? {} : { replaces_key: replacesKey }),
  });
}

/** A stored row's `data` without the three keys the backoffice owns, so re-stamping it
 * cannot carry a stale note -- or a `replaces_key` -- onto the next version. */
function ownData(data: string): string {
  const parsed = parseObject(data);
  for (const key of RESERVED_DATA_KEYS) delete parsed[key];
  return JSON.stringify(parsed);
}

/** Ruling 5: one year is a fiscal year; a span is two dates, an open end NULL; a role
 * with no year at all carries neither and the fold takes it as held now. */
function roleColumns(
  role: SePersonRoleInput | null,
): Pick<SePersonRawInsertRow, "role_original" | "role_key" | "fiscal_year" | "role_from" | "role_to"> {
  if (role === null) {
    return { role_original: null, role_key: null, fiscal_year: null, role_from: null, role_to: null };
  }
  const { code, fromYear, toYear } = role;
  const single = fromYear !== "" && toYear === fromYear;
  return {
    role_original: code,
    role_key: code,
    fiscal_year: single ? Number(fromYear) : null,
    role_from: single || fromYear === "" ? null : `${fromYear}-01-01`,
    role_to: single || toYear === "" ? null : `${toYear}-12-31`,
  };
}

/** Every value column NULL: the tombstone that retires one slot. */
const CLEARED_COLUMNS: SePersonValueColumns = {
  source_record_id: "",
  full_name: null,
  first_name: null,
  last_name: null,
  birth_year: null,
  wikidata_id: null,
  role_original: null,
  role_key: null,
  fiscal_year: null,
  role_from: null,
  role_to: null,
  document_ref: null,
};

/** What the reviewer typed, as one row's worth of columns. */
function inputColumns(
  person: Pick<SePersonInput, "firstName" | "lastName" | "birthYear" | "wikidataId">,
  role: SePersonRoleInput | null,
): SePersonValueColumns {
  return {
    source_record_id: "",
    full_name: null,
    first_name: textOrNull(person.firstName),
    last_name: textOrNull(person.lastName),
    birth_year: numberOrNull(person.birthYear),
    wikidata_id: textOrNull(person.wikidataId),
    ...roleColumns(role),
    document_ref: null,
  };
}

/** A stored row's own columns, for the `reviewer` version Activate promotes it into.
 * The read collapsed every NULL to `''`, so this is the inverse. */
function storedColumns(row: SePersonRawRow): SePersonValueColumns {
  return {
    source_record_id: row.source_record_id,
    full_name: textOrNull(row.full_name),
    first_name: textOrNull(row.first_name),
    last_name: textOrNull(row.last_name),
    birth_year: numberOrNull(row.birth_year),
    wikidata_id: textOrNull(row.wikidata_id),
    role_original: textOrNull(row.role_original),
    role_key: textOrNull(row.role_key),
    fiscal_year: numberOrNull(row.fiscal_year),
    role_from: textOrNull(row.role_from),
    role_to: textOrNull(row.role_to),
    document_ref: textOrNull(row.document_ref),
  };
}

/** One version of a suggestion row, the 18 columns in `tables.SUGGESTION_COLUMNS`
 * order. */
function rowVersion(
  companyId: string,
  source: string,
  slot: string,
  stamp: string,
  columns: SePersonValueColumns,
  data: string,
): SePersonRawInsertRow {
  return {
    company_id: companyId,
    source,
    slot,
    suggestion_id: suggestionId(companyId, source, slot, stamp),
    suggested_at: stamp,
    source_record_id: columns.source_record_id,
    full_name: columns.full_name,
    first_name: columns.first_name,
    last_name: columns.last_name,
    birth_year: columns.birth_year,
    wikidata_id: columns.wikidata_id,
    role_original: columns.role_original,
    role_key: columns.role_key,
    fiscal_year: columns.fiscal_year,
    role_from: columns.role_from,
    role_to: columns.role_to,
    document_ref: columns.document_ref,
    data,
  };
}

/** One version of a rule row, the 9 columns in `tables.RULE_COLUMNS` order. */
function ruleVersion(
  companyId: string,
  ruleIdValue: string,
  kind: string,
  keys: readonly string[],
  slots: readonly string[],
  active: number,
  note: string,
  stamp: string,
): SePersonRuleInsertRow {
  return {
    company_id: companyId,
    rule_id: ruleIdValue,
    kind,
    person_keys: [...keys],
    slots: [...slots],
    active,
    note,
    created_at: stamp,
    created_by: DECIDED_BY,
  };
}

/** A brand-new rule: its id is minted from what it names and when it was made. */
function newRule(
  companyId: string,
  kind: string,
  keys: readonly string[],
  slots: readonly string[],
  note: string,
  stamp: string,
): SePersonRuleInsertRow {
  return ruleVersion(
    companyId,
    ruleId(companyId, kind, keys, slots, stamp),
    kind,
    keys,
    slots,
    1,
    note,
    stamp,
  );
}

/** The rows of one draft group, newest version each, sorted by slot. */
function groupRows(rows: readonly SePersonRawRow[], group: string): SePersonRawRow[] {
  return rows
    .filter((row) => row.source === DRAFT_SOURCE && groupOfRowSlot(row.slot) === group)
    .sort((a, b) => (a.slot < b.slot ? -1 : a.slot > b.slot ? 1 : 0));
}

/**
 * Add person / Correct / edit a draft: one `reviewer_draft` row per role entry, under a
 * new group slot (Ruling 2) or the group being edited. Never refuses -- a draft is not
 * published, so it may hold anything the validation accepted -- and never touches the
 * `reviewer` rows: Activate does that. Editing a draft down to fewer roles tombstones
 * the slots the new set no longer uses, or a deleted role would stay a live
 * observation.
 */
export async function saveSePersonDraft(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "save-draft" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string; slot: string }> {
  const stamp = clickhouseStamp(now);
  const group = decision.slot ?? personGroupSlot(stamp);
  const { input } = decision;
  const data = rowData(input.data, input.note, decision.replacesKey);
  const entries: (SePersonRoleInput | null)[] = input.roles.length === 0 ? [null] : input.roles;
  const rows = entries.map((role, index) =>
    rowVersion(
      companyId,
      DRAFT_SOURCE,
      personRowSlot(group, index + 1),
      stamp,
      inputColumns(input, role),
      data,
    ),
  );
  if (decision.slot !== null) {
    const used = new Set(rows.map((row) => row.slot));
    const stored = await chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId });
    for (const row of groupRows(stored, group)) {
      if (used.has(row.slot)) continue;
      rows.push(
        rowVersion(
          companyId,
          DRAFT_SOURCE,
          row.slot,
          stamp,
          CLEARED_COLUMNS,
          rowData("{}", "role removed", null),
        ),
      );
    }
  }
  await chInsertSeCompanyPersonSuggestions(rows);
  return { decidedAt: stamp, slot: group };
}

/** The identity the normalizer scored for one observation. */
function identityOf(row: SePersonNormalizedRow): SePersonIdentity {
  return {
    firstTokens: row.first_tokens,
    middleTokens: row.middle_tokens,
    lastTokens: row.last_tokens,
    birthYear: row.birth_year,
    wikidataId: row.wikidata_id,
  };
}

/**
 * Activate: the draft becomes the company's `reviewer` person under the same slots, and
 * the draft rows are cleared, in ONE insert so a reader never sees the pair
 * half-applied. A Correct (the draft carries `replaces_key`) also hides the person it
 * replaces -- without that rule the source observations would republish it -- unless
 * some member of that person folds back into the reviewer's spelling (Ruling 1): the
 * reviewer only retyped what a source already delivers, and the hide rule would take
 * the correction down with it. A key naming no published person still gets the rule:
 * the fold resolves it through the previous members.
 */
export async function activateSePersonDraft(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "activate" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const [rawRows, normalizedRows, mainRows] = await Promise.all([
    chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId }),
    chQuery<SePersonNormalizedRow>(PERSON_NORMALIZED_SQL, { companyId }),
    chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
  ]);
  const rows = groupRows(rawRows, decision.slot).filter(holdsName);
  const first = rows[0];
  if (first === undefined) throw new SePersonDecisionError("No draft to activate.");
  const draftData = parseObject(first.data);
  const note = decision.note !== "" ? decision.note : stringAt(draftData, "note");
  const replacesKey = stringAt(draftData, "replaces_key");
  await chInsertSeCompanyPersonSuggestions([
    ...rows.map((row) =>
      rowVersion(
        companyId,
        REVIEWER_SOURCE,
        row.slot,
        stamp,
        storedColumns(row),
        rowData(ownData(row.data), note, null),
      ),
    ),
    ...rows.map((row) =>
      rowVersion(
        companyId,
        DRAFT_SOURCE,
        row.slot,
        stamp,
        CLEARED_COLUMNS,
        rowData("{}", "activated", null),
      ),
    ),
  ]);
  if (replacesKey === "") return { decidedAt: stamp };
  const tokens = normalizeSePersonName({
    fullName: first.full_name,
    firstName: first.first_name,
    lastName: first.last_name,
  });
  const draftIdentity: SePersonIdentity = {
    firstTokens: tokens.firstTokens,
    middleTokens: tokens.middleTokens,
    lastTokens: tokens.lastTokens,
    birthYear: first.birth_year,
    wikidataId: first.wikidata_id,
  };
  const normalizedBySlot = new Map(normalizedRows.map((row) => [slotKey(row.source, row.slot), row]));
  const corrected = mainRows.find((row) => row.person_key === replacesKey);
  // A member with no current normalized row has no identity to compare and can never
  // fold back, so it never spares the corrected person its hide rule.
  const foldsBack =
    corrected !== undefined &&
    corrected.member_sources.some((source, index) => {
      const current = normalizedBySlot.get(slotKey(source, corrected.member_slots[index] ?? ""));
      return current !== undefined && foldsIntoPerson(draftIdentity, identityOf(current));
    });
  if (!foldsBack) {
    await chInsertSeCompanyPersonRules([
      newRule(companyId, HIDE_KIND, [replacesKey], [], "corrected by reviewer", stamp),
    ]);
  }
  return { decidedAt: stamp };
}

/** Discard: every slot of the group keeps its lineage but names nobody again. */
export async function discardSePersonDraft(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "discard" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const rawRows = await chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId });
  const rows = groupRows(rawRows, decision.slot).filter(holdsName);
  if (rows.length === 0) throw new SePersonDecisionError("No draft to discard.");
  await chInsertSeCompanyPersonSuggestions(
    rows.map((row) =>
      rowVersion(
        companyId,
        DRAFT_SOURCE,
        row.slot,
        stamp,
        CLEARED_COLUMNS,
        rowData("{}", "discarded", null),
      ),
    ),
  );
  return { decidedAt: stamp };
}

/** The hide rule in force for a key: a current version (FINAL already collapsed the
 * older ones) that has not been released. */
function activeHideRule(rules: readonly SePersonRuleRow[], personKey: string): SePersonRuleRow | null {
  return (
    rules.find(
      (rule) => rule.kind === HIDE_KIND && rule.active === 1 && rule.person_keys.includes(personKey),
    ) ?? null
  );
}

/**
 * Remove: the person key is computed over the whole member set (`fold.py`), so retiring
 * one member of a mixed person would shrink that set, re-key the person and orphan the
 * rule -- it would come back under a new key with the old one merely hidden. So a
 * person with any non-reviewer member is hidden by the rule alone, every member left in
 * place; only a reviewer-only person is removed by tombstoning its own slots, and then
 * no rule is needed.
 */
export async function removeSePerson(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "remove" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const [mainRows, rules] = await Promise.all([
    chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
    chQuery<SePersonRuleRow>(PERSON_RULES_SQL, { companyId }),
  ]);
  const row = isPersonKey(decision.personKey)
    ? mainRows.find((candidate) => candidate.person_key === decision.personKey)
    : undefined;
  if (row === undefined) throw new SePersonDecisionError("Unknown person.");
  // Hidden already, or hidden by a rule the next fold has yet to apply.
  if (row.inactive_reason === "hidden" || activeHideRule(rules, row.person_key) !== null) {
    throw new SePersonDecisionError("Already hidden.");
  }
  const note = decision.note === "" ? "removed by reviewer" : decision.note;
  if (row.member_sources.some((source) => source !== REVIEWER_SOURCE)) {
    await chInsertSeCompanyPersonRules([
      newRule(companyId, HIDE_KIND, [row.person_key], [], note, stamp),
    ]);
    return { decidedAt: stamp };
  }
  await chInsertSeCompanyPersonSuggestions(
    row.member_slots.map((slot) =>
      rowVersion(
        companyId,
        REVIEWER_SOURCE,
        slot,
        stamp,
        CLEARED_COLUMNS,
        rowData("{}", note, null),
      ),
    ),
  );
  return { decidedAt: stamp };
}

/** Merge: one rule naming the keys the reviewer picked, which the next fold folds into
 * one person. Every key must name a published person -- a stale key would name nothing
 * and the rule would sit there doing nothing. */
export async function mergeSePersons(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "merge" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const mainRows = await chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId });
  const published = new Set(mainRows.map((row) => row.person_key));
  if (decision.personKeys.some((key) => !published.has(key))) {
    throw new SePersonDecisionError("Unknown person.");
  }
  const keys = [...decision.personKeys].sort();
  const note = decision.note === "" ? "merged by reviewer" : decision.note;
  await chInsertSeCompanyPersonRules([newRule(companyId, MERGE_KIND, keys, [], note, stamp)]);
  return { decidedAt: stamp };
}

function sameSet(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

/** Split: one rule naming the observations the reviewer checked, which the next fold
 * keeps out of the person they are in now. Refuses a set that is every observation of
 * one person -- the fold would rebuild exactly what is there. */
export async function splitSePersonSlots(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "split" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const mainRows = await chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId });
  const known = new Set(mainRows.flatMap((row) => row.member_slots));
  if (decision.slots.some((slot) => !known.has(slot))) {
    throw new SePersonDecisionError("Unknown observation.");
  }
  const slots = [...decision.slots].sort();
  const noop = mainRows.some((row) => sameSet([...row.member_slots].sort(), slots));
  if (noop) throw new SePersonDecisionError("That is every observation of one person.");
  const note = decision.note === "" ? "split by reviewer" : decision.note;
  await chInsertSeCompanyPersonRules([newRule(companyId, SPLIT_KIND, [], slots, note, stamp)]);
  return { decidedAt: stamp };
}

/**
 * Reset to default: every rule still in force that names this person -- by its key, or
 * by one of the observations it is built from, which is how a split rule names it --
 * gets a new version with `active = 0`. The `rule_id` is kept: a rule is released, never
 * edited, and ReplacingMergeTree collapses on (company_id, rule_id).
 */
export async function resetSePersonRules(
  companyId: string,
  decision: Extract<SePersonDecision, { intent: "reset" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const stamp = clickhouseStamp(now);
  const [mainRows, rules] = await Promise.all([
    chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
    chQuery<SePersonRuleRow>(PERSON_RULES_SQL, { companyId }),
  ]);
  const row = isPersonKey(decision.personKey)
    ? mainRows.find((candidate) => candidate.person_key === decision.personKey)
    : undefined;
  if (row === undefined) throw new SePersonDecisionError("Unknown person.");
  const naming = rulesFor(rules, row);
  if (naming.length === 0) throw new SePersonDecisionError("No rule to reset.");
  const note = decision.note === "" ? "reset" : `reset: ${decision.note}`;
  await chInsertSeCompanyPersonRules(
    naming.map((rule) =>
      ruleVersion(
        companyId,
        rule.rule_id,
        rule.kind,
        rule.person_keys,
        rule.slots,
        0,
        note,
        stamp,
      ),
    ),
  );
  return { decidedAt: stamp };
}

const FOLD_NOW_TAG = { "backoffice/person": "fold-now" } as const;

/** Fold now (spec 5): one run of the targeted fold -- which normalizes the company's
 * raw rows first, so a person activated a moment ago parses -- for this company alone.
 * `changed_only` already defaults to `false` in `PersonFoldCompaniesConfig`, so the tab
 * never sends it. */
export async function launchSePersonFold(
  companyId: string,
): Promise<{ runId: string; url: string | null }> {
  const run = await launchRun({
    job: ASSET_JOB_NAME,
    assetSelection: [SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET],
    runConfig: {
      ops: { [SE_COMPANY_PERSON_FOLD_COMPANIES_ASSET]: { config: { company_ids: [companyId] } } },
    },
    tags: { ...FOLD_NOW_TAG },
  });
  return { runId: run.runId, url: dagsterRunUrl(run.runId) };
}
