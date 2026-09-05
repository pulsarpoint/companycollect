import { chInsertSeBasicInfoPrecedence, chInsertSeBasicInfoSuggestions, chQuery } from "~/lib/clickhouse.server";
import {
  ASSET_JOB_NAME,
  dagsterRunUrl,
  launchRun,
  SE_BASIC_INFO_FOLD_COMPANIES_ASSET,
} from "~/lib/dagster.server";
import type { SeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";
import { basicInfoFieldLabel, basicInfoSourceLabel, foldPending } from "~/lib/se-basic-info-fields";

/**
 * The Info tab's reads over the basic-info entity (spec 2026-09-03, sections
 * 3.2, 4, 5, 7): the folded main row with the source of every value, every
 * current suggestion row (one per source, reviewer included), the history
 * newest first, the exported precedence table, and the legal-form labels for
 * every code on the page.
 *
 * Every nullable value column is collapsed to '' so the component never has to
 * tell "" from null; dates and stamps arrive as ClickHouse's own strings.
 */

export interface SeBasicInfoRow {
  company_id: string;
  legal_name: string;
  legal_name_source: string;
  legal_form_code: string;
  legal_form_code_source: string;
  status: string;
  status_source: string;
  incorporation_date: string;
  incorporation_date_source: string;
  lei: string;
  lei_source: string;
  wikidata_id: string;
  wikidata_id_source: string;
  description: string;
  description_source: string;
  description_language: string;
  description_sv: string;
  description_sv_source: string;
  /** `YYYY-MM-DD HH:MM:SS.mmm` UTC. */
  folded_at: string;
  fold_version: string;
  source_run_id: string;
}

export interface SeBasicInfoSuggestionRow {
  company_id: string;
  source: string;
  source_record_uid: string;
  observed_at: string;
  suggested_at: string;
  legal_name: string;
  legal_form_code: string;
  status: string;
  incorporation_date: string;
  lei: string;
  wikidata_id: string;
  description: string;
  description_language: string;
  description_sv: string;
  decided_by: string;
  note: string;
  source_run_id: string;
  extractor_version: string;
}

export interface SeBasicInfoHistoryRow {
  folded_at: string;
  fold_version: string;
  source_run_id: string;
  changed_fields: string[];
}

export interface SeBasicInfoPrecedenceRow {
  company_id: string;
  field: string;
  source: string;
  precedence: number;
  /** 1 when this rule has been withdrawn (a Release); the row still exists so
   * a re-fold can see it was newer than the last publish. */
  removed: number;
  decided_by: string;
  note: string;
  /** `YYYY-MM-DD HH:MM:SS.mmm` UTC. */
  decided_at: string;
}

export interface SeBasicInfoLegalFormLabel {
  label_en: string;
  label_sv: string;
}

/** One row of the edit sheet's legal-form select. */
export interface SeBasicInfoLegalFormOption {
  code: string;
  label_sv: string;
  label_en: string;
}

export interface SeBasicInfoDetail {
  /** Null when the company has suggestions but has never been folded (or
   * has no register legal name, spec 5's publish rule). */
  info: SeBasicInfoRow | null;
  suggestions: SeBasicInfoSuggestionRow[];
  history: SeBasicInfoHistoryRow[];
  /** The global rows the code exports (`company_id = ''`). */
  precedence: SeBasicInfoPrecedenceRow[];
  /** This company's own rules that are still in force (`removed = 0`); each
   * overrides (or, for a source the global map does not rank, adds) the
   * global row for the same (field, source). */
  rules: SeBasicInfoPrecedenceRow[];
  /** Keyed by legal-form code: every code on the main row or any suggestion. */
  legalFormLabels: Record<string, SeBasicInfoLegalFormLabel>;
  /** Every numeric legal-form code, for the edit sheet's select. */
  legalFormOptions: SeBasicInfoLegalFormOption[];
  foldPending: boolean;
}

const VALUE_COLUMNS_SQL = (alias: string) => `  ${alias}.legal_name AS legal_name,
  ifNull(${alias}.legal_form_code, '') AS legal_form_code,
  toString(${alias}.status) AS status,
  ifNull(toString(${alias}.incorporation_date), '') AS incorporation_date,
  ifNull(${alias}.lei, '') AS lei,
  ifNull(${alias}.wikidata_id, '') AS wikidata_id,
  ifNull(${alias}.description, '') AS description,
  ifNull(${alias}.description_language, '') AS description_language,
  ifNull(${alias}.description_sv, '') AS description_sv`;

const SOURCE_COLUMNS_SQL = (alias: string) => `  toString(${alias}.legal_name_source) AS legal_name_source,
  toString(${alias}.legal_form_code_source) AS legal_form_code_source,
  toString(${alias}.status_source) AS status_source,
  toString(${alias}.incorporation_date_source) AS incorporation_date_source,
  toString(${alias}.lei_source) AS lei_source,
  toString(${alias}.wikidata_id_source) AS wikidata_id_source,
  toString(${alias}.description_source) AS description_source,
  toString(${alias}.description_sv_source) AS description_sv_source,
  toString(${alias}.folded_at) AS folded_at,
  toString(${alias}.fold_version) AS fold_version,
  ${alias}.source_run_id AS source_run_id`;

export const BASIC_INFO_SQL = `SELECT
  b.company_id AS company_id,
${VALUE_COLUMNS_SQL("b")},
${SOURCE_COLUMNS_SQL("b")}
FROM corpscout.se_company_basic_info AS b FINAL
WHERE b.company_id = {companyId:String}
LIMIT 1`;

export const BASIC_INFO_SUGGESTIONS_SQL = `SELECT
  s.company_id AS company_id,
  toString(s.source) AS source,
  s.source_record_uid AS source_record_uid,
  toString(s.observed_at) AS observed_at,
  toString(s.suggested_at) AS suggested_at,
${VALUE_COLUMNS_SQL("s").replace("s.legal_name AS legal_name", "ifNull(s.legal_name, '') AS legal_name").replace("toString(s.status) AS status", "ifNull(s.status, '') AS status")},
  ifNull(s.decided_by, '') AS decided_by,
  ifNull(s.note, '') AS note,
  s.source_run_id AS source_run_id,
  toString(s.extractor_version) AS extractor_version
FROM corpscout.se_company_basic_info_suggestion AS s FINAL
WHERE s.company_id = {companyId:String}
ORDER BY s.source`;

export const BASIC_INFO_HISTORY_SQL = `SELECT
  toString(h.folded_at) AS folded_at,
  toString(h.fold_version) AS fold_version,
  h.source_run_id AS source_run_id,
  h.changed_fields AS changed_fields
FROM corpscout.se_company_basic_info_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.folded_at DESC
LIMIT 200`;

export const BASIC_INFO_PRECEDENCE_SQL = `SELECT
  p.company_id AS company_id,
  toString(p.field) AS field,
  toString(p.source) AS source,
  toUInt32(p.precedence) AS precedence,
  toUInt8(p.removed) AS removed,
  toString(p.decided_by) AS decided_by,
  p.note AS note,
  toString(p.decided_at) AS decided_at
FROM corpscout.se_company_basic_info_precedence AS p FINAL
WHERE p.company_id IN ('', {companyId:String})
ORDER BY p.field, p.precedence DESC`;

/** This company's other active rules for one field -- read before a use-this
 * write so every other source's rule can be retired in the same insert. */
export const BASIC_INFO_ACTIVE_RULES_SQL = `SELECT
  toString(p.source) AS source
FROM corpscout.se_company_basic_info_precedence AS p FINAL
WHERE p.company_id = {companyId:String} AND p.field = {field:String} AND p.removed = 0`;

/** The curated dictionary for every code on the page at once; argMax over
 * `version` for the same reason as SHELL_LEGAL_FORM_LABEL_SQL. */
export const BASIC_INFO_LEGAL_FORM_LABELS_SQL = `SELECT
  l.code AS code,
  argMax(l.label_en, l.version) AS label_en,
  argMax(l.label_sv, l.version) AS label_sv
FROM corpscout.se_code_labels AS l
WHERE l.code_type = 'legal_form' AND l.code IN {codes:Array(String)}
GROUP BY l.code`;

interface LegalFormLabelQueryRow extends SeBasicInfoLegalFormLabel {
  code: string;
}

/** Every numeric legal-form code for the edit sheet's select, unbound (no
 * company parameter) since the options are the same for every company. */
export const BASIC_INFO_LEGAL_FORM_OPTIONS_SQL = `SELECT
  l.code AS code,
  argMax(l.label_sv, l.version) AS label_sv,
  argMax(l.label_en, l.version) AS label_en
FROM corpscout.se_code_labels AS l
WHERE l.code_type = 'legal_form' AND match(l.code, '^[0-9]{2}$')
GROUP BY l.code
ORDER BY l.code`;

export async function loadSeBasicInfoDetail(
  companyId: string,
): Promise<SeBasicInfoDetail | null> {
  const [infoRows, suggestions, history, precedenceRows, legalFormOptions] = await Promise.all([
    chQuery<SeBasicInfoRow>(BASIC_INFO_SQL, { companyId }),
    chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId }),
    chQuery<SeBasicInfoHistoryRow>(BASIC_INFO_HISTORY_SQL, { companyId }),
    chQuery<SeBasicInfoPrecedenceRow>(BASIC_INFO_PRECEDENCE_SQL, { companyId }),
    chQuery<SeBasicInfoLegalFormOption>(BASIC_INFO_LEGAL_FORM_OPTIONS_SQL),
  ]);
  const info = infoRows[0] ?? null;
  if (!info && suggestions.length === 0) return null;
  const precedence = precedenceRows.filter((row) => row.company_id === "");
  const companyRows = precedenceRows.filter((row) => row.company_id !== "");
  const rules = companyRows.filter((row) => row.removed === 0);
  const codes = [
    ...new Set(
      [info?.legal_form_code ?? "", ...suggestions.map((row) => row.legal_form_code)].filter(
        (code) => code !== "",
      ),
    ),
  ];
  const labelRows =
    codes.length === 0
      ? []
      : await chQuery<LegalFormLabelQueryRow>(BASIC_INFO_LEGAL_FORM_LABELS_SQL, { codes });
  const legalFormLabels: Record<string, SeBasicInfoLegalFormLabel> = {};
  for (const row of labelRows) {
    legalFormLabels[row.code] = { label_en: row.label_en, label_sv: row.label_sv };
  }
  return {
    info,
    suggestions,
    history,
    precedence,
    rules,
    legalFormLabels,
    legalFormOptions,
    // A release must also read as pending until folded, so every one of this
    // company's rows counts here -- not just the active (removed = 0) ones.
    // A `reviewer_draft` row is excluded: it is not yet activated, so it must
    // never raise "Fold pending" on its own (mirrors Dagster's
    // `suggestion_watermarks_sql` exclusion).
    foldPending: foldPending(info?.folded_at ?? null, [
      ...suggestions.filter((row) => row.source !== "reviewer_draft").map((row) => row.suggested_at),
      ...companyRows.map((row) => row.decided_at),
    ]),
  };
}

export class SeBasicInfoDecisionError extends Error {}

/** ClickHouse's own DateTime64(3) text form, UTC. */
export function clickhouseStamp(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

/** The suggestion table's nine value columns, in insert-column order. */
const SUGGESTION_VALUE_FIELDS = [
  "legal_name",
  "legal_form_code",
  "status",
  "incorporation_date",
  "lei",
  "wikidata_id",
  "description",
  "description_language",
  "description_sv",
] as const;

export type SeBasicInfoValueField = (typeof SUGGESTION_VALUE_FIELDS)[number];

/** One row for `chInsertSeBasicInfoSuggestions`: the nine value columns are
 * `Nullable` on the table, so every one of them is `string | null` here --
 * never `''` (spec 3.2's suggestion-row contract). */
export interface SeBasicInfoSuggestionInsertRow {
  company_id: string;
  source: string;
  source_record_uid: string;
  observed_at: string;
  legal_name: string | null;
  legal_form_code: string | null;
  status: string | null;
  incorporation_date: string | null;
  lei: string | null;
  wikidata_id: string | null;
  description: string | null;
  description_language: string | null;
  description_sv: string | null;
  decided_by: string;
  note: string | null;
  suggested_at: string;
  source_run_id: string;
  extractor_version: string;
}

function valueOrNull(value: string | undefined): string | null {
  return value === undefined || value === "" ? null : value;
}

/**
 * One version of a suggestion row (slice 3c): `edit`, `activate`, `discard`
 * and `reset` all write a version that starts from the current row of the
 * same source -- `current` (`undefined` when that source has no row yet) --
 * and changes exactly one field (`description` carries `description_language`
 * along). `''` on `current`'s value columns reads as NULL, same as a
 * missing `current`; `changes` applies directly (a change of `null` clears
 * the field, unconditionally -- it is never itself nullified). The fixed
 * backoffice columns (`decided_by`, `source_run_id`, `extractor_version`,
 * `source_record_uid = ''`) and both stamps (`observed_at = suggested_at`)
 * are the same for every version the backoffice writes.
 */
export function suggestionRowVersion(
  companyId: string,
  current: SeBasicInfoSuggestionRow | undefined,
  source: string,
  changes: Partial<Record<SeBasicInfoValueField, string | null>>,
  note: string,
  stamp: string,
): SeBasicInfoSuggestionInsertRow {
  const base = Object.fromEntries(
    SUGGESTION_VALUE_FIELDS.map((field) => [field, valueOrNull(current?.[field])]),
  ) as Record<SeBasicInfoValueField, string | null>;
  const values = { ...base, ...changes };
  const trimmedNote = note.trim();
  return {
    company_id: companyId,
    source,
    source_record_uid: "",
    observed_at: stamp,
    ...values,
    decided_by: "backoffice",
    note: trimmedNote === "" ? null : trimmedNote,
    suggested_at: stamp,
    source_run_id: "backoffice",
    extractor_version: "backoffice-v1",
  };
}

/**
 * Slice 3c's `edit`: writes a new `reviewer_draft` version with one field set
 * (description's language rides with it), starting from the company's
 * current draft row so every other field's draft value carries forward.
 * Never refuses -- a draft can hold any validated value.
 */
export async function appendSeBasicInfoDraft(
  companyId: string,
  decision: Extract<SeBasicInfoDecision, { intent: "edit" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const { field, value, language, note } = decision;
  const stamp = clickhouseStamp(now);
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const draft = suggestions.find((row) => row.source === "reviewer_draft");
  const changes: Partial<Record<SeBasicInfoValueField, string | null>> =
    field === "description" ? { description: value, description_language: language } : { [field]: value };
  const version = suggestionRowVersion(companyId, draft, "reviewer_draft", changes, note, stamp);
  await chInsertSeBasicInfoSuggestions([version]);
  return { decidedAt: stamp };
}

/**
 * Slice 3c's `activate`: promotes the field's draft value into the active
 * `reviewer` row. Refuses when the draft has no value for the field.
 * Otherwise writes, in one insert, the `reviewer` version with the field (and
 * `description_language` for `description`) copied from the draft, then the
 * `reviewer_draft` version with it cleared (note "activated").
 */
export async function activateSeBasicInfoDraft(
  companyId: string,
  decision: Extract<SeBasicInfoDecision, { intent: "activate" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const { field, note } = decision;
  const stamp = clickhouseStamp(now);
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const draft = suggestions.find((row) => row.source === "reviewer_draft");
  if (!draft || draft[field] === "") {
    throw new SeBasicInfoDecisionError(`No draft value for ${basicInfoFieldLabel(field)} to activate.`);
  }
  const reviewer = suggestions.find((row) => row.source === "reviewer");
  const draftValue = draft[field];
  const activateChanges: Partial<Record<SeBasicInfoValueField, string | null>> =
    field === "description"
      ? { description: draftValue, description_language: valueOrNull(draft.description_language) }
      : { [field]: draftValue };
  const reviewerVersion = suggestionRowVersion(companyId, reviewer, "reviewer", activateChanges, note, stamp);
  const clearChanges: Partial<Record<SeBasicInfoValueField, string | null>> =
    field === "description" ? { description: null, description_language: null } : { [field]: null };
  const draftVersion = suggestionRowVersion(companyId, draft, "reviewer_draft", clearChanges, "activated", stamp);
  await chInsertSeBasicInfoSuggestions([reviewerVersion, draftVersion]);
  return { decidedAt: stamp };
}

/**
 * Slice 3c's `discard`: drops the field's draft value. Refuses when the draft
 * has none; otherwise writes a `reviewer_draft` version with it (and
 * `description_language` for `description`) cleared, note "discarded".
 */
export async function discardSeBasicInfoDraft(
  companyId: string,
  decision: Extract<SeBasicInfoDecision, { intent: "discard" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const { field } = decision;
  const stamp = clickhouseStamp(now);
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const draft = suggestions.find((row) => row.source === "reviewer_draft");
  if (!draft || draft[field] === "") {
    throw new SeBasicInfoDecisionError(`No draft value for ${basicInfoFieldLabel(field)} to discard.`);
  }
  const changes: Partial<Record<SeBasicInfoValueField, string | null>> =
    field === "description" ? { description: null, description_language: null } : { [field]: null };
  const version = suggestionRowVersion(companyId, draft, "reviewer_draft", changes, "discarded", stamp);
  await chInsertSeBasicInfoSuggestions([version]);
  return { decidedAt: stamp };
}

/**
 * One reviewer decision = one new version of this company's precedence rule
 * for the field (slice 3b: a decision is a rule, not a copied value). Use
 * this refuses when the chosen source currently has no opinion on the field,
 * then writes the rule at precedence 10000 (spec 4's reviewer rank) with
 * `removed = 0` -- retiring, in the same insert, every other source's active
 * rule for the field, so at most one rule is ever in force per field. Release
 * writes the same key with `removed = 1` and reads nothing else. Neither
 * touches the folded row or any suggestion -- the next fold applies it.
 */
export async function appendSeBasicInfoRule(
  companyId: string,
  decision: Extract<SeBasicInfoDecision, { intent: "use-this" | "reset" }>,
  now: Date = new Date(),
): Promise<{ decidedAt: string }> {
  const { field, note } = decision;
  const stamp = clickhouseStamp(now);
  const rows: {
    company_id: string;
    field: string;
    source: string;
    precedence: number;
    removed: number;
    decided_by: string;
    note: string;
    decided_at: string;
  }[] = [];
  const retire = (source: string, why: string) =>
    rows.push({
      company_id: companyId,
      field,
      source,
      precedence: 10000,
      removed: 1,
      decided_by: "backoffice",
      note: why,
      decided_at: stamp,
    });
  const activeRules = await chQuery<{ source: string }>(BASIC_INFO_ACTIVE_RULES_SQL, { companyId, field });
  if (decision.intent === "reset") {
    // Reset to default touches two things: every company rule for the field
    // (whatever it ranked, so the global precedence decides again at the next
    // fold) and, when the reviewer typed a value for the field, that value
    // too (slice 3c: a typed value must not survive its own reset). One read
    // of the suggestions serves both the value check and the version base.
    const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
    const reviewer = suggestions.find((row) => row.source === "reviewer");
    const reviewerValue = reviewer?.[field] ?? "";
    if (activeRules.length === 0 && reviewerValue === "") {
      throw new SeBasicInfoDecisionError(
        `No company rule or reviewer value to reset for ${basicInfoFieldLabel(field)}.`,
      );
    }
    const resetNote = note === "" ? "reset to default" : `reset to default: ${note}`;
    if (activeRules.length > 0) {
      for (const rule of activeRules) {
        retire(rule.source, resetNote);
      }
      await chInsertSeBasicInfoPrecedence(rows);
    }
    if (reviewerValue !== "") {
      const changes: Partial<Record<SeBasicInfoValueField, string | null>> =
        field === "description" ? { description: null, description_language: null } : { [field]: null };
      const version = suggestionRowVersion(companyId, reviewer, "reviewer", changes, resetNote, stamp);
      await chInsertSeBasicInfoSuggestions([version]);
    }
    return { decidedAt: stamp };
  }
  const { source } = decision;
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const chosen = suggestions.find((row) => row.source === source);
  const value = chosen?.[field] ?? "";
  if (value === "") {
    throw new SeBasicInfoDecisionError(
      `${basicInfoSourceLabel(source)} has no ${basicInfoFieldLabel(field).toLowerCase()} for this company.`,
    );
  }
  // One preferred source per field: the others' active rules retire in the same write.
  for (const other of activeRules) {
    if (other.source === source) continue;
    retire(other.source, `superseded by ${basicInfoSourceLabel(source)}`);
  }
  rows.push({
    company_id: companyId,
    field,
    source,
    precedence: 10000,
    removed: 0,
    decided_by: "backoffice",
    note,
    decided_at: stamp,
  });
  await chInsertSeBasicInfoPrecedence(rows);
  return { decidedAt: stamp };
}

export const FOLD_NOW_TAG = { "backoffice/basic-info": "fold-now" } as const;

/** Fold now (spec 7): one run of the targeted fold for this company alone. */
export async function launchSeBasicInfoFold(
  companyId: string,
): Promise<{ runId: string; url: string | null }> {
  const run = await launchRun({
    job: ASSET_JOB_NAME,
    assetSelection: [SE_BASIC_INFO_FOLD_COMPANIES_ASSET],
    runConfig: {
      ops: { [SE_BASIC_INFO_FOLD_COMPANIES_ASSET]: { config: { company_ids: [companyId] } } },
    },
    tags: { ...FOLD_NOW_TAG },
  });
  return { runId: run.runId, url: dagsterRunUrl(run.runId) };
}
