import { chInsertSeBasicInfoSuggestions, chQuery } from "~/lib/clickhouse.server";
import type { SeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";
import {
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  foldPending,
  type SeBasicInfoField,
} from "~/lib/se-basic-info-fields";

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

export interface SeBasicInfoHistoryRow extends SeBasicInfoRow {
  changed_fields: string[];
}

export interface SeBasicInfoPrecedenceRow {
  field: string;
  source: string;
  precedence: number;
}

export interface SeBasicInfoLegalFormLabel {
  label_en: string;
  label_sv: string;
}

export interface SeBasicInfoDetail {
  /** Null when the company has suggestions but has never been folded (or
   * has no register legal name, spec 5's publish rule). */
  info: SeBasicInfoRow | null;
  suggestions: SeBasicInfoSuggestionRow[];
  history: SeBasicInfoHistoryRow[];
  precedence: SeBasicInfoPrecedenceRow[];
  /** Keyed by legal-form code: every code on the main row or any suggestion. */
  legalFormLabels: Record<string, SeBasicInfoLegalFormLabel>;
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
  h.company_id AS company_id,
${VALUE_COLUMNS_SQL("h")},
${SOURCE_COLUMNS_SQL("h")},
  h.changed_fields AS changed_fields
FROM corpscout.se_company_basic_info_history AS h
WHERE h.company_id = {companyId:String}
ORDER BY h.folded_at DESC
LIMIT 200`;

export const BASIC_INFO_PRECEDENCE_SQL = `SELECT
  toString(p.field) AS field,
  toString(p.source) AS source,
  toUInt32(p.precedence) AS precedence
FROM corpscout.se_company_basic_info_precedence AS p FINAL
ORDER BY p.field, p.precedence DESC`;

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

export async function loadSeBasicInfoDetail(
  companyId: string,
): Promise<SeBasicInfoDetail | null> {
  const [infoRows, suggestions, history, precedence] = await Promise.all([
    chQuery<SeBasicInfoRow>(BASIC_INFO_SQL, { companyId }),
    chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId }),
    chQuery<SeBasicInfoHistoryRow>(BASIC_INFO_HISTORY_SQL, { companyId }),
    chQuery<SeBasicInfoPrecedenceRow>(BASIC_INFO_PRECEDENCE_SQL),
  ]);
  const info = infoRows[0] ?? null;
  if (!info && suggestions.length === 0) return null;
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
    legalFormLabels,
    foldPending: foldPending(
      info?.folded_at ?? null,
      suggestions.map((row) => row.suggested_at),
    ),
  };
}

export class SeBasicInfoDecisionError extends Error {}

/** ClickHouse's own DateTime64(3) text form, UTC. */
export function clickhouseStamp(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

const VALUE_FIELDS = [
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

type ReviewerRowValues = Record<(typeof VALUE_FIELDS)[number], string | null>;

/** The row as inserted: '' from the reads becomes NULL ("no opinion") here. */
function reviewerValues(row: SeBasicInfoSuggestionRow | undefined): ReviewerRowValues {
  const values = {} as ReviewerRowValues;
  for (const field of VALUE_FIELDS) {
    const value = row?.[field] ?? "";
    values[field] = value === "" ? null : value;
  }
  return values;
}

/**
 * One reviewer decision = one new version of this company's reviewer row
 * (spec 3.2, 7): the current reviewer row's values, one field changed,
 * `observed_at`/`suggested_at` = the decision instant. Use this copies the
 * chosen source's value (and the language with a description); Release sets
 * the field (and that language) back to NULL.
 */
export async function appendSeBasicInfoReviewerDecision(
  companyId: string,
  decision: Exclude<SeBasicInfoDecision, { intent: "fold-now" }>,
  now: Date = new Date(),
): Promise<{ suggestedAt: string }> {
  const suggestions = await chQuery<SeBasicInfoSuggestionRow>(BASIC_INFO_SUGGESTIONS_SQL, { companyId });
  const values = reviewerValues(suggestions.find((row) => row.source === "reviewer"));
  const field: SeBasicInfoField = decision.field;
  if (decision.intent === "use-this") {
    const chosen = suggestions.find((row) => row.source === decision.source);
    const value = chosen?.[field] ?? "";
    if (value === "") {
      throw new SeBasicInfoDecisionError(
        `${basicInfoSourceLabel(decision.source)} has no ${basicInfoFieldLabel(field).toLowerCase()} for this company.`,
      );
    }
    values[field] = value;
    if (field === "description") {
      values.description_language = chosen?.description_language === "" ? null : (chosen?.description_language ?? null);
    }
  } else {
    values[field] = null;
    if (field === "description") values.description_language = null;
  }
  const stamp = clickhouseStamp(now);
  await chInsertSeBasicInfoSuggestions([
    {
      company_id: companyId,
      source: "reviewer",
      source_record_uid: "",
      observed_at: stamp,
      ...values,
      decided_by: "backoffice",
      note: decision.note === "" ? null : decision.note,
      suggested_at: stamp,
      source_run_id: "backoffice",
      extractor_version: "backoffice-v1",
    },
  ]);
  return { suggestedAt: stamp };
}
