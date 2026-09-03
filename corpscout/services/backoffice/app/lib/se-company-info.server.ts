import { randomUUID } from "node:crypto";
import {
  chInsertSeCompanyInfoFieldValues,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  SeInfoFieldValueValidationError,
  validateSeInfoFieldValue,
  type SeInfoFieldValueInput,
} from "~/lib/se-info-field-values";
import {
  ARTIFACT_PAYLOAD_FIELDS,
  type ArtifactSource,
} from "~/lib/se-company-info-payload";

const DECIDED_BY = "backoffice";

/** Every column of the published corpscout.se_company_info row. */
export interface SeCompanyInfoRow {
  company_id: string;
  legal_name: string;
  legal_form_code: string | null;
  /**
   * What the code is called, both languages, copied onto the row by Dagster
   * from the curated corpscout.se_code_labels dictionary (migration 000306).
   * '' when the dictionary does not name the code. Not Nullable: an absent
   * label is the empty string, exactly as the artifact's LEFT JOIN miss wrote it.
   */
  legal_form_label_en: string;
  legal_form_label_sv: string;
  status: string;
  incorporation_date: string | null;
  description: string | null;
  /** The Swedish half of the published pair (migration 000301); null when there is none. */
  description_sv: string | null;
  description_language: string;
  /**
   * Did the published text come out of the model (migration 000304)? True for
   * the model's merged summary and for an approved suggestion; false for
   * anything copied from one input, for a reviewer's own wording, after a
   * rejection, and when there is no text at all. Typed as a union because a
   * ClickHouse `Bool` arrives from JSONEachRow as a JSON boolean, while
   * INFO_SQL's `toUInt8` cast makes it 0/1 -- either reads correctly as truthy.
   */
  llm_enhanced: number | boolean;
  description_sources: string[];
  description_source_record_uids: string[];
  description_source_count: number;
  primary_nace_code: string;
  primary_sni_code: string;
  wikidata_id: string | null;
  lei: string | null;
  source_record_uids: string[];
  evidence_hashes: string[];
  evidence_set_hash: string;
  correction_ids: string[];
  suggestion_id: string | null;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  source_run_id: string;
  resolved_at: string;
}

export interface SeCompanyInfoArtifactRow {
  source: string;
  source_record_uid: string;
  observed_at: string;
  evidence_hash: string;
  /** Every payload column of this artifact table, name -> text (see
   * ARTIFACT_ROWS_SQL). Parsed server-side from `payload_json`. */
  payload: Record<string, string>;
}

/** What ClickHouse returns for ARTIFACT_ROWS_SQL, before payload_json is parsed. */
interface SeCompanyInfoArtifactQueryRow {
  source: string;
  source_record_uid: string;
  observed_at: string;
  evidence_hash: string;
  payload_json: string;
}

export interface SeCompanyInfoSuggestionRow {
  suggestion_id: string;
  input_hash: string;
  suggestion: string;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  created_at: string;
  is_published: number;
  is_newest: number;
}

/**
 * One row of the company's field-value history. `is_live` marks the row that
 * decides the field right now -- the latest one written for it -- which is the
 * only thing the store ranks by; a row whose `value` is NULL is a release back
 * to the pipeline's computed default, and is as live as any other row.
 */
export interface SeCompanyInfoFieldValueRow {
  value_id: string;
  field: string;
  value: string | null;
  source: string;
  source_ref: string;
  /** Nullable in the table, and toString() over a NULL is still JS null -- a
   * reviewer's own wording has no source moment, so this really does arrive
   * null for the rows this app writes most. */
  source_at: string | null;
  decided_by: string;
  note: string;
  created_at: string;
  is_live: number;
}

export interface SeCompanyInfoDetail {
  info: SeCompanyInfoRow;
  artifacts: SeCompanyInfoArtifactRow[];
  suggestions: SeCompanyInfoSuggestionRow[];
  fieldValues: SeCompanyInfoFieldValueRow[];
  /** English NACE class name for info.primary_nace_code ('' when unknown). */
  naceLabel: string;
}

/**
 * Every column is aliased explicitly (including ones already named the
 * same) so the projected shape never depends on ClickHouse's own naming for
 * a wrapped expression. Hash/UUID-typed columns are wrapped in toString():
 * evidence_set_hash (a MATERIALIZED FixedString) and suggestion_id
 * (Nullable(UUID)) come back as plain strings, and correction_ids
 * (Array(UUID)) is mapped the same way DRAFTS_SQL-style queries do
 * elsewhere in this app. LowCardinality(String) columns (status,
 * description_language, model_provider) are also wrapped, matching
 * se-company-person.server.ts's PERSON_SQL convention, and the Bool
 * llm_enhanced is cast to UInt8 for the same reason: one predictable JSON
 * shape rather than whatever the driver makes of the column type.
 */
export const INFO_SQL = `SELECT
  i.company_id AS company_id,
  i.legal_name AS legal_name,
  i.legal_form_code AS legal_form_code,
  i.legal_form_label_en AS legal_form_label_en,
  i.legal_form_label_sv AS legal_form_label_sv,
  toString(i.status) AS status,
  toString(i.incorporation_date) AS incorporation_date,
  i.description AS description,
  i.description_sv AS description_sv,
  toString(i.description_language) AS description_language,
  toUInt8(i.llm_enhanced) AS llm_enhanced,
  i.description_sources AS description_sources,
  i.description_source_record_uids AS description_source_record_uids,
  i.description_source_count AS description_source_count,
  i.primary_nace_code AS primary_nace_code,
  i.primary_sni_code AS primary_sni_code,
  i.wikidata_id AS wikidata_id,
  i.lei AS lei,
  i.source_record_uids AS source_record_uids,
  i.evidence_hashes AS evidence_hashes,
  toString(i.evidence_set_hash) AS evidence_set_hash,
  arrayMap(id -> toString(id), i.correction_ids) AS correction_ids,
  toString(i.suggestion_id) AS suggestion_id,
  toString(i.model_provider) AS model_provider,
  i.model_name AS model_name,
  i.prompt_version AS prompt_version,
  i.source_run_id AS source_run_id,
  toString(i.resolved_at) AS resolved_at
FROM corpscout.se_company_info AS i FINAL
WHERE i.company_id = {companyId:String}
LIMIT 1`;

const ARTIFACT_TABLES: Record<ArtifactSource, string> = {
  scb: "corpscout.se_company_info_scb",
  esef: "corpscout.se_company_info_esef",
  wikidata: "corpscout.se_company_info_wikidata",
};

/** One leg's payload as a JSON map, built from the ONE payload-column list
 * this app keeps (se-company-info-payload.ts). Mirrors Dagster's
 * `build_artifact_rows_sql`: every value is `ifNull(toString(col), '')`, with
 * the cast INSIDE ifNull because ClickHouse has no common type for a
 * Date/number and '' -- so typed NULLs arrive as '' and numbers as text, and
 * the map's value type stays a uniform String. */
function payloadMapSql(source: ArtifactSource): string {
  const pairs = ARTIFACT_PAYLOAD_FIELDS[source]
    .map((field) => `'${field.key}', ifNull(toString(a.${field.key}), '')`)
    .join(",\n      ");
  return `toJSONString(map(\n      ${pairs}\n    ))`;
}

function artifactLegSql(source: ArtifactSource): string {
  return `  SELECT
    '${source}' AS source,
    a.source_record_uid AS source_record_uid,
    toString(a.observed_at) AS observed_at,
    toString(a.evidence_hash) AS evidence_hash,
    ${payloadMapSql(source)} AS payload_json
  FROM ${ARTIFACT_TABLES[source]} AS a FINAL
  WHERE a.company_id = {companyId:String}`;
}

/**
 * Every artifact leg reads FINAL (each source table is itself a
 * ReplacingMergeTree of versions) and aliases every projected expression, in
 * the same order in every leg (UNION ALL matches columns positionally): the
 * envelope the review page shows -- source, record uid, observed_at,
 * evidence_hash -- then the leg's FULL payload as one JSON map. The review
 * page is the hub for this company, so it renders every payload column rather
 * than one pre-picked summary; picking which text to show is a display
 * decision, not a query one.
 *
 * ClickHouse only applies a trailing ORDER BY to the last SELECT of a UNION
 * ALL chain, so the three legs are wrapped in a subquery and the ORDER BY sits
 * outside it, over the union's own output columns. The `source_record_uid`
 * tiebreak matters in practice: bulk-loaded SCB rows routinely share one
 * `observed_at`. ESEF grows one row per filing, so (unlike
 * INFO_SQL/SUGGESTIONS_SQL/FIELD_VALUES_SQL, which are bounded by construction)
 * this one needs its own LIMIT.
 */
export const ARTIFACT_ROWS_SQL = `SELECT * FROM (
${artifactLegSql("scb")}
  UNION ALL
${artifactLegSql("esef")}
  UNION ALL
${artifactLegSql("wikidata")}
)
ORDER BY source, observed_at DESC, source_record_uid
LIMIT 500`;

/**
 * is_newest is the company's newest observation by (created_at,
 * suggestion_id) -- the only one Dagster's reuse/staleness logic (input_hash
 * matching) would even consider live, so it is the only one a reviewer may
 * approve or reject. is_published is a plain equality against the
 * published row's suggestion_id, not a hash comparison.
 */
export const SUGGESTIONS_SQL = `SELECT
  toString(s.suggestion_id) AS suggestion_id,
  toString(s.input_hash) AS input_hash,
  s.suggestion AS suggestion,
  toString(s.model_provider) AS model_provider,
  s.model_name AS model_name,
  s.prompt_version AS prompt_version,
  toString(s.created_at) AS created_at,
  toUInt8(ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)) AS is_published,
  toUInt8(s.suggestion_id = (
    SELECT suggestion_id
    FROM corpscout.se_company_info_enrichment_observation
    WHERE company_id = {companyId:String}
    ORDER BY created_at DESC, suggestion_id DESC
    LIMIT 1
  )) AS is_newest
FROM corpscout.se_company_info_enrichment_observation AS s
WHERE s.company_id = {companyId:String}
ORDER BY s.created_at DESC
LIMIT 50`;

/**
 * The company's whole field-value history, newest first, with the live row per
 * field flagged.
 *
 * The `live` subquery picks that row exactly the way Dagster's
 * `apply_field_values` does -- the greatest `(created_at, str(value_id))` --
 * so the badge this page shows and the value the next pipeline run publishes
 * can never disagree. The uuid is compared as TEXT on purpose: ClickHouse
 * would otherwise break a same-timestamp tie by the UUID's bytes while Python
 * breaks it by the string, and the two layers would pick different rows.
 *
 * Nothing here depends on the published row: which value is live is decided
 * by the store alone (no evidence hash, no applied-id list, no kinds), so the
 * query takes only the company. Bounded like the other detail queries -- a
 * reviewer decides a handful of values per company, so 200 rows is the whole
 * story with room to spare.
 */
export const FIELD_VALUES_SQL = `SELECT
  toString(v.value_id) AS value_id,
  v.field AS field,
  v.value AS value,
  toString(v.source) AS source,
  v.source_ref AS source_ref,
  toString(v.source_at) AS source_at,
  v.decided_by AS decided_by,
  v.note AS note,
  toString(v.created_at) AS created_at,
  toUInt8(v.value_id = live.value_id) AS is_live
FROM corpscout.se_company_info_field_value AS v
LEFT JOIN (
  SELECT field, argMax(value_id, (created_at, toString(value_id))) AS value_id
  FROM corpscout.se_company_info_field_value
  WHERE company_id = {companyId:String}
  GROUP BY field
) AS live ON live.field = v.field
WHERE v.company_id = {companyId:String}
ORDER BY v.created_at DESC, v.value_id DESC
LIMIT 200`;

/**
 * `payload_json` is a ClickHouse `map(String, String)` rendered by
 * toJSONString, so every value is already text. Parsed defensively anyway: a
 * malformed payload must leave the rest of the review page renderable rather
 * than 500 the whole company, and a non-string value (impossible from that
 * map, but cheap to guard) is stringified rather than handed to React.
 */
function parseArtifactPayload(raw: string): Record<string, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
  const payload: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    payload[key] = typeof value === "string" ? value : String(value ?? "");
  }
  return payload;
}

/**
 * Loads a company's published info row plus its artifact provenance, model
 * suggestions and field-value history for the review page. Returns null when
 * the company has no published row (nothing to review yet).
 */
// Published rows carry dot-less codes ("6419") while the catalog's `code` is
// dotted ("64.19") -- normalized_code covers that form.
export const NACE_LABEL_SQL = `SELECT description_en
FROM corpscout.nace_categories
WHERE classification_version = 'NACE_REV_2'
  AND (code = {code:String}
    OR normalized_code = replaceAll({code:String}, '.', ''))
LIMIT 1`;

async function naceLabelFor(code: string): Promise<string> {
  if (code === "") return "";
  const rows = await chQuery<{ description_en: string }>(NACE_LABEL_SQL, {
    code,
  });
  const description = rows[0]?.description_en ?? "";
  // description_en repeats the code ("62.01 Computer programming activities");
  // the strip shows the code separately, so drop that leading token.
  return description.replace(/^[0-9][0-9.]*\s+/, "");
}

export async function loadSeCompanyInfoDetail(
  companyId: string,
): Promise<SeCompanyInfoDetail | null> {
  const [info] = await chQuery<SeCompanyInfoRow>(INFO_SQL, { companyId });
  if (!info) return null;
  const [artifactRows, suggestions, fieldValues, naceLabel] =
    await Promise.all([
      chQuery<SeCompanyInfoArtifactQueryRow>(ARTIFACT_ROWS_SQL, { companyId }),
      chQuery<SeCompanyInfoSuggestionRow>(SUGGESTIONS_SQL, {
        companyId,
        publishedSuggestionId: info.suggestion_id,
      }),
      chQuery<SeCompanyInfoFieldValueRow>(FIELD_VALUES_SQL, { companyId }),
      naceLabelFor(info.primary_nace_code ?? ""),
    ]);
  const artifacts: SeCompanyInfoArtifactRow[] = artifactRows.map((row) => ({
    source: row.source,
    source_record_uid: row.source_record_uid,
    observed_at: row.observed_at,
    evidence_hash: row.evidence_hash,
    payload: parseArtifactPayload(row.payload_json),
  }));
  return { info, artifacts, suggestions, fieldValues, naceLabel };
}

/** ClickHouse's DateTime64(3) text form, which is what the driver's
 * JSONEachRow insert needs (an ISO string's `T`/`Z` are not that form). */
function valueTimestamp(): string {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

/** The company must already have a published row: a field value for a company
 * Dagster has never published has nothing to decide, and the reviewer would
 * never see it take effect. */
const PUBLISHED_CHECK_SQL = `SELECT 1
FROM corpscout.se_company_info FINAL
WHERE company_id = {companyId:String}
LIMIT 1`;

/**
 * Appends one decision -- which may be several rows, e.g. both languages of an
 * About-card choice -- to the field-value store, and returns the ids written.
 *
 * The whole batch is validated before ClickHouse is touched at all, so a bad
 * row cannot leave the good half of a decision behind. All rows must name the
 * same company: the published check below is per company, and a mixed batch
 * would leave part of it unchecked.
 */
export async function appendSeCompanyInfoFieldValues(
  inputs: SeInfoFieldValueInput[],
): Promise<{ valueIds: string[] }> {
  const drafts = inputs.map(validateSeInfoFieldValue);
  const [first] = drafts;
  if (!first) {
    throw new SeInfoFieldValueValidationError("Nothing to write.");
  }
  if (drafts.some((draft) => draft.company_id !== first.company_id)) {
    throw new SeInfoFieldValueValidationError(
      "Every value in one write must belong to the same company.",
    );
  }
  // One field, one row: the whole batch shares a created_at (below), so two
  // rows for the same field would tie there and the live one would fall to the
  // uuid-text tie-break -- a coin flip between two things the reviewer meant
  // in some order. Refuse instead of writing an arbitrary winner.
  if (new Set(drafts.map((draft) => draft.field)).size !== drafts.length) {
    throw new SeInfoFieldValueValidationError(
      "Each field may appear only once per decision.",
    );
  }
  const [published] = await chQuery<Record<string, unknown>>(
    PUBLISHED_CHECK_SQL,
    { companyId: first.company_id },
  );
  if (!published) {
    throw new SeInfoFieldValueValidationError("This company is not published.");
  }
  // One timestamp for the whole batch: the rows are one decision, and giving
  // them the same created_at keeps the per-field tie-break (created_at, then
  // the uuid's text) deciding between rows of DIFFERENT decisions only.
  const createdAt = valueTimestamp();
  const rows = drafts.map((draft) => ({
    value_id: randomUUID(),
    ...draft,
    decided_by: DECIDED_BY,
    created_at: createdAt,
  }));
  await chInsertSeCompanyInfoFieldValues(rows);
  return { valueIds: rows.map((row) => row.value_id) };
}
