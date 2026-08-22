import { createHash, randomUUID } from "node:crypto";
import {
  chInsertSeCompanyPersonCorrections,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  SePersonCorrectionValidationError,
  validateSePersonCorrection,
  type SePersonCorrectionInput,
} from "~/lib/se-person-corrections";

export const ZERO_EVIDENCE_HASH = "0".repeat(64);
const CORRECTION_ACTOR = "backoffice";

export interface SeCompanyPersonRow {
  person_id: string;
  company_id: string;
  name: string;
  description: string | null;
  draft_ids: string[];
  draft_set_hash: string;
  correction_ids: string[];
  suggestion_id: string | null;
  merged_into_person_id: string | null;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  updated_at: string;
}

export interface SeCompanyPersonDraftRow {
  draft_id: string;
  source: string;
  name: string;
  role_original: string;
  fiscal_year: number | null;
  source_observed_at: string;
  source_value_json: string;
}

export interface SeCompanyPersonRoleRow {
  role_id: string;
  role_code: string;
  fiscal_year: number | null;
  sources: string[];
  role_draft_ids: string[];
  person_draft_ids: string[];
  correction_ids: string[];
  is_current: number;
}

export interface SeCompanyPersonSuggestionRow {
  suggestion_id: string;
  input_hash: string;
  draft_ids: string[];
  suggestion: string;
  model_provider: string;
  model_name: string;
  prompt_version: string;
  created_at: string;
  is_published: number;
}

export interface SeCompanyPersonCorrectionRow {
  correction_id: string;
  correction_kind: string;
  subject_person_id: string;
  target_person_id: string | null;
  draft_ids: string[];
  payload: string;
  evidence_hash: string;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  created_at: string;
  is_current: number;
  is_stale: number;
  is_applied: number;
}

export interface SeCompanyPersonDetail {
  person: SeCompanyPersonRow;
  drafts: SeCompanyPersonDraftRow[];
  roles: SeCompanyPersonRoleRow[];
  suggestions: SeCompanyPersonSuggestionRow[];
  corrections: SeCompanyPersonCorrectionRow[];
}

/** Same hash as dagster_v3 normalization.person_id_for: sha256 of company + first|last token key. */
export function seCompanyPersonId(companyId: string, name: string): string {
  const tokens = name.trim().replace(/\s+/g, " ").toLowerCase().split(" ").filter(Boolean);
  const key =
    tokens.length < 2 ? (tokens[0] ?? "") : `${tokens[0]}|${tokens[tokens.length - 1]}`;
  const hex = createHash("sha256")
    .update(`se-company-person-v1\n${companyId}\n${key}`)
    .digest("hex")
    .slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export const PERSON_SQL = `SELECT
  toString(person_id) AS person_id, company_id, name, description,
  arrayMap(id -> toString(id), draft_ids) AS draft_ids,
  toString(draft_set_hash) AS draft_set_hash,
  arrayMap(id -> toString(id), correction_ids) AS correction_ids,
  toString(suggestion_id) AS suggestion_id,
  toString(merged_into_person_id) AS merged_into_person_id,
  toString(model_provider) AS model_provider, model_name, prompt_version,
  toString(updated_at) AS updated_at
FROM corpscout.se_company_person FINAL
WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
LIMIT 1`;

export const DRAFTS_SQL = `SELECT
  toString(draft_id) AS draft_id, toString(source) AS source,
  multiIf(
    source = 'bolagsverket',
    trim(concat(
      JSONExtractString(source_value_json, 'first_name'), ' ',
      JSONExtractString(source_value_json, 'last_name')
    )),
    JSONExtractString(source_value_json, 'name')
  ) AS name,
  multiIf(
    source = 'bolagsverket', JSONExtractString(source_value_json, 'role_original'),
    source = 'esef', JSONExtractString(source_value_json, 'role'),
    JSONExtractString(source_value_json, 'role_label')
  ) AS role_original,
  fiscal_year, toString(source_observed_at) AS source_observed_at, source_value_json
FROM corpscout.se_company_person_draft FINAL
WHERE company_id = {companyId:String} AND draft_id IN {draftIds:Array(UUID)}
ORDER BY source, fiscal_year, draft_id`;

export const ROLES_SQL = `SELECT
  toString(role_id) AS role_id, role_code, fiscal_year, sources,
  arrayMap(id -> toString(id), role_draft_ids) AS role_draft_ids,
  arrayMap(id -> toString(id), person_draft_ids) AS person_draft_ids,
  arrayMap(id -> toString(id), correction_ids) AS correction_ids,
  toUInt8(is_current) AS is_current
FROM corpscout.se_company_person_role FINAL
WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
ORDER BY is_current DESC, fiscal_year DESC NULLS LAST, role_code`;

export const SUGGESTIONS_SQL = `SELECT
  toString(s.suggestion_id) AS suggestion_id, toString(s.input_hash) AS input_hash,
  arrayMap(id -> toString(id), s.draft_ids) AS draft_ids, s.suggestion AS suggestion,
  toString(s.model_provider) AS model_provider, s.model_name, s.prompt_version,
  toString(s.created_at) AS created_at,
  toUInt8(ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)) AS is_published
FROM corpscout.se_company_person_enrichment_observation AS s
WHERE s.company_id = {companyId:String} AND s.person_id = {personId:UUID}
ORDER BY s.created_at DESC
LIMIT 50`;

export const CORRECTIONS_SQL = `WITH superseded AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_person_correction
  WHERE company_id = {companyId:String} AND supersedes_correction_id IS NOT NULL
)
SELECT
  toString(c.correction_id) AS correction_id, c.correction_kind,
  toString(c.subject_person_id) AS subject_person_id,
  toString(c.target_person_id) AS target_person_id,
  arrayMap(id -> toString(id), c.draft_ids) AS draft_ids,
  c.payload, toString(c.evidence_hash) AS evidence_hash, c.reason, c.decided_by,
  toString(c.supersedes_correction_id) AS supersedes_correction_id,
  toString(c.created_at) AS created_at,
  toUInt8(c.correction_id NOT IN (SELECT id FROM superseded)) AS is_current,
  toUInt8(
    c.correction_id NOT IN (SELECT id FROM superseded)
    AND toString(c.evidence_hash) != {zeroHash:String}
    AND toString(c.evidence_hash) != {draftSetHash:String}
  ) AS is_stale,
  toUInt8(has({appliedIds:Array(String)}, toString(c.correction_id))) AS is_applied
FROM corpscout.se_company_person_correction AS c
WHERE c.company_id = {companyId:String}
  AND (c.subject_person_id = {personId:UUID} OR c.target_person_id = {personId:UUID})
ORDER BY c.created_at DESC, c.correction_id DESC
LIMIT 200`;

export async function getSeCompanyPerson(
  companyId: string,
  personId: string,
): Promise<SeCompanyPersonDetail | null> {
  const [person] = await chQuery<SeCompanyPersonRow>(PERSON_SQL, { companyId, personId });
  if (!person) return null;
  const [drafts, roles, suggestions, corrections] = await Promise.all([
    chQuery<SeCompanyPersonDraftRow>(DRAFTS_SQL, { companyId, draftIds: person.draft_ids }),
    chQuery<SeCompanyPersonRoleRow>(ROLES_SQL, { companyId, personId }),
    chQuery<SeCompanyPersonSuggestionRow>(SUGGESTIONS_SQL, {
      companyId, personId, publishedSuggestionId: person.suggestion_id,
    }),
    chQuery<SeCompanyPersonCorrectionRow>(CORRECTIONS_SQL, {
      companyId, personId, zeroHash: ZERO_EVIDENCE_HASH,
      draftSetHash: person.draft_set_hash, appliedIds: person.correction_ids,
    }),
  ]);
  return { person, drafts, roles, suggestions, corrections };
}

function correctionTimestamp(): string {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

export async function appendSeCompanyPersonCorrection(
  input: SePersonCorrectionInput,
): Promise<{ correctionId: string }> {
  const draft = validateSePersonCorrection(input);
  if (draft.correction_kind !== "undo") {
    const [current] = await chQuery<{ draft_set_hash: string }>(
      `SELECT toString(draft_set_hash) AS draft_set_hash
       FROM corpscout.se_company_person FINAL
       WHERE company_id = {companyId:String} AND person_id = {personId:UUID}
       LIMIT 1`,
      { companyId: draft.company_id, personId: draft.subject_person_id },
    );
    if (!current) {
      throw new SePersonCorrectionValidationError("This person is not published.");
    }
    if (current.draft_set_hash !== draft.evidence_hash) {
      throw new SePersonCorrectionValidationError(
        "The evidence changed while you were reviewing. Reload and decide again.",
      );
    }
  }
  const correctionId = randomUUID();
  await chInsertSeCompanyPersonCorrections([
    {
      correction_id: correctionId,
      ...draft,
      decided_by: CORRECTION_ACTOR,
      created_at: correctionTimestamp(),
    },
  ]);
  return { correctionId };
}
