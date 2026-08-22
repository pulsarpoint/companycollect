import { createHash, randomUUID } from "node:crypto";
import {
  chInsertSeCompanyPersonCorrections,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  SePersonCorrectionValidationError,
  validateSePersonCorrection,
  ZERO_EVIDENCE_HASH,
  type SePersonCorrectionInput,
} from "~/lib/se-person-corrections";

export { ZERO_EVIDENCE_HASH };
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
  is_current: number;
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

/**
 * The WHERE clause filters through the table alias on purpose. Every id is
 * projected as `toString(x) AS x`, which shadows the underlying column: an
 * unqualified `person_id = {personId:UUID}` compares String with UUID and dies
 * with NO_COMMON_TYPE, and the equivalent `IN` in DRAFTS_SQL matches nothing at
 * all. The output names stay as the TypeScript row types expect.
 */
export const PERSON_SQL = `SELECT
  toString(p.person_id) AS person_id, p.company_id AS company_id,
  p.name AS name, p.description AS description,
  arrayMap(id -> toString(id), p.draft_ids) AS draft_ids,
  toString(p.draft_set_hash) AS draft_set_hash,
  arrayMap(id -> toString(id), p.correction_ids) AS correction_ids,
  toString(p.suggestion_id) AS suggestion_id,
  toString(p.merged_into_person_id) AS merged_into_person_id,
  toString(p.model_provider) AS model_provider, p.model_name AS model_name,
  p.prompt_version AS prompt_version,
  toString(p.updated_at) AS updated_at
FROM corpscout.se_company_person AS p FINAL
WHERE p.company_id = {companyId:String} AND p.person_id = {personId:UUID}
LIMIT 1`;

export const DRAFTS_SQL = `SELECT
  toString(d.draft_id) AS draft_id, toString(d.source) AS source,
  multiIf(
    d.source = 'bolagsverket',
    trim(concat(
      JSONExtractString(source_value_json, 'first_name'), ' ',
      JSONExtractString(source_value_json, 'last_name')
    )),
    JSONExtractString(source_value_json, 'name')
  ) AS name,
  multiIf(
    d.source = 'bolagsverket', JSONExtractString(source_value_json, 'role_original'),
    d.source = 'esef', JSONExtractString(source_value_json, 'role'),
    JSONExtractString(source_value_json, 'role_label')
  ) AS role_original,
  d.fiscal_year AS fiscal_year,
  toString(d.source_observed_at) AS source_observed_at,
  d.source_value_json AS source_value_json
FROM corpscout.se_company_person_draft AS d FINAL
WHERE d.company_id = {companyId:String} AND d.draft_id IN {draftIds:Array(UUID)}
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
  toUInt8(ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)) AS is_published,
  -- "Current" is the closest the backoffice can get to Dagster's request hash:
  -- a suggestion answers the same evidence as the published one when it carries
  -- the same input_hash. Only those may be approved or rejected.
  toUInt8(ifNull(s.input_hash = (
    SELECT input_hash
    FROM corpscout.se_company_person_enrichment_observation
    WHERE suggestion_id = {publishedSuggestionId:Nullable(UUID)}
    LIMIT 1
  ), 0)) AS is_current
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
  -- Staleness belongs to the row's SUBJECT, not to the person whose page this
  -- is: a merge or reassign listed on the destination's page binds the source
  -- person's evidence. A subject that no longer exists joins to '' and is stale.
  toUInt8(
    c.correction_id NOT IN (SELECT id FROM superseded)
    AND toString(c.evidence_hash) != {zeroHash:String}
    AND toString(c.evidence_hash) != toString(subj.draft_set_hash)
  ) AS is_stale,
  toUInt8(has({appliedIds:Array(String)}, toString(c.correction_id))) AS is_applied
FROM corpscout.se_company_person_correction AS c
LEFT JOIN corpscout.se_company_person AS subj FINAL
  ON subj.company_id = c.company_id AND subj.person_id = c.subject_person_id
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
      appliedIds: person.correction_ids,
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
      `SELECT toString(p.draft_set_hash) AS draft_set_hash
       FROM corpscout.se_company_person AS p FINAL
       WHERE p.company_id = {companyId:String} AND p.person_id = {personId:UUID}
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
