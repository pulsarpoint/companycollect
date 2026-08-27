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
import {
  parseMergeSuggestionPayload,
  type SeMergeSuggestionPayload,
} from "~/lib/se-person-merge-suggestions";

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

/** One live ledger row the pipeline can no longer apply (spec §4.3). */
export interface SeStaleCorrectionRow {
  company_id: string;
  correction_id: string;
  correction_kind: string;
  subject_person_id: string;
  reason: string;
  decided_by: string;
  created_at: string;
  subject_missing: number;
  evidence_moved: number;
  drafts_missing: number;
}

export interface SeCompanyPersonDetail {
  person: SeCompanyPersonRow;
  drafts: SeCompanyPersonDraftRow[];
  roles: SeCompanyPersonRoleRow[];
  suggestions: SeCompanyPersonSuggestionRow[];
  corrections: SeCompanyPersonCorrectionRow[];
}

/**
 * Same hash as dagster_v3 normalization.person_id_for: sha256 of the v2 domain, the company,
 * and an already-canonical group key.
 *
 * SE People Experiment Task 3 moved person_id off the v1 domain (first|last token, K1) to
 * v2, keyed by K3 production (dagster_v3 company_people/identity_eval.py +
 * normalization._company_person_group_keys). This caller only ever has a single free-text
 * name in hand (`row.name` from the legacy DuckDB draft-two admin page, a preview link — see
 * admin-se-people.tsx) with no company-wide observation set to run K3's reconciliation pass
 * over, so it cannot reproduce K3 here. What it CAN reproduce is K2 (all tokens, casefolded,
 * whitespace-normalized) -- a singleton K3 group's canonical key is trivially its own K2 key,
 * which is exactly the "already-canonical key" person_id_for hashes on the Python side. This
 * mirrors dagster_v3's identity_eval.identity_key_k2 modulo one pre-existing, unchanged
 * discrepancy: Python's str.casefold() vs this function's toLowerCase() (immaterial for
 * Swedish å/ä/ö, which this v1 implementation already relied on).
 */
export function seCompanyPersonId(companyId: string, name: string): string {
  const key = name.trim().replace(/\s+/g, " ").toLowerCase();
  const hex = createHash("sha256")
    .update(`se-company-person-v2\n${companyId}\n${key}`)
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

/**
 * SE People Experiment Task 5: `se_company_person_draft` is retired (Task 3
 * already moved normalization/roles off it; this is the last backoffice
 * reader). The evidence panel now reads the three source views directly --
 * `se_company_person_bolagsverket` / `se_company_person_esef` /
 * `se_company_person_wikidata` (migration 000330/000331) -- through a
 * hand-ported TS mirror of dagster_v3's shared `source_observations` CTE
 * (`company_people/source_views.py`,
 * `build_se_company_person_source_observations_sql`). This has to be a real
 * port, not a paraphrase: `draft_id` here MUST hash to the exact same UUID
 * Dagster's SQL computes for the same row, because `person.draft_ids` (the
 * `IN {draftIds:...}` filter below) was populated by THAT formula. Every
 * piece is copied verbatim: the v2 hash domain, the per-branch row-level
 * disambiguator folded into the hash (`signatory_uid` / `candidate_uid` /
 * `company_wikidata_id` -- fixes the same "two rows collide onto one
 * draft_id" bug Task 3's fix round found), and ClickHouse's
 * `reinterpretAsUUID(unhex(...))` byte-reversal quirk (inherited for free:
 * this is the same SQL text, not a Python-side reimplementation of it).
 *
 * The per-branch `source_value_json` shape also mirrors source_views.py
 * field-for-field (bolagsverket: first_name/last_name/role_original/
 * role_kind/signatory_kind; esef: name/role/role_category/...; wikidata:
 * name/role_property/role_label/description/...), which is what keeps the
 * `name`/`role_original` derivation below unchanged from the old
 * draft-table version -- those two `multiIf`s only ever read keys this
 * shape already carries.
 *
 * `company_id = {companyId:String}` is pushed into each branch's WHERE
 * (Dagster's own shared CTE does not scope by company -- callers there
 * already work company-batch-at-a-time upstream); pushing it here keeps a
 * single person's evidence read from re-scanning every SE person in all
 * three source tables.
 */
const SOURCE_OBSERVATION_HASH_DOMAIN = "se-company-person-source-observation-v2";

function sourceObservationIdSql(sourceLiteral: string, disambiguator: string): string {
  return `reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            '${SOURCE_OBSERVATION_HASH_DOMAIN}\\n',
            company_id, '\\n', ${sourceLiteral}, '\\n', toString(source_record_uid), '\\n',
            toString(person_profile_hash), '\\n', toString(person_role_hash), '\\n',
            toString(${disambiguator})
        ))), 1, 32)))`;
}

const SOURCE_OBSERVATIONS_CTE = `source_observations AS (
    SELECT
        'bolagsverket' AS source,
        company_id,
        full_name,
        if(
            fiscal_year > 0,
            toNullable(toUInt16(fiscal_year)),
            CAST(NULL, 'Nullable(UInt16)')
        ) AS fiscal_year,
        source_observed_at,
        ${sourceObservationIdSql("'bolagsverket'", "signatory_uid")} AS draft_id,
        toJSONString(CAST(tuple(
            first_name, last_name, role_original, role_kind, signatory_kind
        ) AS Tuple(
            first_name String, last_name String, role_original String, role_kind String,
            signatory_kind String
        ))) AS source_value_json
    FROM corpscout.se_company_person_bolagsverket
    WHERE trim(full_name) != '' AND company_id = {companyId:String}

    UNION ALL

    SELECT
        'esef' AS source,
        company_id,
        full_name,
        toNullable(fiscal_year) AS fiscal_year,
        source_observed_at,
        ${sourceObservationIdSql("'esef'", "candidate_uid")} AS draft_id,
        toJSONString(CAST(tuple(
            full_name, role, role_category, organization, status, effective_from,
            effective_to, confidence
        ) AS Tuple(
            name String, role String, role_category String, organization String,
            status String, effective_from Nullable(Date32), effective_to Nullable(Date32),
            confidence Float32
        ))) AS source_value_json
    FROM corpscout.se_company_person_esef
    WHERE trim(full_name) != '' AND company_id = {companyId:String}

    UNION ALL

    SELECT
        'wikidata' AS source,
        company_id,
        full_name,
        CAST(NULL, 'Nullable(UInt16)') AS fiscal_year,
        source_observed_at,
        ${sourceObservationIdSql("'wikidata'", "company_wikidata_id")} AS draft_id,
        toJSONString(CAST(tuple(
            full_name, role_property, role_label, ifNull(description, ''),
            person_wikidata_id, start_date, end_date, birth_year
        ) AS Tuple(
            name String, role_property String, role_label String, description String,
            person_wikidata_id String, start_date Nullable(Date), end_date Nullable(Date),
            birth_year Nullable(UInt16)
        ))) AS source_value_json
    FROM corpscout.se_company_person_wikidata
    WHERE trim(full_name) != '' AND company_id = {companyId:String}
)`;

export const DRAFTS_SQL = `WITH ${SOURCE_OBSERVATIONS_CTE}
SELECT
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
  fiscal_year AS fiscal_year,
  toString(source_observed_at) AS source_observed_at,
  source_value_json AS source_value_json
FROM source_observations
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

/**
 * Every live correction the pipeline will skip, across companies: the reviewer
 * has to be able to find them again, not only see a count in asset metadata.
 * The three reasons are reported separately so the page can say which one it is.
 */
export const STALE_CORRECTIONS_SQL = `WITH superseded AS (
  SELECT supersedes_correction_id AS id
  FROM corpscout.se_company_person_correction
  WHERE supersedes_correction_id IS NOT NULL
)
SELECT
  c.company_id AS company_id,
  toString(c.correction_id) AS correction_id,
  c.correction_kind AS correction_kind,
  toString(c.subject_person_id) AS subject_person_id,
  c.reason AS reason,
  c.decided_by AS decided_by,
  toString(c.created_at) AS created_at,
  toUInt8(subj.company_id = '') AS subject_missing,
  toUInt8(
    subj.company_id != ''
    AND toString(subj.draft_set_hash) != toString(c.evidence_hash)
  ) AS evidence_moved,
  toUInt8(
    subj.company_id != ''
    AND notEmpty(c.draft_ids)
    AND NOT hasAll(subj.draft_ids, c.draft_ids)
  ) AS drafts_missing
FROM corpscout.se_company_person_correction AS c
LEFT JOIN corpscout.se_company_person AS subj FINAL
  ON subj.company_id = c.company_id AND subj.person_id = c.subject_person_id
WHERE c.correction_id NOT IN (SELECT id FROM superseded)
  AND c.correction_kind != 'undo'
  AND toString(c.evidence_hash) != {zeroHash:String}
  AND (
    subj.company_id = ''
    OR toString(subj.draft_set_hash) != toString(c.evidence_hash)
    OR (notEmpty(c.draft_ids) AND NOT hasAll(subj.draft_ids, c.draft_ids))
  )
ORDER BY c.created_at DESC
LIMIT 500`;

export async function listStaleSeCompanyPersonCorrections(): Promise<
  SeStaleCorrectionRow[]
> {
  return chQuery<SeStaleCorrectionRow>(STALE_CORRECTIONS_SQL, {
    zeroHash: ZERO_EVIDENCE_HASH,
  });
}

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

/* -------------------------------------------------------------------- */
/* Collision-candidate + merge-suggestion review (SE People Experiment   */
/* Task 5). Groups come from Task 2's se_company_person_collision_candidate */
/* table; a group's suggestion (if any) comes from Task 4's merge asset,    */
/* which writes it into se_company_person_enrichment_observation, the SAME */
/* table normalization.py's profile-suggestion path already uses.          */
/* -------------------------------------------------------------------- */

export interface SeCollisionCandidateMember {
  person_key: string;
  full_name: string;
  source: string;
  source_record_uid: string;
}

export interface SeCollisionCandidateSuggestion {
  suggestion_id: string;
  decision: "merge" | "keep_separate";
  confidence: number;
  rationale: string;
  into_person_id: string;
  from_person_ids: string[];
  member_person_ids: string[];
  created_at: string;
}

export interface SeCollisionCandidateGroup {
  candidate_group_id: string;
  members: SeCollisionCandidateMember[];
  /** The most recent merge suggestion filed against this group, if any. A
   * group with none has not been through se_company_person_merge_job yet. */
  suggestion: SeCollisionCandidateSuggestion | null;
  /** A human has already ruled on this group (merge_persons or keep_separate,
   * not superseded by a later undo) -- mirrors merge.py's own decided-marker
   * read exactly (same kinds, same undo-exclusion), so a group this page
   * still offers Approve/Keep separate on is a group Dagster's merge job
   * would still consider open too. */
  is_decided: boolean;
}

export const COLLISION_CANDIDATES_SQL = `SELECT
  candidate_group_id AS candidate_group_id, person_key AS person_key,
  full_name AS full_name, source AS source, source_record_uid AS source_record_uid
FROM corpscout.se_company_person_collision_candidate
WHERE company_id = {companyId:String}
ORDER BY candidate_group_id, source, full_name`;

/** Mirrors merge.py's own decided-marker read: both decision kinds, a payload
 * naming this group, excluding a decision a later undo superseded -- so a
 * group this query still calls open is a group the merge asset would still
 * consider open too. */
export const DECIDED_CANDIDATE_GROUPS_SQL = `SELECT DISTINCT
  JSONExtractString(payload, 'candidate_group_id') AS candidate_group_id
FROM corpscout.se_company_person_correction
WHERE company_id = {companyId:String}
  AND correction_kind IN ('merge_persons', 'keep_separate')
  AND JSONExtractString(payload, 'candidate_group_id') != ''
  AND correction_id NOT IN (
    SELECT supersedes_correction_id
    FROM corpscout.se_company_person_correction
    WHERE company_id = {companyId:String} AND supersedes_correction_id IS NOT NULL
  )`;

/** Every merge suggestion ever written for this company (any subject person),
 * newest first -- unlike SUGGESTIONS_SQL, not scoped to one person_id, because
 * a merge suggestion is filed under its group's into_person_id, and a
 * collision group's other members would otherwise never see it on their own
 * page. Rows this reader did not write (an ordinary profile suggestion,
 * sharing the same table) fail parseMergeSuggestionPayload and are dropped. */
export const MERGE_SUGGESTIONS_FOR_COMPANY_SQL = `SELECT
  toString(s.suggestion_id) AS suggestion_id, s.suggestion AS suggestion,
  toString(s.created_at) AS created_at
FROM corpscout.se_company_person_enrichment_observation AS s
WHERE s.company_id = {companyId:String}
  AND JSONExtractString(s.suggestion, 'candidate_group_id') != ''
ORDER BY s.created_at DESC
LIMIT 200`;

export async function loadSeCompanyPersonCollisionReview(
  companyId: string,
): Promise<SeCollisionCandidateGroup[]> {
  const [candidateRows, suggestionRows, decidedRows] = await Promise.all([
    chQuery<SeCollisionCandidateMember & { candidate_group_id: string }>(
      COLLISION_CANDIDATES_SQL,
      { companyId },
    ),
    chQuery<{ suggestion_id: string; suggestion: string; created_at: string }>(
      MERGE_SUGGESTIONS_FOR_COMPANY_SQL,
      { companyId },
    ),
    chQuery<{ candidate_group_id: string }>(DECIDED_CANDIDATE_GROUPS_SQL, { companyId }),
  ]);

  const decided = new Set(
    decidedRows.map((row) => row.candidate_group_id).filter((id) => id !== ""),
  );

  // Newest first (query order): the first suggestion seen per group is kept.
  const suggestionByGroup = new Map<string, SeCollisionCandidateSuggestion>();
  for (const row of suggestionRows) {
    const payload = parseMergeSuggestionPayload(row.suggestion);
    if (!payload || suggestionByGroup.has(payload.candidate_group_id)) continue;
    suggestionByGroup.set(payload.candidate_group_id, {
      suggestion_id: row.suggestion_id,
      decision: payload.decision,
      confidence: payload.confidence,
      rationale: payload.rationale,
      into_person_id: payload.into_person_id,
      from_person_ids: payload.from_person_ids,
      member_person_ids: payload.member_person_ids,
      created_at: row.created_at,
    });
  }

  const groups = new Map<string, SeCollisionCandidateGroup>();
  for (const row of candidateRows) {
    let group = groups.get(row.candidate_group_id);
    if (!group) {
      group = {
        candidate_group_id: row.candidate_group_id,
        members: [],
        suggestion: suggestionByGroup.get(row.candidate_group_id) ?? null,
        is_decided: decided.has(row.candidate_group_id),
      };
      groups.set(row.candidate_group_id, group);
    }
    group.members.push({
      person_key: row.person_key,
      full_name: row.full_name,
      source: row.source,
      source_record_uid: row.source_record_uid,
    });
  }
  return [...groups.values()];
}

export const MERGE_SUGGESTION_BY_ID_SQL = `SELECT
  toString(s.suggestion_id) AS suggestion_id,
  s.suggestion AS suggestion,
  arrayMap(id -> toString(id), s.draft_ids) AS draft_ids
FROM corpscout.se_company_person_enrichment_observation AS s
WHERE s.company_id = {companyId:String} AND s.suggestion_id = {suggestionId:UUID}
LIMIT 1`;

/** The group's currently-published, non-tombstoned people and the drafts they
 * hold right now -- read fresh, never trusted from the suggestion row, which
 * may be arbitrarily old by the time a human reviews it. */
export const MERGE_GROUP_LIVE_SQL = `SELECT
  toString(person_id) AS person_id,
  toString(draft_set_hash) AS draft_set_hash,
  arrayMap(id -> toString(id), draft_ids) AS draft_ids,
  toUInt8(merged_into_person_id IS NULL) AS is_live
FROM corpscout.se_company_person FINAL
WHERE company_id = {companyId:String} AND person_id IN {personIds:Array(UUID)}`;

interface MergeGroupLiveRow {
  person_id: string;
  draft_set_hash: string;
  draft_ids: string[];
  is_live: number;
}

export type SeMergeRevalidation =
  | { ok: true; evidenceHashByPersonId: Record<string, string> }
  | { ok: false; reason: string };

/**
 * CARRY-FORWARD REQUIREMENT from Task 4's review: a merge suggestion's
 * into/from ids can go stale between when the model answered and when a human
 * approves it (new evidence merged/split the people involved). This
 * re-reads se_company_person live and refuses with a clear reason rather than
 * trusting the suggestion's ids -- both halves the brief asks for: the
 * into/from ids must still exist un-tombstoned, AND (going further than the
 * "at minimum" bar) the suggestion's own draft_ids must still be owned by
 * SOME member of the group, so evidence a correction moved out of the group
 * entirely since the suggestion was written cannot be silently re-merged.
 */
export async function revalidateMergeSuggestion(
  companyId: string,
  payload: SeMergeSuggestionPayload,
  suggestionDraftIds: readonly string[],
): Promise<SeMergeRevalidation> {
  const groupPersonIds = [payload.into_person_id, ...payload.from_person_ids];
  const rows = await chQuery<MergeGroupLiveRow>(MERGE_GROUP_LIVE_SQL, {
    companyId,
    personIds: groupPersonIds,
  });
  const byId = new Map(rows.map((row) => [row.person_id, row]));

  const missing = groupPersonIds.filter((id) => !byId.has(id));
  if (missing.length > 0) {
    return {
      ok: false,
      reason:
        `This suggestion is stale: ${missing.join(", ")} ` +
        `${missing.length === 1 ? "is" : "are"} no longer published for this company.`,
    };
  }
  const tombstoned = groupPersonIds.filter((id) => byId.get(id)?.is_live === 0);
  if (tombstoned.length > 0) {
    return {
      ok: false,
      reason:
        `This suggestion is stale: ${tombstoned.join(", ")} ` +
        `${tombstoned.length === 1 ? "was" : "were"} already merged elsewhere.`,
    };
  }
  const liveDraftIds = new Set(rows.flatMap((row) => row.draft_ids));
  const movedDrafts = suggestionDraftIds.filter((id) => !liveDraftIds.has(id));
  if (movedDrafts.length > 0) {
    return {
      ok: false,
      reason:
        "This suggestion is stale: the underlying evidence moved since it was written. " +
        "Reload and reconsider.",
    };
  }
  return {
    ok: true,
    evidenceHashByPersonId: Object.fromEntries(
      rows.map((row) => [row.person_id, row.draft_set_hash]),
    ),
  };
}

async function loadMergeSuggestionForReview(
  companyId: string,
  suggestionId: string,
): Promise<{ payload: SeMergeSuggestionPayload; draftIds: string[] }> {
  const [row] = await chQuery<{ suggestion_id: string; suggestion: string; draft_ids: string[] }>(
    MERGE_SUGGESTION_BY_ID_SQL,
    { companyId, suggestionId },
  );
  if (!row) throw new SePersonCorrectionValidationError("Suggestion not found.");
  const payload = parseMergeSuggestionPayload(row.suggestion);
  if (!payload) {
    throw new SePersonCorrectionValidationError(
      "This suggestion is not a recognizable merge suggestion.",
    );
  }
  return { payload, draftIds: row.draft_ids };
}

/**
 * Fresh is_live + draft_set_hash for exactly `personIds`, read a SECOND time
 * (revalidateMergeSuggestion already read it once, upfront) immediately
 * before the INSERT that decides a group. Cannot be replaced by
 * appendSeCompanyPersonCorrection's own hash recheck: merge-tombstoning
 * (normalization.py's merge_persons handler) sets `merged_into_person_id`
 * WITHOUT touching `draft_ids`/`draft_set_hash`, so a hash-only comparison
 * cannot see a subject tombstoned by ANOTHER correction landing in the gap
 * between the upfront revalidation and this write. Refuses the whole write
 * (throws, nothing inserted) if any subject is no longer live.
 */
async function requireLiveAtWriteTime(
  companyId: string,
  personIds: readonly string[],
): Promise<Record<string, string>> {
  const rows = await chQuery<MergeGroupLiveRow>(MERGE_GROUP_LIVE_SQL, { companyId, personIds });
  const byId = new Map(rows.map((row) => [row.person_id, row]));
  const stale = personIds.filter((id) => byId.get(id)?.is_live !== 1);
  if (stale.length > 0) {
    throw new SePersonCorrectionValidationError(
      `This suggestion went stale while saving: ${stale.join(", ")} ` +
        `${stale.length === 1 ? "is" : "are"} no longer a live, published person. Reload and reconsider.`,
    );
  }
  return Object.fromEntries(rows.map((row) => [row.person_id, row.draft_set_hash]));
}

function correctionRow(
  draft: ReturnType<typeof validateSePersonCorrection>,
  createdAt: string,
): Record<string, unknown> {
  return {
    correction_id: randomUUID(),
    ...draft,
    decided_by: CORRECTION_ACTOR,
    created_at: createdAt,
  };
}

/**
 * Approving a merge suggestion writes one merge_persons correction PER
 * from_person_id -- apply_person_corrections' merge_persons handler
 * (normalization.py ~1153-1169) reads only a correction row's own singular
 * `subject_person_id`/`target_person_id`; it never reads `payload` at all,
 * and neither `PersonCorrection` (corrections.py) nor `CORRECTION_COLUMNS`
 * has an array-of-subjects field. A single correction row carrying
 * `{into_person_id, from_person_ids: [...]}"` in its payload would therefore
 * NOT be honored -- Dagster would silently merge only whichever one
 * `subject_person_id` that row names and ignore the rest. True single-row
 * atomicity for an N-way merge needs a dagster_v3-side schema change (an
 * array subject column + updated apply logic), which is out of this
 * backoffice-only task's scope -- flagged for the controller, not
 * implemented here.
 *
 * What IS achievable without touching dagster_v3: every row this call needs
 * is built and validated FIRST, re-checked live immediately before writing,
 * and sent in ONE `chInsertSeCompanyPersonCorrections` call -- a single
 * ClickHouse INSERT of N rows is atomic at the client/network level (it
 * either lands as one accepted request or the call throws and nothing is
 * written), unlike N sequential single-row `insert()` calls, each of which
 * carries its own independent failure window. A group can therefore never be
 * observed half-merged: either every from_person_id's correction exists, or
 * none of them do, so DECIDED_CANDIDATE_GROUPS_SQL (which flips a group to
 * "decided" the instant ANY correction names it) can never see a partial
 * group.
 */
export async function approveMergeSuggestion(input: {
  companyId: string;
  suggestionId: string;
  reason: string;
}): Promise<{ correctionIds: string[] }> {
  const { payload, draftIds } = await loadMergeSuggestionForReview(
    input.companyId,
    input.suggestionId,
  );
  if (payload.from_person_ids.length === 0) {
    throw new SePersonCorrectionValidationError(
      "This suggestion names no people to merge away.",
    );
  }
  const revalidation = await revalidateMergeSuggestion(input.companyId, payload, draftIds);
  if (!revalidation.ok) throw new SePersonCorrectionValidationError(revalidation.reason);

  const drafts = payload.from_person_ids.map((fromPersonId) =>
    validateSePersonCorrection({
      companyId: input.companyId,
      kind: "merge_persons",
      subjectPersonId: fromPersonId,
      targetPersonId: payload.into_person_id,
      payload: { candidate_group_id: payload.candidate_group_id },
      evidenceHash: revalidation.evidenceHashByPersonId[fromPersonId],
      reason: input.reason,
      activeRoleCodes: new Set(),
    }),
  );

  // Immediately-before-INSERT recheck (is_live, not just hash -- see
  // requireLiveAtWriteTime's docstring for why): if anything changed in the
  // gap since the upfront revalidation above, this throws and NOTHING below
  // is written.
  const freshHashes = await requireLiveAtWriteTime(input.companyId, [
    payload.into_person_id,
    ...payload.from_person_ids,
  ]);
  for (const draft of drafts) {
    if (freshHashes[draft.subject_person_id] !== draft.evidence_hash) {
      throw new SePersonCorrectionValidationError(
        "The evidence changed the instant before saving. Reload and reconsider.",
      );
    }
  }

  const createdAt = correctionTimestamp();
  const rows = drafts.map((draft) => correctionRow(draft, createdAt));
  await chInsertSeCompanyPersonCorrections(rows);
  return { correctionIds: rows.map((row) => row.correction_id as string) };
}

/**
 * Keeping a group separate writes one keep_separate correction, anchored on
 * the group's into_person_id (an arbitrary but real, currently-published
 * member -- keep_separate moves no evidence, so which member anchors it is
 * not semantically meaningful, only that it be a real, current one). Same
 * two-read shape as approveMergeSuggestion: revalidated upfront, then
 * re-checked live immediately before the INSERT.
 */
export async function keepSeparateMergeSuggestion(input: {
  companyId: string;
  suggestionId: string;
  reason: string;
}): Promise<{ correctionId: string }> {
  const { payload, draftIds } = await loadMergeSuggestionForReview(
    input.companyId,
    input.suggestionId,
  );
  const revalidation = await revalidateMergeSuggestion(input.companyId, payload, draftIds);
  if (!revalidation.ok) throw new SePersonCorrectionValidationError(revalidation.reason);

  const draft = validateSePersonCorrection({
    companyId: input.companyId,
    kind: "keep_separate",
    subjectPersonId: payload.into_person_id,
    payload: { candidate_group_id: payload.candidate_group_id },
    evidenceHash: revalidation.evidenceHashByPersonId[payload.into_person_id],
    reason: input.reason,
    activeRoleCodes: new Set(),
  });

  const freshHashes = await requireLiveAtWriteTime(input.companyId, [payload.into_person_id]);
  if (freshHashes[payload.into_person_id] !== draft.evidence_hash) {
    throw new SePersonCorrectionValidationError(
      "The evidence changed the instant before saving. Reload and reconsider.",
    );
  }

  const row = correctionRow(draft, correctionTimestamp());
  await chInsertSeCompanyPersonCorrections([row]);
  return { correctionId: row.correction_id as string };
}
