/**
 * The shape of one merge/keep-separate suggestion payload, as dagster_v3's
 * `company_people/merge.py` writes it to
 * `se_company_person_enrichment_observation.suggestion` (SE People Experiment
 * Task 4). Client-safe on purpose: both the review page's component (to tell a
 * merge suggestion apart from an ordinary profile suggestion, whose JSON has a
 * `name`/`description` shape instead) and `se-company-person.server.ts` (to
 * re-validate a suggestion against live state before writing a correction)
 * need it.
 *
 * `merge.py`'s `suggestion_payload` dict (module docstring / the asset's own
 * insert code):
 * `{candidate_group_id, decision, confidence, rationale, into_person_id,
 * from_person_ids, member_person_ids}` -- every field here mirrors one of
 * those keys exactly; nothing is renamed.
 */

export type SeMergeSuggestionDecision = "merge" | "keep_separate";

export interface SeMergeSuggestionPayload {
  candidate_group_id: string;
  decision: SeMergeSuggestionDecision;
  confidence: number;
  rationale: string;
  into_person_id: string;
  from_person_ids: string[];
  member_person_ids: string[];
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

/**
 * Parses one `suggestion` JSON blob as a merge suggestion, or returns null
 * when it is not one (an ordinary person-profile suggestion, or malformed
 * JSON). Never throws: a suggestion row this reader did not write (the
 * profile-resolution path shares the same table) is a normal, expected input,
 * not an error.
 */
export function parseMergeSuggestionPayload(raw: string): SeMergeSuggestionPayload | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (value === null || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  if (record.decision !== "merge" && record.decision !== "keep_separate") return null;
  if (typeof record.candidate_group_id !== "string" || record.candidate_group_id.trim() === "") {
    return null;
  }
  if (typeof record.into_person_id !== "string" || record.into_person_id.trim() === "") {
    return null;
  }
  if (!isStringArray(record.from_person_ids) || !isStringArray(record.member_person_ids)) {
    return null;
  }
  return {
    candidate_group_id: record.candidate_group_id,
    decision: record.decision,
    confidence: typeof record.confidence === "number" ? record.confidence : 0,
    rationale: typeof record.rationale === "string" ? record.rationale : "",
    into_person_id: record.into_person_id,
    from_person_ids: record.from_person_ids,
    member_person_ids: record.member_person_ids,
  };
}
