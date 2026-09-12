/**
 * The LLM identity-matching band, the read row and the pure helpers the People tab
 * needs (spec 2026-09-11 sections 5 and 9). Client-safe: no `.server` import, so the
 * workspace renders a badge and a merge note without dragging ClickHouse into the
 * client bundle.
 *
 * The two constants are the pipeline's own. At or above `MATCH_THRESHOLD`
 * (`fold.py::MATCH_THRESHOLD`) the fold has ALREADY merged the pair into one person, so
 * the tab only reports what happened; between `POSSIBLE_MATCH_FLOOR` and the threshold
 * nothing was merged and the reviewer decides with a merge rule. `confidence` is
 * Float64 in ClickHouse and the comparison is inclusive at the floor, so a threshold
 * edit stays exact (spec section 8).
 *
 * `llm_match` is FOLD-OWNED: `fold.py::_with_llm_match` writes it onto the published
 * row's `data` after `merge_member_data`, no member ever carries it, and nothing here
 * writes it back. It is deliberately NOT in `RESERVED_DATA_KEYS` -- that list is the
 * three keys the reviewer's own sheet may not post, stripped from a draft or member
 * `data` by `ownData()`/`reviewerData()`, and neither of those ever sees a published
 * row's `data`.
 */
import { MAX_NOTE_LENGTH } from "~/lib/se-person-fields";

export const MATCH_THRESHOLD = 0.8;
export const POSSIBLE_MATCH_FLOOR = 0.5;
export const LLM_MATCH_KEY = "llm_match";

/** One scored pair as `PERSON_MATCH_SQL` delivers it. `members_a`/`members_b` are the
 * normalized ids each side stands for -- a candidate is a group of same-name rows
 * within one source -- which is what maps a pair onto a published person's
 * `normalized_ids`. */
export interface SePersonMatchRow {
  company_id: string;
  candidate_a: string;
  candidate_b: string;
  members_a: string[];
  members_b: string[];
  name_a: string;
  name_b: string;
  confidence: number;
  reason: string;
}

/** A pair at or above the threshold, resolved INSIDE one published person. */
export interface SePersonMatch {
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
  /** Both sides' normalized ids, so one member can tell whether it is named. */
  members: string[];
}

/** A pair inside the band whose two sides sit in two DIFFERENT active persons: the one
 * thing the reviewer can act on, and what the possible-matches card lists. */
export interface SePersonPossibleMatch {
  personKeyA: string;
  personKeyB: string;
  nameA: string;
  nameB: string;
  confidence: number;
  reason: string;
}

/** One entry of the fold's own record (`data.llm_match.pairs`). */
export interface SeLlmMatchPair {
  a: string;
  b: string;
  confidence: number;
  reason: string;
}
export interface SeLlmMatchBlock {
  pairs: SeLlmMatchPair[];
  model: string;
  promptVersion: string;
}

export function isMatch(confidence: number): boolean {
  return confidence >= MATCH_THRESHOLD;
}
export function isPossibleMatch(confidence: number): boolean {
  return confidence >= POSSIBLE_MATCH_FLOOR && confidence < MATCH_THRESHOLD;
}

/** The spec's own spelling of a score (`0.93`, `0.64`). */
export function formatConfidence(confidence: number): string {
  return confidence.toFixed(2);
}

/**
 * The note a Merge from the possible-matches card writes onto the merge rule, spec
 * section 5's `LLM match 0.64: <reason>`. The reason is model text, so it is flattened
 * to plain single-spaced text and the whole note is clamped to `MAX_NOTE_LENGTH`:
 * `parseSePersonDecision` runs every note through `noteError`, which refuses a control
 * character and anything past 500 characters -- a refusal here would read to the
 * reviewer as "the Merge button is broken".
 */
export function matchNote(confidence: number, reason: string): string {
  const plain = reason.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
  const head = `LLM match ${formatConfidence(confidence)}`;
  return (plain === "" ? head : `${head}: ${plain}`).slice(0, MAX_NOTE_LENGTH).trim();
}

/** A `data` string as an object; a hand-written row that is not one reads as empty
 * rather than throwing the whole tab. */
function parseObject(data: string): Record<string, unknown> {
  try {
    const parsed: unknown = JSON.parse(data === "" ? "{}" : data);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
}

/** The fold's record of what it merged, or null when this person was not joined by the
 * model. Every field is defended: the block is stored JSON, not a typed column. */
export function parseLlmMatch(data: string): SeLlmMatchBlock | null {
  const block = parseObject(data)[LLM_MATCH_KEY];
  if (block === null || typeof block !== "object" || Array.isArray(block)) return null;
  const record = block as Record<string, unknown>;
  const pairs: unknown[] = Array.isArray(record.pairs) ? record.pairs : [];
  return {
    pairs: pairs
      .filter(
        (pair): pair is Record<string, unknown> =>
          pair !== null && typeof pair === "object" && !Array.isArray(pair),
      )
      .map((pair) => ({
        a: typeof pair.a === "string" ? pair.a : "",
        b: typeof pair.b === "string" ? pair.b : "",
        confidence: typeof pair.confidence === "number" ? pair.confidence : 0,
        reason: typeof pair.reason === "string" ? pair.reason : "",
      })),
    model: typeof record.model === "string" ? record.model : "",
    promptVersion: typeof record.prompt_version === "string" ? record.prompt_version : "",
  };
}

/** `data` without the fold-owned key, for the panel's JSON block. A `data` with nothing
 * to strip comes back BYTE FOR BYTE, so the reviewer still reads what is stored rather
 * than a re-serialization of it. */
export function stripLlmMatch(data: string): string {
  const parsed = parseObject(data);
  if (!Object.hasOwn(parsed, LLM_MATCH_KEY)) return data;
  delete parsed[LLM_MATCH_KEY];
  return JSON.stringify(parsed);
}
