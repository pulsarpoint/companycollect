/**
 * Client-safe validator for the Sweden company-info correction ledger
 * (se_company_info_correction). Mirrors se-person-corrections.ts's shape
 * with a smaller kind set: the subject is the company's published row
 * itself, so there is no subject/target/draft bookkeeping here. Reuses
 * ZERO_EVIDENCE_HASH from se-person-corrections since both ledgers share
 * the same "undo carries no evidence" convention.
 */
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export { ZERO_EVIDENCE_HASH };

export const SE_INFO_CORRECTION_KINDS = [
  "override_field",
  "approve_suggestion",
  "reject_suggestion",
  "undo",
] as const;

export type SeInfoCorrectionKind = (typeof SE_INFO_CORRECTION_KINDS)[number];

export class SeInfoCorrectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeInfoCorrectionValidationError";
  }
}

export interface SeInfoCorrectionInput {
  companyId: string;
  kind: string;
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
}

export interface SeInfoCorrectionDraft {
  company_id: string;
  correction_kind: SeInfoCorrectionKind;
  payload: string;
  evidence_hash: string;
  reason: string;
  supersedes_correction_id: string | null;
}

// Legal entities carry a 10-digit organisationsnummer; sole traders (enskild
// firma) carry a 12-digit personnummer-based id. Mirrors has_company in
// migration 000299 -- both are published to se_company_info.
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH_PATTERN = /^[0-9a-f]{64}$/;

const ALLOWED_PAYLOAD_KEYS: Record<SeInfoCorrectionKind, readonly string[]> = {
  override_field: ["description"],
  approve_suggestion: ["suggestion_id"],
  reject_suggestion: ["suggestion_id", "note"],
  undo: [],
};

function fail(message: string): never {
  throw new SeInfoCorrectionValidationError(message);
}

function uuidOrFail(value: unknown, label: string): string {
  const clean = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (!UUID_PATTERN.test(clean)) fail(`${label} must be a UUID.`);
  return clean;
}

function isKind(value: string): value is SeInfoCorrectionKind {
  return (SE_INFO_CORRECTION_KINDS as readonly string[]).includes(value);
}

export function validateSeInfoCorrection(
  input: SeInfoCorrectionInput,
): SeInfoCorrectionDraft {
  const companyId = input.companyId.replace(/[^0-9]/g, "");
  if (!COMPANY_ID_PATTERN.test(companyId) || companyId !== input.companyId.trim()) {
    fail("Company must be a 10-digit or 12-digit Swedish company id.");
  }
  if (!isKind(input.kind)) fail("Unknown correction kind.");
  const kind = input.kind;

  const evidenceHash = input.evidenceHash.trim().toLowerCase();
  if (!HASH_PATTERN.test(evidenceHash)) fail("The evidence hash is missing or malformed.");

  const reason = input.reason.trim();
  if (reason === "" || reason.length > 1000) fail("Reason is required (max 1000 characters).");

  // Scope supersedes_correction_id to undo only.
  if (kind !== "undo" && input.supersedesCorrectionId) {
    fail("Only undo may supersede a correction.");
  }

  const payload = input.payload ?? {};
  for (const key of Object.keys(payload)) {
    if (!ALLOWED_PAYLOAD_KEYS[kind].includes(key)) {
      fail(`Payload key "${key}" is not allowed for ${kind}.`);
    }
  }

  const cleanPayload: Record<string, unknown> = {};
  let supersedesCorrectionId: string | null = null;

  switch (kind) {
    case "override_field": {
      // legal_name is SCB's; only description may be reviewer-overridden.
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      if (!("description" in payload)) fail("Override needs a description.");
      const description = payload.description;
      if (description !== null && typeof description !== "string") {
        fail("Override description must be a string or null.");
      }
      const trimmed = typeof description === "string" ? description.trim() : null;
      if (trimmed === "") fail("Override description cannot be empty.");
      cleanPayload.description = trimmed;
      break;
    }
    case "approve_suggestion":
    case "reject_suggestion": {
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      cleanPayload.suggestion_id = uuidOrFail(payload.suggestion_id, "suggestion_id");
      if (kind === "reject_suggestion" && typeof payload.note === "string" && payload.note.trim()) {
        cleanPayload.note = payload.note.trim();
      }
      break;
    }
    case "undo": {
      if (!input.supersedesCorrectionId) fail("Undo needs the correction it supersedes.");
      supersedesCorrectionId = uuidOrFail(input.supersedesCorrectionId, "Superseded correction");
      if (evidenceHash !== ZERO_EVIDENCE_HASH) fail("Undo must carry the zero evidence hash.");
      break;
    }
  }

  return {
    company_id: companyId,
    correction_kind: kind,
    payload: JSON.stringify(cleanPayload),
    evidence_hash: evidenceHash,
    reason,
    supersedes_correction_id: supersedesCorrectionId,
  };
}

/**
 * Dagster's INFO_KIND_ORDER (info_rules.py) ranks override_field after
 * approve/reject regardless of created_at, so a live override always wins
 * over any approve/reject decided before or after it -- a reviewer who
 * approves a suggestion after overriding the description sees nothing
 * happen. This mirrors that rule at the review layer: while a live override
 * exists, approve/reject is a no-op server-side, so the UI should refuse to
 * offer it and point at the override instead.
 *
 * A "live" override is one no later `undo` row supersedes (an undo row's
 * `supersedes_correction_id` names the correction it cancels). `corrections`
 * is expected newest-first, matching CORRECTIONS_SQL's own
 * `ORDER BY created_at DESC` -- this row shape carries no created_at of its
 * own, so "newest" means the first live match in that given order, not a
 * recomputed sort.
 */
export function liveOverrideCorrectionId(
  corrections: ReadonlyArray<{
    correction_id: string;
    correction_kind: string;
    supersedes_correction_id: string | null;
  }>,
): string | null {
  const supersededIds = new Set(
    corrections
      .filter((row) => row.correction_kind === "undo" && row.supersedes_correction_id)
      .map((row) => row.supersedes_correction_id as string),
  );
  const live = corrections.find(
    (row) => row.correction_kind === "override_field" && !supersededIds.has(row.correction_id),
  );
  return live ? live.correction_id : null;
}
