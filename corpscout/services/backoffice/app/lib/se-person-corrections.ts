/**
 * The evidence hash an `undo` carries: it supersedes a decision, not evidence.
 * Client-safe on purpose -- the review form needs it while building a row.
 */
export const ZERO_EVIDENCE_HASH = "0".repeat(64);

export const SE_PERSON_CORRECTION_KINDS = [
  "merge_persons",
  "reassign_draft",
  "split_person",
  "approve_suggestion",
  "reject_suggestion",
  "override_field",
  "set_role",
  "remove_role",
  "undo",
] as const;

export type SePersonCorrectionKind = (typeof SE_PERSON_CORRECTION_KINDS)[number];

export class SePersonCorrectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SePersonCorrectionValidationError";
  }
}

export interface SePersonCorrectionInput {
  companyId: string;
  kind: string;
  subjectPersonId: string;
  targetPersonId?: string | null;
  draftIds?: string[];
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
  activeRoleCodes: ReadonlySet<string>;
}

export interface SePersonCorrectionDraft {
  company_id: string;
  correction_kind: SePersonCorrectionKind;
  subject_person_id: string;
  target_person_id: string | null;
  draft_ids: string[];
  payload: string;
  evidence_hash: string;
  reason: string;
  supersedes_correction_id: string | null;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const ALLOWED_PAYLOAD_KEYS: Record<SePersonCorrectionKind, readonly string[]> = {
  merge_persons: [],
  reassign_draft: [],
  split_person: ["name"],
  approve_suggestion: ["suggestion_id"],
  reject_suggestion: ["suggestion_id", "note"],
  override_field: ["name", "description"],
  set_role: ["role_code", "fiscal_year"],
  remove_role: [],
  undo: [],
};

function fail(message: string): never {
  throw new SePersonCorrectionValidationError(message);
}

function uuidOrFail(value: string | null | undefined, label: string): string {
  const clean = (value ?? "").trim().toLowerCase();
  if (!UUID_PATTERN.test(clean)) fail(`${label} must be a UUID.`);
  return clean;
}

function isKind(value: string): value is SePersonCorrectionKind {
  return (SE_PERSON_CORRECTION_KINDS as readonly string[]).includes(value);
}

export function validateSePersonCorrection(
  input: SePersonCorrectionInput,
): SePersonCorrectionDraft {
  const companyId = input.companyId.replace(/[^0-9]/g, "");
  if (companyId.length !== 10 || companyId !== input.companyId.trim()) {
    fail("Company must be a 10-digit Swedish organization number.");
  }
  if (!isKind(input.kind)) fail("Unknown correction kind.");
  const kind = input.kind;
  const subject = uuidOrFail(input.subjectPersonId, "Subject person");
  const evidenceHash = input.evidenceHash.trim().toLowerCase();
  if (!HASH_PATTERN.test(evidenceHash)) fail("The evidence hash is missing or malformed.");
  const reason = input.reason.trim();
  if (reason === "" || reason.length > 1000) fail("Reason is required (max 1000 characters).");

  // Scope supersedes_correction_id to undo only
  if (kind !== "undo" && input.supersedesCorrectionId) {
    fail("Only undo may supersede a correction.");
  }

  const payload = input.payload ?? {};
  for (const key of Object.keys(payload)) {
    if (!ALLOWED_PAYLOAD_KEYS[kind].includes(key)) {
      fail(`Payload key "${key}" is not allowed for ${kind}.`);
    }
  }
  const draftIds = [...new Set((input.draftIds ?? []).map((id) => uuidOrFail(id, "Draft")))];
  let target: string | null = null;
  const cleanPayload: Record<string, unknown> = {};

  switch (kind) {
    case "merge_persons":
    case "reassign_draft": {
      target = uuidOrFail(input.targetPersonId, "Target person");
      if (target === subject) fail("Target person must differ from the subject.");
      if (kind === "reassign_draft" && (input.draftIds ?? []).length !== 1) {
        fail("Reassign moves exactly one draft.");
      }
      if (kind === "merge_persons" && draftIds.length !== 0) {
        fail("Merge does not take draft ids.");
      }
      break;
    }
    case "split_person": {
      if (draftIds.length === 0) fail("Select at least one draft to split out.");
      if (typeof payload.name !== "string") fail("Split name must be a string.");
      const name = payload.name.trim();
      if (name === "") fail("Split needs the new person's name.");
      cleanPayload.name = name;
      break;
    }
    case "approve_suggestion":
    case "reject_suggestion": {
      cleanPayload.suggestion_id = uuidOrFail(
        typeof payload.suggestion_id === "string" ? payload.suggestion_id : null,
        "suggestion_id",
      );
      if (kind === "reject_suggestion" && typeof payload.note === "string" && payload.note.trim()) {
        cleanPayload.note = payload.note.trim();
      }
      break;
    }
    case "override_field": {
      if ("name" in payload) {
        if (typeof payload.name !== "string") fail("Override name must be a string.");
        const name = payload.name.trim();
        if (name === "") fail("Override name cannot be empty.");
        cleanPayload.name = name;
      }
      if ("description" in payload) {
        const description = payload.description;
        if (description !== null && typeof description !== "string") fail("Override description must be a string or null.");
        cleanPayload.description =
          description === null || (typeof description === "string" && description.trim() === "")
            ? null
            : (description as string).trim();
      }
      if (Object.keys(cleanPayload).length === 0) fail("Override needs a name or description.");
      break;
    }
    case "set_role":
    case "remove_role": {
      if (draftIds.length === 0) fail("Select at least one draft for the role change.");
      if (kind === "set_role") {
        if (typeof payload.role_code !== "string") fail("Set role_code must be a string.");
        const roleCode = payload.role_code.trim();
        if (!input.activeRoleCodes.has(roleCode)) fail("Select an active canonical role.");
        cleanPayload.role_code = roleCode;
        if ("fiscal_year" in payload && payload.fiscal_year !== null) {
          const year = Number(payload.fiscal_year);
          if (!Number.isInteger(year) || year < 1800 || year > 9999) fail("fiscal_year must be a year.");
          cleanPayload.fiscal_year = year;
        }
      }
      break;
    }
    case "undo": {
      if (!input.supersedesCorrectionId) fail("Undo needs the correction it supersedes.");
      break;
    }
  }

  let supersedesCorrectionId: string | null = null;
  if (kind === "undo") {
    supersedesCorrectionId = uuidOrFail(input.supersedesCorrectionId, "Superseded correction");
  }

  return {
    company_id: companyId,
    correction_kind: kind,
    subject_person_id: subject,
    target_person_id: target,
    draft_ids: draftIds,
    payload: JSON.stringify(cleanPayload),
    evidence_hash: evidenceHash,
    reason,
    supersedes_correction_id: supersedesCorrectionId,
  };
}
