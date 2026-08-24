/**
 * Client-safe validator for the Sweden company-address correction ledger
 * (se_company_address_correction). Mirrors se-info-corrections.ts, with two
 * differences that come from the datatype rather than from taste:
 *
 * - every payload names the `address_key` it decides, because a company has
 *   several published addresses and a correction without one has no subject;
 * - there is no approve/reject of a model suggestion, because nothing in this
 *   datatype is model-written. `reject_address` is not that: it says "this is
 *   not an address of this company", and Dagster publishes the row
 *   is_current = false.
 *
 * Reuses ZERO_EVIDENCE_HASH from se-person-corrections since all three ledgers
 * share the "undo carries no evidence" convention.
 */
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export { ZERO_EVIDENCE_HASH };

export const SE_ADDRESS_CORRECTION_KINDS = [
  "override_field",
  "reject_address",
  "undo",
] as const;

export type SeAddressCorrectionKind = (typeof SE_ADDRESS_CORRECTION_KINDS)[number];

/**
 * A ledger row's status relative to the published rows, as the
 * `/admin/se/company-address/corrections` list computes it in SQL. Declared
 * here (client-safe) so the list's filter can import the value set instead of
 * keeping a second copy.
 */
export const SE_ADDRESS_CORRECTION_STATUSES = ["pending", "applied", "stale", "undone"] as const;

export type SeAddressCorrectionStatus = (typeof SE_ADDRESS_CORRECTION_STATUSES)[number];

/**
 * One ledger row's status, from the three flags the server module computes in
 * SQL. Client-safe so the Address tab and the ledger list name a row the same
 * way.
 *
 * Superseded wins over everything: an undone correction is history whatever it
 * once did. Applied is checked before stale because the SQL already excludes an
 * applied row from staleness (ruling A11: a reject whose address is gone is
 * applied, never stale) -- this keeps the two in step if that ever loosens.
 */
export function correctionStatus(row: {
  is_current: number;
  is_applied: number;
  is_stale: number;
}): SeAddressCorrectionStatus {
  if (row.is_current !== 1) return "undone";
  if (row.is_applied === 1) return "applied";
  if (row.is_stale === 1) return "stale";
  return "pending";
}

export class SeAddressCorrectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeAddressCorrectionValidationError";
  }
}

export interface SeAddressCorrectionInput {
  companyId: string;
  kind: string;
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
}

export interface SeAddressCorrectionDraft {
  company_id: string;
  correction_kind: SeAddressCorrectionKind;
  payload: string;
  evidence_hash: string;
  reason: string;
  supersedes_correction_id: string | null;
}

// Legal entities carry a 10-digit organisationsnummer; sole traders carry a
// 12-digit personnummer-based id. Mirrors has_company in migration 000307.
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH_PATTERN = /^[0-9a-f]{64}$/;

// The text fields a reviewer may decide. address_type is NOT among them: it is
// part of address_key, so overriding it would move the row to a different
// identity -- Dagster's OVERRIDABLE_FIELDS says exactly the same thing, and the
// two lists must stay in step.
export const OVERRIDABLE_FIELDS = [
  "care_of",
  "street_address",
  "normalized_address",
  "postal_code",
  "city",
  "country_code",
] as const;

export type SeAddressOverridableField = (typeof OVERRIDABLE_FIELDS)[number];

const ALLOWED_PAYLOAD_KEYS: Record<SeAddressCorrectionKind, readonly string[]> = {
  override_field: ["address_key", ...OVERRIDABLE_FIELDS],
  reject_address: ["address_key"],
  undo: [],
};

function fail(message: string): never {
  throw new SeAddressCorrectionValidationError(message);
}

function isKind(value: string): value is SeAddressCorrectionKind {
  return (SE_ADDRESS_CORRECTION_KINDS as readonly string[]).includes(value);
}

function addressKeyOrFail(payload: Record<string, unknown>): string {
  const value =
    typeof payload.address_key === "string" ? payload.address_key.trim().toLowerCase() : "";
  if (!HASH_PATTERN.test(value)) fail("The address key is missing or malformed.");
  return value;
}

export function validateSeAddressCorrection(
  input: SeAddressCorrectionInput,
): SeAddressCorrectionDraft {
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
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      cleanPayload.address_key = addressKeyOrFail(payload);
      let named = 0;
      for (const field of OVERRIDABLE_FIELDS) {
        if (!(field in payload)) continue; // absent means "leave it as computed"
        const value = payload[field];
        if (value !== null && typeof value !== "string") {
          fail(`Override ${field} must be a string or null.`);
        }
        const trimmed = typeof value === "string" ? value.trim() : null;
        // "" is not a decision: clearing a field is an explicit null.
        cleanPayload[field] = trimmed === "" ? null : trimmed;
        named += 1;
      }
      if (named === 0) fail("Override needs at least one address field.");
      break;
    }
    case "reject_address": {
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      cleanPayload.address_key = addressKeyOrFail(payload);
      break;
    }
    case "undo": {
      if (!input.supersedesCorrectionId) fail("Undo needs the correction it supersedes.");
      const superseded = input.supersedesCorrectionId.trim().toLowerCase();
      if (!UUID_PATTERN.test(superseded)) fail("Superseded correction must be a UUID.");
      supersedesCorrectionId = superseded;
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
 * Dagster's `apply_address_ledger` drops a stale correction before ranking, and
 * among what is left ADDRESS_KIND_ORDER ranks `override_field` after
 * `reject_address` -- so a live override's field values always survive, whatever
 * order the reviewer decided in. That is fine for the two kinds to coexist (they
 * decide different things), but a SECOND override of the same row is not: the
 * later one wins by created_at and the earlier is invisible. The page uses this
 * to show "this address is already overridden" and offer undo instead.
 *
 * A "live" override is one no later `undo` supersedes. Sorts the array itself by
 * `created_at DESC, correction_id DESC`, so arrival order does not matter.
 */
export function liveOverrideCorrectionId(
  corrections: ReadonlyArray<{
    correction_id: string;
    correction_kind: string;
    address_key: string;
    supersedes_correction_id: string | null;
    is_current: number;
    is_stale: number;
    created_at: string;
  }>,
  addressKey: string,
): string | null {
  const supersededIds = new Set(
    corrections
      .filter((row) => row.correction_kind === "undo" && row.supersedes_correction_id)
      .map((row) => row.supersedes_correction_id as string),
  );
  const live = corrections.filter(
    (row) =>
      row.correction_kind === "override_field" &&
      row.address_key === addressKey &&
      row.is_current === 1 &&
      row.is_stale === 0 &&
      !supersededIds.has(row.correction_id),
  );
  if (live.length === 0) return null;
  live.sort((a, b) => {
    if (a.created_at !== b.created_at) return a.created_at > b.created_at ? -1 : 1;
    return a.correction_id > b.correction_id ? -1 : 1;
  });
  return live[0].correction_id;
}
