import { describe, expect, it } from "vitest";
import {
  correctionStatus,
  liveOverrideCorrectionId,
  SeAddressCorrectionValidationError,
  validateSeAddressCorrection,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";

const KEY = "a".repeat(64);
const OTHER_KEY = "c".repeat(64);
const HASH = "b".repeat(64);
const base = { companyId: "5565200028", evidenceHash: HASH, reason: "Fixed the care-of line." };

describe("validateSeAddressCorrection", () => {
  it("accepts a partial override and keeps absent fields absent", () => {
    const draft = validateSeAddressCorrection({
      ...base, kind: "override_field", payload: { address_key: KEY, care_of: "c/o Anna" },
    });
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY, care_of: "c/o Anna" });
  });

  it("passes an explicit null through as a decision to clear the field", () => {
    const draft = validateSeAddressCorrection({
      ...base, kind: "override_field", payload: { address_key: KEY, care_of: null },
    });
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY, care_of: null });
  });

  it("refuses an override that names no field, an unknown field, or the key itself", () => {
    for (const payload of [{ address_key: KEY }, { address_key: KEY, legal_name: "x" },
                           { address_key: KEY, address_type: "postal" }, { care_of: "x" }]) {
      expect(() => validateSeAddressCorrection({ ...base, kind: "override_field", payload }))
        .toThrow(SeAddressCorrectionValidationError);
    }
  });

  it("accepts a reject that names only the address key", () => {
    const draft = validateSeAddressCorrection({ ...base, kind: "reject_address", payload: { address_key: KEY } });
    expect(draft.correction_kind).toBe("reject_address");
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY });
  });

  it("requires the zero hash and a superseded id for undo, and forbids both elsewhere", () => {
    const undo = validateSeAddressCorrection({
      ...base, kind: "undo", evidenceHash: ZERO_EVIDENCE_HASH,
      supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    });
    expect(undo.supersedes_correction_id).not.toBeNull();
    expect(() => validateSeAddressCorrection({ ...base, kind: "undo", evidenceHash: HASH,
      supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" })).toThrow();
    expect(() => validateSeAddressCorrection({ ...base, kind: "reject_address",
      payload: { address_key: KEY }, supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" })).toThrow();
  });

  it("accepts both Swedish company id widths and rejects anything else", () => {
    expect(validateSeAddressCorrection({ ...base, companyId: "196408233412", kind: "reject_address",
      payload: { address_key: KEY } }).company_id).toBe("196408233412");
    expect(() => validateSeAddressCorrection({ ...base, companyId: "55652000",
      kind: "reject_address", payload: { address_key: KEY } })).toThrow();
  });

  it("requires a 64-hex address key on every non-undo kind", () => {
    expect(() => validateSeAddressCorrection({ ...base, kind: "reject_address", payload: { address_key: "nope" } }))
      .toThrow(/address key/i);
  });

  /**
   * The zero hash is the undo marker. Dagster treats it as "compare nothing"
   * (address_rules.py: `correction.evidence_hash not in (ZERO_HASH, ...)`), so
   * accepting it on an override or a reject would hand a reviewer a decision
   * that can never go stale -- the same rule se-info-corrections.ts enforces.
   */
  it("forbids the zero hash on the kinds that decide a published row", () => {
    for (const kind of ["override_field", "reject_address"]) {
      expect(() => validateSeAddressCorrection({
        ...base, kind, evidenceHash: ZERO_EVIDENCE_HASH,
        payload: { address_key: KEY, care_of: "c/o Anna" },
      })).toThrow(SeAddressCorrectionValidationError);
    }
  });

  it("requires a reason on every kind and caps it", () => {
    expect(() => validateSeAddressCorrection({
      ...base, reason: "   ", kind: "reject_address", payload: { address_key: KEY },
    })).toThrow(/reason/i);
    expect(() => validateSeAddressCorrection({
      ...base, reason: "x".repeat(1001), kind: "reject_address", payload: { address_key: KEY },
    })).toThrow(/reason/i);
  });

  it("normalises the key and the hash to lower case hex", () => {
    const draft = validateSeAddressCorrection({
      ...base, evidenceHash: HASH.toUpperCase(), kind: "reject_address",
      payload: { address_key: KEY.toUpperCase() },
    });
    expect(draft.evidence_hash).toBe(HASH);
    expect(JSON.parse(draft.payload).address_key).toBe(KEY);
  });

  it("treats an empty override value as a clear, not as empty text", () => {
    const draft = validateSeAddressCorrection({
      ...base, kind: "override_field", payload: { address_key: KEY, care_of: "   " },
    });
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY, care_of: null });
  });
});

describe("liveOverrideCorrectionId", () => {
  const row = (over: Partial<{
    correction_id: string;
    correction_kind: string;
    address_key: string;
    supersedes_correction_id: string | null;
    is_current: number;
    is_stale: number;
    created_at: string;
  }>) => ({
    correction_id: "1",
    correction_kind: "override_field",
    address_key: KEY,
    supersedes_correction_id: null,
    is_current: 1,
    is_stale: 0,
    created_at: "2026-08-24 09:00:00.000",
    ...over,
  });

  it("is per address key: an override of another row does not block this one", () => {
    expect(liveOverrideCorrectionId([row({ address_key: OTHER_KEY })], KEY)).toBeNull();
    expect(liveOverrideCorrectionId([row({ address_key: OTHER_KEY })], OTHER_KEY)).toBe("1");
  });

  it("ignores superseded, stale and non-override rows", () => {
    expect(liveOverrideCorrectionId([
      row({ correction_id: "1" }),
      row({ correction_id: "u", correction_kind: "undo", supersedes_correction_id: "1" }),
    ], KEY)).toBeNull();
    expect(liveOverrideCorrectionId([row({ is_stale: 1 })], KEY)).toBeNull();
    expect(liveOverrideCorrectionId([row({ is_current: 0 })], KEY)).toBeNull();
    expect(liveOverrideCorrectionId([row({ correction_kind: "reject_address" })], KEY)).toBeNull();
  });

  it("picks the newest live override whatever order the rows arrive in", () => {
    const older = row({ correction_id: "a", created_at: "2026-08-20 09:00:00.000" });
    const newer = row({ correction_id: "b", created_at: "2026-08-23 09:00:00.000" });
    expect(liveOverrideCorrectionId([older, newer], KEY)).toBe("b");
    expect(liveOverrideCorrectionId([newer, older], KEY)).toBe("b");
  });
});

describe("correctionStatus", () => {
  it("names a row from the three flags, superseded first", () => {
    expect(correctionStatus({ is_current: 0, is_applied: 1, is_stale: 0 })).toBe("undone");
    expect(correctionStatus({ is_current: 1, is_applied: 1, is_stale: 0 })).toBe("applied");
    expect(correctionStatus({ is_current: 1, is_applied: 0, is_stale: 1 })).toBe("stale");
    expect(correctionStatus({ is_current: 1, is_applied: 0, is_stale: 0 })).toBe("pending");
  });

  /**
   * Ruling A11: a reject whose address key is not in the company's live set is
   * applied -- Dagster had no row to stamp its id on. The server module says so
   * in SQL; this is the guarantee at the display layer that "applied" is never
   * overruled by a stale flag arriving with it.
   */
  it("never calls an applied correction stale", () => {
    expect(correctionStatus({ is_current: 1, is_applied: 1, is_stale: 1 })).toBe("applied");
  });
});
