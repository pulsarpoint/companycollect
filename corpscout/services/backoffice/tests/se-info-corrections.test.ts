import { describe, expect, it } from "vitest";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";
import {
  liveOverrideCorrectionId,
  SE_INFO_CORRECTION_KINDS,
  SeInfoCorrectionValidationError,
  validateSeInfoCorrection,
} from "~/lib/se-info-corrections";

const HASH = "a".repeat(64);
const SUGGESTION = "11111111-1111-4111-8111-111111111111";
const OVERRIDE = "22222222-2222-4222-8222-222222222222";
const OTHER = "33333333-3333-4333-8333-333333333333";
const base = { companyId: "5565200028", evidenceHash: HASH, reason: "reviewed" };

describe("validateSeInfoCorrection", () => {
  it("lists four kinds", () => {
    expect(SE_INFO_CORRECTION_KINDS).toEqual([
      "override_field",
      "approve_suggestion",
      "reject_suggestion",
      "undo",
    ]);
  });

  it("override carries only description (string or null)", () => {
    expect(
      validateSeInfoCorrection({
        ...base,
        kind: "override_field",
        payload: { description: " New text " },
      }).payload,
    ).toBe(JSON.stringify({ description: "New text" }));
    expect(
      validateSeInfoCorrection({
        ...base,
        kind: "override_field",
        payload: { description: null },
      }).payload,
    ).toBe(JSON.stringify({ description: null }));
    expect(() =>
      validateSeInfoCorrection({ ...base, kind: "override_field", payload: { legal_name: "x" } }),
    ).toThrow("not allowed");
    expect(() =>
      validateSeInfoCorrection({ ...base, kind: "override_field", payload: {} }),
    ).toThrow("description");
  });

  it("approve/reject need a uuid suggestion id; reject may carry a note", () => {
    expect(() =>
      validateSeInfoCorrection({ ...base, kind: "approve_suggestion", payload: { suggestion_id: "x" } }),
    ).toThrow("suggestion_id");
    expect(
      JSON.parse(
        validateSeInfoCorrection({
          ...base,
          kind: "reject_suggestion",
          payload: { suggestion_id: SUGGESTION, note: "bad" },
        }).payload,
      ),
    ).toEqual({ suggestion_id: SUGGESTION, note: "bad" });
  });

  it("undo requires supersedes and the zero hash; others reject supersedes", () => {
    expect(() => validateSeInfoCorrection({ ...base, kind: "undo" })).toThrow("supersede");
    const row = validateSeInfoCorrection({
      ...base,
      kind: "undo",
      evidenceHash: ZERO_EVIDENCE_HASH,
      supersedesCorrectionId: SUGGESTION,
    });
    expect(row.supersedes_correction_id).toBe(SUGGESTION);
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        kind: "override_field",
        payload: { description: "x" },
        supersedesCorrectionId: SUGGESTION,
      }),
    ).toThrow(SeInfoCorrectionValidationError);
  });

  // P1 ruling: company ids are 10 OR 12 digits (sole traders are published);
  // the id-format error message matches "digit", not "10-digit".
  it("accepts a 12-digit sole-trader id", () => {
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        companyId: "196408123456",
        kind: "override_field",
        payload: { description: "x" },
      }),
    ).not.toThrow();
  });

  it("rejects bad company ids, hashes and reasons", () => {
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        companyId: "556520-0028",
        kind: "override_field",
        payload: { description: "x" },
      }),
    ).toThrow("digit");
    expect(() =>
      validateSeInfoCorrection({ ...base, evidenceHash: "zz", kind: "override_field", payload: { description: "x" } }),
    ).toThrow("evidence");
    expect(() =>
      validateSeInfoCorrection({ ...base, reason: " ", kind: "override_field", payload: { description: "x" } }),
    ).toThrow("Reason");
  });

  it("rejects the zero evidence hash on every kind except undo", () => {
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        evidenceHash: ZERO_EVIDENCE_HASH,
        kind: "override_field",
        payload: { description: "x" },
      }),
    ).toThrow(SeInfoCorrectionValidationError);
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        evidenceHash: ZERO_EVIDENCE_HASH,
        kind: "approve_suggestion",
        payload: { suggestion_id: SUGGESTION },
      }),
    ).toThrow(SeInfoCorrectionValidationError);
  });

  it("rejects a reason over 1000 characters", () => {
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        reason: "x".repeat(1001),
        kind: "override_field",
        payload: { description: "x" },
      }),
    ).toThrow("Reason");
  });

  it("rejects an unknown correction kind", () => {
    expect(() =>
      validateSeInfoCorrection({ ...base, kind: "bogus_kind", payload: {} }),
    ).toThrow("Unknown correction kind");
  });

  it("rejects a note on approve_suggestion (only reject may carry one)", () => {
    expect(() =>
      validateSeInfoCorrection({
        ...base,
        kind: "approve_suggestion",
        payload: { suggestion_id: SUGGESTION, note: "n" },
      }),
    ).toThrow("not allowed");
  });
});

// P7 ruling: an override_field outranks a later approve/reject in Dagster's
// INFO_KIND_ORDER, so Task 10 needs to know whether a live override exists
// before offering approve/reject. But Dagster's apply_info_ledger drops a
// STALE correction before it ever reaches kind-ranking, so a stale override
// must not count.
describe("liveOverrideCorrectionId", () => {
  const UNDO_A = "44444444-4444-4444-8444-444444444444";

  const overrideRow = (
    id: string,
    overrides: Partial<{ is_current: number; is_stale: number; created_at: string }> = {},
  ) => ({
    correction_id: id,
    correction_kind: "override_field",
    supersedes_correction_id: null,
    is_current: 1,
    is_stale: 0,
    created_at: "2026-08-20 10:00:00.000",
    ...overrides,
  });

  const undoRow = (
    id: string,
    supersedes: string,
    overrides: Partial<{ created_at: string }> = {},
  ) => ({
    correction_id: id,
    correction_kind: "undo",
    supersedes_correction_id: supersedes,
    is_current: 1,
    is_stale: 0,
    created_at: "2026-08-21 10:00:00.000",
    ...overrides,
  });

  it("returns the override's id when it stands alone", () => {
    expect(liveOverrideCorrectionId([overrideRow(OVERRIDE)])).toBe(OVERRIDE);
  });

  it("returns null for a stale override, even unsuperseded", () => {
    expect(liveOverrideCorrectionId([overrideRow(OVERRIDE, { is_stale: 1 })])).toBeNull();
  });

  it("returns null once a later undo supersedes a current override", () => {
    const corrections = [
      overrideRow(OVERRIDE),
      undoRow(UNDO_A, OVERRIDE, { created_at: "2026-08-22 10:00:00.000" }),
    ];
    expect(liveOverrideCorrectionId(corrections)).toBeNull();
  });

  it("still returns the override's id when the undo supersedes a different correction", () => {
    const corrections = [
      overrideRow(OVERRIDE),
      undoRow(UNDO_A, OTHER, { created_at: "2026-08-22 10:00:00.000" }),
    ];
    expect(liveOverrideCorrectionId(corrections)).toBe(OVERRIDE);
  });

  it("picks the newest of two current overrides by created_at, regardless of input order", () => {
    const older = overrideRow(OTHER, { created_at: "2026-08-19 10:00:00.000" });
    const newer = overrideRow(OVERRIDE, { created_at: "2026-08-23 10:00:00.000" });
    expect(liveOverrideCorrectionId([older, newer])).toBe(OVERRIDE);
    expect(liveOverrideCorrectionId([newer, older])).toBe(OVERRIDE);
  });
});
