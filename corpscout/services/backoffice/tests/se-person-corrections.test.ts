import { describe, expect, it } from "vitest";
import {
  SE_PERSON_CORRECTION_KINDS,
  SePersonCorrectionValidationError,
  validateSePersonCorrection,
} from "~/lib/se-person-corrections";

const SUBJECT = "43234b7d-0184-16b5-de47-dc086a2b0ed9";
const TARGET = "6942ffc1-e104-ebea-7aa0-ef7377e8a508";
const DRAFT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const HASH = "a".repeat(64);
const base = {
  companyId: "5565200028",
  subjectPersonId: SUBJECT,
  evidenceHash: HASH,
  reason: "Reviewer note",
  activeRoleCodes: new Set(["board_member", "board_chair"]),
};

describe("validateSePersonCorrection", () => {
  it("lists the nine kinds", () => {
    expect(SE_PERSON_CORRECTION_KINDS).toEqual([
      "merge_persons", "reassign_draft", "split_person", "approve_suggestion",
      "reject_suggestion", "override_field", "set_role", "remove_role", "undo",
    ]);
  });

  it("builds an override row with a JSON payload", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "override_field", payload: { name: " Anna K. Svensson " },
    });
    expect(row).toEqual({
      company_id: "5565200028",
      correction_kind: "override_field",
      subject_person_id: SUBJECT,
      target_person_id: null,
      draft_ids: [],
      payload: JSON.stringify({ name: "Anna K. Svensson" }),
      evidence_hash: HASH,
      reason: "Reviewer note",
      supersedes_correction_id: null,
    });
  });

  it("requires exactly one draft and a distinct target for reassign_draft", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: SUBJECT, draftIds: [DRAFT] }),
    ).toThrow(SePersonCorrectionValidationError);
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: TARGET, draftIds: [DRAFT, DRAFT] }),
    ).toThrow("exactly one");
    expect(
      validateSePersonCorrection({ ...base, kind: "reassign_draft", targetPersonId: TARGET, draftIds: [DRAFT] }).draft_ids,
    ).toEqual([DRAFT]);
  });

  it("requires a non-empty name for split_person", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "split_person", draftIds: [DRAFT], payload: { name: " " } }),
    ).toThrow("name");
  });

  it("accepts only active role codes for set_role and rejects unknown payload keys", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "set_role", draftIds: [DRAFT], payload: { role_code: "ceo" } }),
    ).toThrow("active canonical role");
    const row = validateSePersonCorrection({
      ...base, kind: "set_role", draftIds: [DRAFT], payload: { role_code: "board_chair", fiscal_year: 2023 },
    });
    expect(JSON.parse(row.payload)).toEqual({ role_code: "board_chair", fiscal_year: 2023 });
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "override_field", payload: { nickname: "x" } }),
    ).toThrow("not allowed");
  });

  it("requires supersedes for undo and a uuid suggestion id for approvals", () => {
    expect(() => validateSePersonCorrection({ ...base, kind: "undo" })).toThrow("supersedes");
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "approve_suggestion", payload: { suggestion_id: "nope" } }),
    ).toThrow("suggestion_id");
  });

  it("rejects bad company ids, hashes, kinds and empty reasons", () => {
    expect(() => validateSePersonCorrection({ ...base, companyId: "123", kind: "override_field", payload: { name: "A" } })).toThrow("10-digit");
    expect(() => validateSePersonCorrection({ ...base, evidenceHash: "xyz", kind: "override_field", payload: { name: "A" } })).toThrow("evidence");
    expect(() => validateSePersonCorrection({ ...base, kind: "delete_person" })).toThrow("Unknown correction");
    expect(() => validateSePersonCorrection({ ...base, reason: " ", kind: "override_field", payload: { name: "A" } })).toThrow("Reason");
  });
});
