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
    expect(() => validateSePersonCorrection({ ...base, companyId: "556520-0028", kind: "override_field", payload: { name: "A" } })).toThrow("10-digit");
    expect(() => validateSePersonCorrection({ ...base, evidenceHash: "xyz", kind: "override_field", payload: { name: "A" } })).toThrow("evidence");
    expect(() => validateSePersonCorrection({ ...base, kind: "delete_person" })).toThrow("Unknown correction");
    expect(() => validateSePersonCorrection({ ...base, reason: " ", kind: "override_field", payload: { name: "A" } })).toThrow("Reason");
  });

  it("rejects non-string name/role_code/description values", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "override_field", payload: { name: 123 } }),
    ).toThrow("name");
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "set_role", draftIds: [DRAFT], payload: { role_code: 123 } }),
    ).toThrow("role_code");
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "override_field", payload: { description: 123 } }),
    ).toThrow("description");
  });

  it("builds a successful merge_persons row", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "merge_persons", targetPersonId: TARGET, draftIds: [],
    });
    expect(row).toEqual({
      company_id: "5565200028",
      correction_kind: "merge_persons",
      subject_person_id: SUBJECT,
      target_person_id: TARGET,
      draft_ids: [],
      payload: "{}",
      evidence_hash: HASH,
      reason: "Reviewer note",
      supersedes_correction_id: null,
    });
  });

  it("rejects merge_persons with draft ids", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "merge_persons", targetPersonId: TARGET, draftIds: [DRAFT] }),
    ).toThrow("does not take draft");
  });

  it("builds a successful remove_role row", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "remove_role", draftIds: [DRAFT],
    });
    expect(row).toEqual({
      company_id: "5565200028",
      correction_kind: "remove_role",
      subject_person_id: SUBJECT,
      target_person_id: null,
      draft_ids: [DRAFT],
      payload: "{}",
      evidence_hash: HASH,
      reason: "Reviewer note",
      supersedes_correction_id: null,
    });
  });

  it("rejects remove_role with no drafts", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "remove_role", draftIds: [] }),
    ).toThrow("at least one draft");
  });

  it("scopes supersedes_correction_id to undo only", () => {
    expect(() =>
      validateSePersonCorrection({ ...base, kind: "override_field", payload: { name: "A" }, supersedesCorrectionId: SUBJECT }),
    ).toThrow("Only undo may supersede");
    const row = validateSePersonCorrection({
      ...base, kind: "undo", supersedesCorrectionId: SUBJECT,
    });
    expect(row.supersedes_correction_id).toEqual(SUBJECT.toLowerCase());
  });

  it("builds override_field with null description", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "override_field", payload: { description: null },
    });
    expect(JSON.parse(row.payload)).toEqual({ description: null });
  });

  it("builds override_field with description string", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "override_field", payload: { description: " Test description " },
    });
    expect(JSON.parse(row.payload)).toEqual({ description: "Test description" });
  });

  it("builds successful split_person row", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "split_person", draftIds: [DRAFT], payload: { name: " New Person " },
    });
    expect(JSON.parse(row.payload)).toEqual({ name: "New Person" });
  });

  it("builds successful approve_suggestion row", () => {
    const row = validateSePersonCorrection({
      ...base, kind: "approve_suggestion", payload: { suggestion_id: DRAFT },
    });
    expect(JSON.parse(row.payload)).toEqual({ suggestion_id: DRAFT.toLowerCase() });
  });

  it("builds successful reject_suggestion row with and without note", () => {
    const rowWithoutNote = validateSePersonCorrection({
      ...base, kind: "reject_suggestion", payload: { suggestion_id: DRAFT },
    });
    expect(JSON.parse(rowWithoutNote.payload)).toEqual({ suggestion_id: DRAFT.toLowerCase() });

    const rowWithNote = validateSePersonCorrection({
      ...base, kind: "reject_suggestion", payload: { suggestion_id: DRAFT, note: " Duplicate " },
    });
    expect(JSON.parse(rowWithNote.payload)).toEqual({ suggestion_id: DRAFT.toLowerCase(), note: "Duplicate" });
  });
});
