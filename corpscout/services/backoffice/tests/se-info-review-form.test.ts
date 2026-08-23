import { describe, expect, it } from "vitest";
import {
  buildCorrectionInput,
  liveOverrideRefusal,
  payloadFor,
} from "~/lib/se-info-review-form";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

const form = (entries: Record<string, string>) => {
  const f = new FormData();
  for (const [k, v] of Object.entries(entries)) f.append(k, v);
  return f;
};
const params = { companyId: "5565200028" };
const CORRECTION_ID = "33333333-3333-4333-8333-333333333333";

describe("company info review form", () => {
  it("sends description only when changed; clear checkbox → null; nothing changed refused", () => {
    expect(
      payloadFor(
        form({ description: "New", original_description: "Old" }),
        "override_field",
      ),
    ).toEqual({ description: "New" });
    expect(
      payloadFor(
        form({ description: "Old ", original_description: "Old" }),
        "override_field",
      ),
    ).toEqual({});
    expect(
      payloadFor(
        form({
          description: "x",
          original_description: "x",
          clear_description: "yes",
        }),
        "override_field",
      ),
    ).toEqual({ description: null });
    const built = buildCorrectionInput(
      form({
        correction_kind: "override_field",
        description: "Old",
        original_description: "Old",
        evidence_hash: "a".repeat(64),
        reason: "r",
      }),
      params,
    );
    expect(built).toEqual({ ok: false, error: "Nothing changed." });
  });

  it("approve/reject carry suggestion_id; reject may carry a note; undo uses the zero hash", () => {
    expect(
      payloadFor(
        form({
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          note: "n",
        }),
        "reject_suggestion",
      ),
    ).toEqual({ suggestion_id: "11111111-1111-4111-8111-111111111111", note: "n" });
    expect(
      payloadFor(
        form({
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          note: "n",
        }),
        "approve_suggestion",
      ),
    ).toEqual({ suggestion_id: "11111111-1111-4111-8111-111111111111" });
    const undo = buildCorrectionInput(
      form({
        correction_kind: "undo",
        supersedes_correction_id: "11111111-1111-4111-8111-111111111111",
        reason: "r",
      }),
      params,
    );
    expect(undo).toMatchObject({
      ok: true,
      input: {
        kind: "undo",
        evidenceHash: ZERO_EVIDENCE_HASH,
        supersedesCorrectionId: "11111111-1111-4111-8111-111111111111",
      },
    });
  });
});

describe("live override refusal (P7)", () => {
  const liveOverrideRow = {
    correction_id: CORRECTION_ID,
    correction_kind: "override_field",
    supersedes_correction_id: null,
    is_current: 1,
    is_stale: 0,
    created_at: "2026-08-22 12:00:00.000",
  };

  it("refuses approve/reject while a live override stands, naming its id", () => {
    for (const kind of ["approve_suggestion", "reject_suggestion"]) {
      expect(liveOverrideRefusal(kind, [liveOverrideRow])).toBe(
        `Undo the current override first (${CORRECTION_ID}).`,
      );
    }
  });

  it("allows approve/reject once the override is undone or stale", () => {
    expect(
      liveOverrideRefusal("approve_suggestion", [
        liveOverrideRow,
        {
          correction_id: "44444444-4444-4444-8444-444444444444",
          correction_kind: "undo",
          supersedes_correction_id: CORRECTION_ID,
          is_current: 1,
          is_stale: 0,
          created_at: "2026-08-22 13:00:00.000",
        },
      ]),
    ).toBeNull();
    expect(
      liveOverrideRefusal("reject_suggestion", [
        { ...liveOverrideRow, is_stale: 1 },
      ]),
    ).toBeNull();
  });

  it("never blocks override_field or undo themselves", () => {
    expect(liveOverrideRefusal("override_field", [liveOverrideRow])).toBeNull();
    expect(liveOverrideRefusal("undo", [liveOverrideRow])).toBeNull();
  });
});
