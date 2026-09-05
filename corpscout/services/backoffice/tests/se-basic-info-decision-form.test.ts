import { describe, expect, it } from "vitest";
import { parseSeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";

function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [key, value] of Object.entries(entries)) data.set(key, value);
  return data;
}

describe("parseSeBasicInfoDecision", () => {
  it("accepts use-this with a field, a non-reviewer source and an optional note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "scb", note: " keep " }))).toEqual({
      ok: true,
      decision: { intent: "use-this", field: "status", source: "scb", note: "keep" },
    });
  });

  it("refuses use-this from the reviewer, the reviewer's draft, or an unknown source or field", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "reviewer" }))).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "reviewer_draft" }))).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "elsewhere" }))).toEqual({ ok: false, error: "Unknown source." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "description_language", source: "scb" }))).toEqual({ ok: false, error: "Unknown field." });
  });

  it("accepts reset with a field and an optional note, ignoring any source; fold-now with nothing else", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "reset", field: "description", note: " back to default " }))).toEqual({
      ok: true,
      decision: { intent: "reset", field: "description", note: "back to default" },
    });
    // A stray source on a reset is not an error: the reset covers every rule of the field.
    expect(parseSeBasicInfoDecision(form({ intent: "reset", field: "status", source: "scb" }))).toEqual({
      ok: true,
      decision: { intent: "reset", field: "status", note: "" },
    });
    expect(parseSeBasicInfoDecision(form({ intent: "fold-now" }))).toEqual({ ok: true, decision: { intent: "fold-now" } });
  });

  it("refuses reset without a known field", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "reset", field: "description_language" }))).toEqual({ ok: false, error: "Unknown field." });
    expect(parseSeBasicInfoDecision(form({ intent: "reset" }))).toEqual({ ok: false, error: "Unknown field." });
  });

  it("refuses an unknown intent (release is gone, edit/activate/discard are not typos) and an over-long note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "release", field: "status", source: "scb" }))).toEqual({ ok: false, error: "Unknown intent." });
    expect(parseSeBasicInfoDecision(form({ intent: "", field: "status" }))).toEqual({ ok: false, error: "Unknown intent." });
    expect(parseSeBasicInfoDecision(form({ intent: "reset", field: "status", note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
  });
});

const TODAY = "2026-09-05";
const LEGAL_FORM_CODES = { "legal_form_codes": " 16 , 49 " };

describe("parseSeBasicInfoDecision: edit", () => {
  it("accepts an edit that validates, trimming the value and the note", () => {
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "legal_name", value: "  Acme AB  ", note: " draft it " }),
        { today: TODAY },
      ),
    ).toEqual({
      ok: true,
      decision: { intent: "edit", field: "legal_name", value: "Acme AB", language: "", note: "draft it" },
    });
  });

  it("upper-cases an LEI edit and carries the validated value through", () => {
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "lei", value: "529900t8bm49awsehw01", note: "" }),
        { today: TODAY },
      ),
    ).toEqual({
      ok: true,
      decision: { intent: "edit", field: "lei", value: "529900T8BM49AWSEHW01", language: "", note: "" },
    });
  });

  it("carries a description's validated language through the edit decision", () => {
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "description", value: "A company.", language: "en", note: "" }),
        { today: TODAY },
      ),
    ).toEqual({
      ok: true,
      decision: { intent: "edit", field: "description", value: "A company.", language: "en", note: "" },
    });
  });

  it("reads legal_form_codes from the hidden field, split on commas and trimmed, only for legal_form_code", () => {
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "legal_form_code", value: "16", ...LEGAL_FORM_CODES }),
        { today: TODAY },
      ),
    ).toEqual({
      ok: true,
      decision: { intent: "edit", field: "legal_form_code", value: "16", language: "", note: "" },
    });
    // Not among the offered codes: refused even though it is shape-valid.
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "legal_form_code", value: "99", ...LEGAL_FORM_CODES }),
        { today: TODAY },
      ),
    ).toEqual({ ok: false, error: "Legal form must be one of the SCB codes." });
  });

  it("refuses edit for an unknown field before looking at the note or the value", () => {
    expect(
      parseSeBasicInfoDecision(form({ intent: "edit", field: "description_language", value: "x" }), { today: TODAY }),
    ).toEqual({ ok: false, error: "Unknown field." });
  });

  it("refuses edit for an over-long note before validating the value", () => {
    expect(
      parseSeBasicInfoDecision(
        form({ intent: "edit", field: "legal_name", value: "", note: "x".repeat(501) }),
        { today: TODAY },
      ),
    ).toEqual({ ok: false, error: "Note is longer than 500 characters." });
  });

  it("refuses edit with the value validation error verbatim", () => {
    expect(
      parseSeBasicInfoDecision(form({ intent: "edit", field: "status", value: "closed" }), { today: TODAY }),
    ).toEqual({ ok: false, error: "Status must be active or inactive." });
    expect(
      parseSeBasicInfoDecision(form({ intent: "edit", field: "legal_name", value: "   " }), { today: TODAY }),
    ).toEqual({ ok: false, error: "Value cannot be empty." });
  });

  it("defaults today to the current UTC date when no override is given", () => {
    const utcToday = new Date().toISOString().slice(0, 10);
    expect(
      parseSeBasicInfoDecision(form({ intent: "edit", field: "incorporation_date", value: utcToday })),
    ).toEqual({
      ok: true,
      decision: { intent: "edit", field: "incorporation_date", value: utcToday, language: "", note: "" },
    });
  });
});

describe("parseSeBasicInfoDecision: activate", () => {
  it("accepts activate with a field and an optional note, trimmed", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "activate", field: "status", note: " looks right " }))).toEqual({
      ok: true,
      decision: { intent: "activate", field: "status", note: "looks right" },
    });
    expect(parseSeBasicInfoDecision(form({ intent: "activate", field: "status" }))).toEqual({
      ok: true,
      decision: { intent: "activate", field: "status", note: "" },
    });
  });

  it("refuses activate without a known field and with an over-long note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "activate", field: "description_language" }))).toEqual({ ok: false, error: "Unknown field." });
    expect(parseSeBasicInfoDecision(form({ intent: "activate" }))).toEqual({ ok: false, error: "Unknown field." });
    expect(parseSeBasicInfoDecision(form({ intent: "activate", field: "status", note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
  });
});

describe("parseSeBasicInfoDecision: discard", () => {
  it("accepts discard with only a field, carrying no note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "discard", field: "lei" }))).toEqual({
      ok: true,
      decision: { intent: "discard", field: "lei" },
    });
    // A note sent alongside discard is ignored: discard has no note field.
    expect(parseSeBasicInfoDecision(form({ intent: "discard", field: "lei", note: "ignored" }))).toEqual({
      ok: true,
      decision: { intent: "discard", field: "lei" },
    });
  });

  it("refuses discard without a known field", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "discard", field: "description_language" }))).toEqual({ ok: false, error: "Unknown field." });
    expect(parseSeBasicInfoDecision(form({ intent: "discard" }))).toEqual({ ok: false, error: "Unknown field." });
  });
});
