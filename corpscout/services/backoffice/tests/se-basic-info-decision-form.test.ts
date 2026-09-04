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

  it("refuses use-this from the reviewer or an unknown source or field", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "reviewer" }))).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "status", source: "elsewhere" }))).toEqual({ ok: false, error: "Unknown source." });
    expect(parseSeBasicInfoDecision(form({ intent: "use-this", field: "description_language", source: "scb" }))).toEqual({ ok: false, error: "Unknown field." });
  });

  it("accepts release with a field, fold-now with nothing else", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "release", field: "description" }))).toEqual({
      ok: true,
      decision: { intent: "release", field: "description", note: "" },
    });
    expect(parseSeBasicInfoDecision(form({ intent: "fold-now" }))).toEqual({ ok: true, decision: { intent: "fold-now" } });
  });

  it("refuses an unknown intent and an over-long note", () => {
    expect(parseSeBasicInfoDecision(form({ intent: "edit", field: "status" }))).toEqual({ ok: false, error: "Unknown intent." });
    expect(parseSeBasicInfoDecision(form({ intent: "release", field: "status", note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
  });
});
