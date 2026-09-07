import { describe, expect, it } from "vitest";
import { parseSeAddressDecision } from "~/lib/se-address-decision-form";

const KEY = "b".repeat(64);
function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [k, v] of Object.entries(entries)) data.set(k, v);
  return data;
}

describe("parseSeAddressDecision", () => {
  it("parses fold-now, remove, reset, activate and discard", () => {
    expect(parseSeAddressDecision(form({ intent: "fold-now" }))).toEqual({ ok: true, decision: { intent: "fold-now" } });
    expect(parseSeAddressDecision(form({ intent: "remove", address_key: KEY, note: " why " }))).toEqual({ ok: true, decision: { intent: "remove", addressKey: KEY, note: "why" } });
    expect(parseSeAddressDecision(form({ intent: "reset", address_key: KEY }))).toEqual({ ok: true, decision: { intent: "reset", addressKey: KEY, note: "" } });
    expect(parseSeAddressDecision(form({ intent: "activate", slot: "r20260907100000000", note: "" }))).toEqual({ ok: true, decision: { intent: "activate", slot: "r20260907100000000", note: "" } });
    expect(parseSeAddressDecision(form({ intent: "discard", slot: "r20260907100000000" }))).toEqual({ ok: true, decision: { intent: "discard", slot: "r20260907100000000" } });
  });
  it("parses save-draft with the validated input, an optional slot and an optional replaces key", () => {
    const result = parseSeAddressDecision(form({
      intent: "save-draft", care_of: "", street_line: "Storgatan 5", postal_code: "111 22", city: "Stockholm",
      country: "SE", kind: "postal", note: "typed", slot: "", replaces_key: KEY,
    }));
    expect(result).toEqual({
      ok: true,
      decision: {
        intent: "save-draft", slot: null, replacesKey: KEY,
        input: { careOf: "", streetLine: "Storgatan 5", postalCode: "11122", city: "Stockholm", country: "SE", kind: "postal", note: "typed" },
      },
    });
  });
  it("refuses a bad key, a missing slot, a bad note and an unknown intent", () => {
    expect(parseSeAddressDecision(form({ intent: "remove", address_key: "zz" }))).toEqual({ ok: false, error: "Unknown address." });
    expect(parseSeAddressDecision(form({ intent: "activate" }))).toEqual({ ok: false, error: "Unknown draft." });
    expect(parseSeAddressDecision(form({ intent: "reset", address_key: KEY, note: "x".repeat(501) }))).toEqual({ ok: false, error: "Note is longer than 500 characters." });
    expect(parseSeAddressDecision(form({ intent: "save-draft", street_line: "", postal_code: "11122", city: "S", country: "SE", kind: "postal" }))).toEqual({ ok: false, error: "A street line or a box is required." });
    expect(parseSeAddressDecision(form({ intent: "nope" }))).toEqual({ ok: false, error: "Unknown intent." });
  });
});
