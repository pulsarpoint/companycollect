import { describe, expect, it } from "vitest";
import { parseSePersonDecision } from "~/lib/se-person-decision-form";

const KEY = "b".repeat(64);
const OTHER = "c".repeat(64);
const SLOT = "r20260910120000123";
const ROLE_CODES = ["board_member", "board_chair"];

function form(entries: Record<string, string>, repeated: [string, string][] = []): FormData {
  const data = new FormData();
  for (const [key, value] of Object.entries(entries)) data.set(key, value);
  for (const [key, value] of repeated) data.append(key, value);
  return data;
}

describe("parseSePersonDecision", () => {
  it("parses fold-now, remove, reset, activate and discard", () => {
    expect(parseSePersonDecision(form({ intent: "fold-now" }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "fold-now" },
    });
    expect(parseSePersonDecision(form({ intent: "remove", person_key: KEY, note: " gone " }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "remove", personKey: KEY, note: "gone" },
    });
    expect(parseSePersonDecision(form({ intent: "reset", person_key: KEY }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "reset", personKey: KEY, note: "" },
    });
    expect(parseSePersonDecision(form({ intent: "activate", slot: SLOT, note: "" }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "activate", slot: SLOT, note: "" },
    });
    expect(parseSePersonDecision(form({ intent: "discard", slot: SLOT }), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "discard", slot: SLOT },
    });
  });

  it("parses merge over the checked keys and split over the checked slots, sorted and deduplicated", () => {
    expect(
      parseSePersonDecision(
        form({ intent: "merge", note: "same person" }, [["person_key", OTHER], ["person_key", KEY], ["person_key", KEY]]),
        ROLE_CODES,
      ),
    ).toEqual({ ok: true, decision: { intent: "merge", personKeys: [KEY, OTHER], note: "same person" } });
    expect(
      parseSePersonDecision(
        form({ intent: "split", note: "two people" }, [["slot", "uid-2:sig-1"], ["slot", "uid-1:sig-1"]]),
        ROLE_CODES,
      ),
    ).toEqual({ ok: true, decision: { intent: "split", slots: ["uid-1:sig-1", "uid-2:sig-1"], note: "two people" } });
  });

  it("parses save-draft with the validated input, the role rows and an optional replaces key", () => {
    const result = parseSePersonDecision(
      form(
        {
          intent: "save-draft", first_name: "Anna", last_name: "Svensson", birth_year: "1975",
          wikidata_id: "Q42", data: "", note: "typed", slot: "", replaces_key: KEY,
        },
        [
          ["role_code", "board_member"], ["role_from", "2023"], ["role_to", "2025"],
          ["role_code", "board_chair"], ["role_from", "2026"], ["role_to", ""],
        ],
      ),
      ROLE_CODES,
    );
    expect(result).toEqual({
      ok: true,
      decision: {
        intent: "save-draft", slot: null, replacesKey: KEY,
        input: {
          firstName: "Anna", lastName: "Svensson", birthYear: "1975", wikidataId: "Q42",
          roles: [
            { code: "board_member", fromYear: "2023", toYear: "2025" },
            { code: "board_chair", fromYear: "2026", toYear: "" },
          ],
          data: "{}", note: "typed",
        },
      },
    });
  });

  it("refuses a bad key, a bad slot, too few merge keys, no split slot, a bad note and an unknown intent", () => {
    expect(parseSePersonDecision(form({ intent: "remove", person_key: "zz" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown person.",
    });
    expect(parseSePersonDecision(form({ intent: "activate" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown draft.",
    });
    expect(parseSePersonDecision(form({ intent: "merge" }, [["person_key", KEY]]), ROLE_CODES)).toEqual({
      ok: false, error: "Pick at least two persons to merge.",
    });
    expect(parseSePersonDecision(form({ intent: "split" }), ROLE_CODES)).toEqual({
      ok: false, error: "Pick at least one observation to split off.",
    });
    expect(parseSePersonDecision(form({ intent: "reset", person_key: KEY, note: "x".repeat(501) }), ROLE_CODES)).toEqual({
      ok: false, error: "Note is longer than 500 characters.",
    });
    // A dialog's note never passes through the sheet's validation, so the parser is
    // where spec 7's "line breaks yes, other control characters no" has to hold.
    expect(parseSePersonDecision(form({ intent: "merge", note: "two\u0007lines" }, [["person_key", KEY], ["person_key", OTHER]]), ROLE_CODES)).toEqual({
      ok: false, error: "Note must be plain text.",
    });
    expect(parseSePersonDecision(form({ intent: "merge", note: "two\nlines" }, [["person_key", KEY], ["person_key", OTHER]]), ROLE_CODES)).toEqual({
      ok: true, decision: { intent: "merge", personKeys: [KEY, OTHER], note: "two\nlines" },
    });
    expect(
      parseSePersonDecision(form({ intent: "save-draft", first_name: "Anna", last_name: "" }), ROLE_CODES),
    ).toEqual({ ok: false, error: "only one name word" });
    expect(parseSePersonDecision(form({ intent: "nope" }), ROLE_CODES)).toEqual({
      ok: false, error: "Unknown intent.",
    });
  });
});
