import { describe, expect, it } from "vitest";
import { noteError } from "~/lib/se-person-fields";
import {
  formatConfidence,
  isMatch,
  isPossibleMatch,
  LLM_MATCH_KEY,
  matchNote,
  MATCH_THRESHOLD,
  parseLlmMatch,
  POSSIBLE_MATCH_FLOOR,
  stripLlmMatch,
} from "~/lib/se-person-match";

/** Exactly what `fold.py::_with_llm_match` writes: sorted keys, no spaces, the
 * confidence rounded to four decimals. */
const FOLD_DATA =
  '{"llm_match":{"model":"deepseek-v4-flash","pairs":[{"a":"Erik Bo Bengtsson","b":"Bo Bengtsson","confidence":0.93,"reason":"call name"}],"prompt_version":"se-person-match-v1"},"role_kind":"board_member"}';

describe("se-person-match", () => {
  it("pins the two bands to the fold's own constants, inclusive at the floor", () => {
    expect(MATCH_THRESHOLD).toBe(0.8);
    expect(POSSIBLE_MATCH_FLOOR).toBe(0.5);
    expect(LLM_MATCH_KEY).toBe("llm_match");
    // The fold merged everything at or above the threshold, so the tab reports it.
    expect(isMatch(0.8)).toBe(true);
    expect(isMatch(0.7999)).toBe(false);
    // The band is [0.5, 0.8): the reviewer's to decide, nothing merged it.
    expect(isPossibleMatch(0.5)).toBe(true);
    expect(isPossibleMatch(0.79)).toBe(true);
    expect(isPossibleMatch(0.8)).toBe(false);
    expect(isPossibleMatch(0.49)).toBe(false);
  });

  it("shows a confidence with two decimals", () => {
    expect(formatConfidence(0.93)).toBe("0.93");
    expect(formatConfidence(0.6)).toBe("0.60");
    expect(formatConfidence(1)).toBe("1.00");
  });

  it("writes a merge note the decision parser accepts, whatever the model said", () => {
    expect(matchNote(0.64, "same call name")).toBe("LLM match 0.64: same call name");
    // A reason is model text: it can carry a newline or a tab, which `noteError`
    // refuses on a rule note, and it can be longer than the 500-character limit.
    expect(matchNote(0.64, "two\nlines\tapart")).toBe("LLM match 0.64: two lines apart");
    const long = matchNote(0.64, "x".repeat(900));
    expect(long.length).toBe(500);
    expect(noteError(long)).toBeNull();
    expect(noteError(matchNote(0.64, "two\nlines"))).toBeNull();
    // No reason at all still names the score.
    expect(matchNote(0.5, "   ")).toBe("LLM match 0.50");
  });

  it("reads the fold's llm_match block", () => {
    expect(parseLlmMatch(FOLD_DATA)).toEqual({
      pairs: [
        { a: "Erik Bo Bengtsson", b: "Bo Bengtsson", confidence: 0.93, reason: "call name" },
      ],
      model: "deepseek-v4-flash",
      promptVersion: "se-person-match-v1",
    });
    expect(parseLlmMatch('{"role_kind":"board_member"}')).toBeNull();
    expect(parseLlmMatch("")).toBeNull();
    expect(parseLlmMatch("not json")).toBeNull();
  });

  it("takes the fold-owned key out of the data block and leaves everything else byte for byte", () => {
    expect(stripLlmMatch(FOLD_DATA)).toBe('{"role_kind":"board_member"}');
    // Nothing to strip: the stored text is shown exactly as it is stored.
    expect(stripLlmMatch('{"b":1,"a":2}')).toBe('{"b":1,"a":2}');
    expect(stripLlmMatch("")).toBe("");
    expect(stripLlmMatch("not json")).toBe("not json");
    // The only key: an empty object, which `hasData` already renders as "none".
    expect(stripLlmMatch('{"llm_match":{"pairs":[]}}')).toBe("{}");
  });
});
