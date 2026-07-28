import { describe, expect, it } from "vitest";
import { legalFormCodeOf } from "./entity-type.server";

describe("legalFormCodeOf", () => {
  it("reads the code from a plain SELECT * record", () => {
    expect(legalFormCodeOf("no", { legal_form_code: "KOMM" })).toBe("KOMM");
  });

  it("finds the code when a custom recordQuery has aliased it", () => {
    // Sweden joins its translations, so its detail record comes back with
    // `c.legal_form_code`. An exact-key lookup silently found nothing there and
    // every Swedish entity rendered unclassified — including the state agencies
    // the classification exists to label.
    expect(legalFormCodeOf("se", { "c.legal_form_code": "81" })).toBe("81");
  });

  it("prefers the unaliased key when a record somehow carries both", () => {
    expect(
      legalFormCodeOf("se", { legal_form_code: "82", "c.legal_form_code": "81" }),
    ).toBe("82");
  });

  it("returns empty for a register that publishes no legal form", () => {
    // Brazil and Denmark have no such column, so there is nothing to classify
    // and nothing to guess from.
    expect(legalFormCodeOf("br", { legal_form_code: "X" })).toBe("");
    expect(legalFormCodeOf("dk", {})).toBe("");
  });

  it("returns empty rather than a stringified null when the row omits it", () => {
    expect(legalFormCodeOf("no", {})).toBe("");
    expect(legalFormCodeOf("no", { legal_form_code: null })).toBe("");
  });

  it("does not match a column that merely ends with the same word", () => {
    // `legal_form_label_en` sits beside the code in the Swedish record; picking
    // it up would classify on a display label instead of a code.
    expect(legalFormCodeOf("se", { legal_form_label_en: "Statlig myndighet" })).toBe(
      "",
    );
  });
});
