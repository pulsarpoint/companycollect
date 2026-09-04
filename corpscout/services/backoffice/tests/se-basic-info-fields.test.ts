import { describe, expect, it } from "vitest";
import {
  BASIC_INFO_FIELDS,
  BASIC_INFO_SOURCES,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  DEFAULT_BASIC_INFO_FIELD,
  foldPending,
  isBasicInfoField,
  isBasicInfoSource,
  selectedFieldFromSearch,
} from "~/lib/se-basic-info-fields";

describe("basic-info field catalogue", () => {
  it("lists the nine entity fields in display order", () => {
    expect(BASIC_INFO_FIELDS.map((field) => field.name)).toEqual([
      "legal_name",
      "legal_form_code",
      "status",
      "incorporation_date",
      "lei",
      "wikidata_id",
      "description",
      "description_sv",
    ]);
    expect(basicInfoFieldLabel("legal_form_code")).toBe("Legal form");
    expect(basicInfoFieldLabel("description_sv")).toBe("Description (Swedish)");
  });

  it("guards field and source names", () => {
    expect(isBasicInfoField("lei")).toBe(true);
    expect(isBasicInfoField("description_language")).toBe(false);
    expect(isBasicInfoField("")).toBe(false);
    expect(isBasicInfoSource("ratsit")).toBe(true);
    expect(isBasicInfoSource("scb ")).toBe(false);
  });

  it("names the seven sources with the reviewer first", () => {
    expect(BASIC_INFO_SOURCES).toEqual([
      "reviewer",
      "llm",
      "scb",
      "bolagsverket",
      "esef",
      "wikidata",
      "ratsit",
    ]);
    expect(basicInfoSourceLabel("scb")).toBe("SCB");
    expect(basicInfoSourceLabel("llm")).toBe("Model");
    expect(basicInfoSourceLabel("reviewer")).toBe("Reviewer");
    // An unknown token reads as itself rather than crashing the page.
    expect(basicInfoSourceLabel("somewhere")).toBe("somewhere");
  });

  it("reads the selected field from the URL and falls back to legal name", () => {
    expect(selectedFieldFromSearch(new URLSearchParams("field=status"))).toBe("status");
    expect(selectedFieldFromSearch(new URLSearchParams("field=nope"))).toBe(DEFAULT_BASIC_INFO_FIELD);
    expect(selectedFieldFromSearch(new URLSearchParams(""))).toBe("legal_name");
  });

  it("marks a fold pending when a suggestion is newer than the fold", () => {
    expect(foldPending("2026-09-04 17:04:01.293", ["2026-09-04 17:46:53.852"])).toBe(true);
    expect(foldPending("2026-09-04 17:04:01.293", ["2026-09-03 18:16:21.117"])).toBe(false);
    // Never folded but suggested: pending. Never folded, nothing suggested: not.
    expect(foldPending(null, ["2026-09-03 18:16:21.117"])).toBe(true);
    expect(foldPending(null, [])).toBe(false);
  });
});
