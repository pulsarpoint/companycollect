import { describe, expect, it } from "vitest";
import {
  BASIC_INFO_FIELDS,
  BASIC_INFO_LANGUAGES,
  BASIC_INFO_SOURCES,
  BASIC_INFO_STATUSES,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  DEFAULT_BASIC_INFO_FIELD,
  foldPending,
  isBasicInfoField,
  isBasicInfoSource,
  MAX_DESCRIPTION_LENGTH,
  MAX_LEGAL_NAME_LENGTH,
  selectedFieldFromSearch,
  validateSeBasicInfoValue,
} from "~/lib/se-basic-info-fields";

const TODAY = "2026-09-05";
const LEGAL_FORM_CODES = ["16", "49"];

function options(overrides: Partial<{ legalFormCodes: readonly string[]; today: string }> = {}) {
  return { legalFormCodes: LEGAL_FORM_CODES, today: TODAY, ...overrides };
}

describe("basic-info field catalogue", () => {
  it("lists the eight decidable fields in display order", () => {
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

  it("names the eight sources with the reviewer first and the draft last", () => {
    expect(BASIC_INFO_SOURCES).toEqual([
      "reviewer",
      "llm",
      "scb",
      "bolagsverket",
      "esef",
      "wikidata",
      "ratsit",
      "reviewer_draft",
    ]);
    expect(basicInfoSourceLabel("scb")).toBe("SCB");
    expect(basicInfoSourceLabel("llm")).toBe("Model");
    expect(basicInfoSourceLabel("reviewer")).toBe("Reviewer");
    expect(basicInfoSourceLabel("reviewer_draft")).toBe("Draft");
    expect(isBasicInfoSource("reviewer_draft")).toBe(true);
    // An unknown token reads as itself rather than crashing the page.
    expect(basicInfoSourceLabel("somewhere")).toBe("somewhere");
  });

  it("names the two statuses and the two languages", () => {
    expect(BASIC_INFO_STATUSES).toEqual(["active", "inactive"]);
    expect(BASIC_INFO_LANGUAGES).toEqual(["en", "sv"]);
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

describe("validateSeBasicInfoValue", () => {
  it("trims the value before checking anything else", () => {
    expect(validateSeBasicInfoValue("legal_name", "  Acme AB  ", "", options())).toEqual({
      ok: true,
      value: "Acme AB",
      language: "",
    });
  });

  it("refuses an empty (or whitespace-only) value for every field, before the field's own rule", () => {
    expect(validateSeBasicInfoValue("legal_name", "   ", "", options())).toEqual({ ok: false, error: "Value cannot be empty." });
    expect(validateSeBasicInfoValue("status", "", "", options())).toEqual({ ok: false, error: "Value cannot be empty." });
    expect(validateSeBasicInfoValue("lei", "   ", "", options())).toEqual({ ok: false, error: "Value cannot be empty." });
    expect(validateSeBasicInfoValue("description", "  ", "en", options())).toEqual({ ok: false, error: "Value cannot be empty." });
  });

  it("refuses a legal name longer than 500 characters and accepts one at the limit", () => {
    const tooLong = "A".repeat(MAX_LEGAL_NAME_LENGTH + 1);
    expect(validateSeBasicInfoValue("legal_name", tooLong, "", options())).toEqual({ ok: false, error: "Value is longer than 500 characters." });
    const atLimit = "A".repeat(MAX_LEGAL_NAME_LENGTH);
    expect(validateSeBasicInfoValue("legal_name", atLimit, "", options())).toEqual({ ok: true, value: atLimit, language: "" });
  });

  it("accepts a legal form code the loader offered", () => {
    expect(validateSeBasicInfoValue("legal_form_code", "16", "", options())).toEqual({ ok: true, value: "16", language: "" });
  });

  it("refuses a legal form code that fails the two-digit shape", () => {
    expect(validateSeBasicInfoValue("legal_form_code", "1", "", options())).toEqual({ ok: false, error: "Legal form must be one of the SCB codes." });
    expect(validateSeBasicInfoValue("legal_form_code", "abc", "", options())).toEqual({ ok: false, error: "Legal form must be one of the SCB codes." });
  });

  it("refuses a well-shaped legal form code the loader did not offer", () => {
    expect(validateSeBasicInfoValue("legal_form_code", "99", "", options())).toEqual({ ok: false, error: "Legal form must be one of the SCB codes." });
    // An empty list of offered codes refuses everything, even a shape-valid code.
    expect(validateSeBasicInfoValue("legal_form_code", "16", "", options({ legalFormCodes: [] }))).toEqual({ ok: false, error: "Legal form must be one of the SCB codes." });
  });

  it("accepts active or inactive and refuses anything else, without case folding", () => {
    expect(validateSeBasicInfoValue("status", "active", "", options())).toEqual({ ok: true, value: "active", language: "" });
    expect(validateSeBasicInfoValue("status", "inactive", "", options())).toEqual({ ok: true, value: "inactive", language: "" });
    expect(validateSeBasicInfoValue("status", "Active", "", options())).toEqual({ ok: false, error: "Status must be active or inactive." });
    expect(validateSeBasicInfoValue("status", "closed", "", options())).toEqual({ ok: false, error: "Status must be active or inactive." });
  });

  it("accepts a YYYY-MM-DD incorporation date within the 1900-01-01..today range, boundaries included", () => {
    expect(validateSeBasicInfoValue("incorporation_date", "1999-12-31", "", options())).toEqual({ ok: true, value: "1999-12-31", language: "" });
    expect(validateSeBasicInfoValue("incorporation_date", "1900-01-01", "", options())).toEqual({ ok: true, value: "1900-01-01", language: "" });
    expect(validateSeBasicInfoValue("incorporation_date", TODAY, "", options())).toEqual({ ok: true, value: TODAY, language: "" });
  });

  it("refuses a malformed or non-existent calendar incorporation date", () => {
    expect(validateSeBasicInfoValue("incorporation_date", "1999/12/31", "", options())).toEqual({ ok: false, error: "Date must be YYYY-MM-DD." });
    expect(validateSeBasicInfoValue("incorporation_date", "99-12-31", "", options())).toEqual({ ok: false, error: "Date must be YYYY-MM-DD." });
    // February never has a 30th, in any year.
    expect(validateSeBasicInfoValue("incorporation_date", "2024-02-30", "", options())).toEqual({ ok: false, error: "Date must be YYYY-MM-DD." });
  });

  it("refuses an incorporation date outside 1900-01-01..today", () => {
    // 1900-01-01 is the storage floor, not a historical judgment: ClickHouse's
    // Nullable(Date32) saturates any earlier date to 1900-01-01 silently.
    expect(validateSeBasicInfoValue("incorporation_date", "1899-12-31", "", options())).toEqual({ ok: false, error: "Date must be between 1900-01-01 and today." });
    expect(validateSeBasicInfoValue("incorporation_date", "2026-09-06", "", options())).toEqual({ ok: false, error: "Date must be between 1900-01-01 and today." });
  });

  it("upper-cases and accepts a 20-character LEI", () => {
    expect(validateSeBasicInfoValue("lei", "529900t8bm49awsehw01", "", options())).toEqual({ ok: true, value: "529900T8BM49AWSEHW01", language: "" });
  });

  it("refuses an LEI that is not 20 letters or digits after upper-casing", () => {
    expect(validateSeBasicInfoValue("lei", "529900T8BM49AWSEHW0", "", options())).toEqual({ ok: false, error: "LEI must be 20 letters or digits." });
    expect(validateSeBasicInfoValue("lei", "529900T8BM49AWSEHW0!", "", options())).toEqual({ ok: false, error: "LEI must be 20 letters or digits." });
  });

  it("accepts a Q-prefixed Wikidata id and refuses anything else, without case folding", () => {
    expect(validateSeBasicInfoValue("wikidata_id", "Q12345", "", options())).toEqual({ ok: true, value: "Q12345", language: "" });
    expect(validateSeBasicInfoValue("wikidata_id", "q12345", "", options())).toEqual({ ok: false, error: "Wikidata id must be Q followed by digits." });
    expect(validateSeBasicInfoValue("wikidata_id", "Q", "", options())).toEqual({ ok: false, error: "Wikidata id must be Q followed by digits." });
    expect(validateSeBasicInfoValue("wikidata_id", "12345", "", options())).toEqual({ ok: false, error: "Wikidata id must be Q followed by digits." });
  });

  it("accepts a description in English or Swedish and echoes the language back", () => {
    expect(validateSeBasicInfoValue("description", "A company.", "en", options())).toEqual({ ok: true, value: "A company.", language: "en" });
    expect(validateSeBasicInfoValue("description", "Ett bolag.", "sv", options())).toEqual({ ok: true, value: "Ett bolag.", language: "sv" });
  });

  it("refuses a description language outside en/sv", () => {
    expect(validateSeBasicInfoValue("description", "A company.", "no", options())).toEqual({ ok: false, error: "Language must be en or sv." });
    expect(validateSeBasicInfoValue("description", "A company.", "", options())).toEqual({ ok: false, error: "Language must be en or sv." });
  });

  it("refuses a description or description_sv longer than 8000 characters", () => {
    const tooLong = "a".repeat(MAX_DESCRIPTION_LENGTH + 1);
    expect(validateSeBasicInfoValue("description", tooLong, "en", options())).toEqual({ ok: false, error: "Value is longer than 8000 characters." });
    expect(validateSeBasicInfoValue("description_sv", tooLong, "en", options())).toEqual({ ok: false, error: "Value is longer than 8000 characters." });
  });

  it("does not require or echo a language for description_sv", () => {
    expect(validateSeBasicInfoValue("description_sv", "Ett bolag.", "", options())).toEqual({ ok: true, value: "Ett bolag.", language: "" });
    // Whatever the form sent for language, description_sv ignores it and returns ''.
    expect(validateSeBasicInfoValue("description_sv", "Ett bolag.", "en", options())).toEqual({ ok: true, value: "Ett bolag.", language: "" });
  });

  it("returns an empty language for every field but description", () => {
    expect(validateSeBasicInfoValue("legal_name", "Acme AB", "sv", options())).toEqual({ ok: true, value: "Acme AB", language: "" });
    expect(validateSeBasicInfoValue("status", "active", "en", options())).toEqual({ ok: true, value: "active", language: "" });
    expect(validateSeBasicInfoValue("lei", "529900T8BM49AWSEHW01", "sv", options())).toEqual({ ok: true, value: "529900T8BM49AWSEHW01", language: "" });
  });
});
