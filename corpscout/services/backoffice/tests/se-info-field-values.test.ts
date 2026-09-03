import { describe, expect, it } from "vitest";
import {
  fieldVocabulary,
  REVIEWER_SOURCE,
  SeInfoFieldValueValidationError,
  validateSeInfoFieldValue,
  type SeInfoFieldValueInput,
} from "~/lib/se-info-field-values";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const SUGGESTION = "11111111-1111-4111-8111-111111111111";
const VOCABULARY = fieldVocabulary(REGISTRY_FIXTURE);
const validate = (input: SeInfoFieldValueInput) =>
  validateSeInfoFieldValue(input, VOCABULARY);
const base = {
  companyId: "5565200028",
  field: "description",
  source: "scb",
  sourceRef: "scb:5565200028",
};

describe("fieldVocabulary", () => {
  // The vocabulary is DERIVED from the registry export (spec 11): the field
  // names as listed, and the union of every field's sources plus `reviewer`
  // -- reviewer decisions are not a source in the registry (spec 4.1) but
  // are a source of a decision row (spec 6).
  it("lists the registry's fields and the union of their sources plus reviewer", () => {
    expect(VOCABULARY.fields).toEqual([
      "description",
      "description_sv",
      "legal_name",
      "website",
    ]);
    expect(VOCABULARY.sources).toEqual([
      "llm",
      "esef",
      "wikidata",
      "scb",
      "bolagsverket",
      "domains",
      REVIEWER_SOURCE,
    ]);
  });

  it("names reviewer exactly once even when a registry lists it", () => {
    expect(
      fieldVocabulary({ fields: [{ field: "x", sources: ["reviewer"] }] })
        .sources,
    ).toEqual(["reviewer"]);
  });

  it("lists the fields the registry marks structured", () => {
    // The fixture marks none: today's twelve info fields are all text.
    expect(VOCABULARY.structured).toEqual([]);
  });
});

describe("validateSeInfoFieldValue", () => {
  it("returns the ClickHouse row shape, trimmed", () => {
    expect(
      validate({
        ...base,
        value: "  Alpha builds payment software.  ",
        sourceRef: "  scb:5565200028  ",
        sourceAt: "  2026-08-01 00:00:00.000  ",
        note: "  from SCB  ",
      }),
    ).toEqual({
      company_id: "5565200028",
      field: "description",
      value: "Alpha builds payment software.",
      source: "scb",
      source_ref: "scb:5565200028",
      source_at: "2026-08-01 00:00:00.000",
      note: "from SCB",
    });
  });

  it("accepts both Swedish company-id lengths and refuses anything else", () => {
    // Legal entities carry a 10-digit organisationsnummer; sole traders carry a
    // 12-digit personnummer-based id -- both are published to se_company_info.
    expect(
      validate({ ...base, companyId: " 5565200028 ", value: "x" })
        .company_id,
    ).toBe("5565200028");
    expect(
      validate({
        ...base,
        companyId: "195560125220",
        value: "x",
      }).company_id,
    ).toBe("195560125220");
    for (const companyId of ["556520002", "55652000280", "556520-0028", ""]) {
      expect(() =>
        validate({ ...base, companyId, value: "x" }),
      ).toThrow(SeInfoFieldValueValidationError);
    }
  });

  it("accepts only the known fields", () => {
    expect(
      validate({ ...base, field: "description_sv", value: "x" })
        .field,
    ).toBe("description_sv");
    expect(validate({ ...base, field: "legal_name", value: "x" }).field).toBe(
      "legal_name",
    );
    expect(() => validate({ ...base, field: "not_a_field", value: "x" })).toThrow(
      "Unknown field.",
    );
  });

  // A structured field's value is JSON (amount / currency / fiscal year) and a
  // decision row carries text alone, so a text decision on one would publish a
  // long-table value the wide row's JSONExtract cannot express. Refused until
  // phase B gives those fields their own editor.
  it("refuses a decision on a structured field", () => {
    const structured = fieldVocabulary({
      fields: [{ field: "employee_count", sources: ["scb"], structured: true }],
    });
    expect(structured.structured).toEqual(["employee_count"]);
    expect(() =>
      validateSeInfoFieldValue(
        {
          companyId: "5565200028",
          field: "employee_count",
          value: "42",
          source: "scb",
          sourceRef: "scb:5565200028",
        },
        structured,
      ),
    ).toThrow("Structured fields cannot be decided as text yet.");
  });

  it("accepts only the known sources", () => {
    for (const source of VOCABULARY.sources) {
      const sourceRef = source === "llm" ? SUGGESTION : "ref:1";
      expect(
        validate({ ...base, source, sourceRef, value: "x" })
          .source,
      ).toBe(source);
    }
    expect(() =>
      validate({ ...base, source: "human", value: "x" }),
    ).toThrow("Unknown source.");
  });

  // A NULL value is the release instruction: it hands the field back to the
  // pipeline's computed default, so it must survive validation untouched
  // rather than becoming "" (which would pin the field to an empty string).
  it("keeps a null value null, and refuses a blank one", () => {
    expect(
      validate({ ...base, source: "reviewer", value: null })
        .value,
    ).toBeNull();
    expect(() =>
      validate({ ...base, source: "reviewer", value: "   " }),
    ).toThrow("Value cannot be empty.");
    // Exactly what se-info-field-value-form's `edit` intent emits for a
    // textarea the reviewer emptied without ticking its clear box: the empty
    // string is a value, not a release, so the whole decision is refused here
    // rather than pinning the published column to ''.
    expect(() =>
      validate({ ...base, source: "reviewer", value: "" }),
    ).toThrow("Value cannot be empty.");
  });

  // FormData plumbing hands over "no value" as undefined as easily as null
  // (form.get() on an absent field), and both mean the same thing here: release.
  it("treats an undefined value as a release, like null", () => {
    expect(
      validate({
        ...base,
        source: "reviewer",
        value: undefined as unknown as string | null,
      }).value,
    ).toBeNull();
  });

  it("requires a UUID source_ref for llm and a non-empty one for the artifact sources", () => {
    // llm's source_ref is the suggestion_id Dagster reads back as a UUID
    // (apply_field_values parses it), so a free-text ref would be dropped there.
    expect(
      validate({
        ...base,
        source: "llm",
        sourceRef: ` ${SUGGESTION.toUpperCase()} `,
        value: "x",
      }).source_ref,
    ).toBe(SUGGESTION);
    expect(() =>
      validate({
        ...base,
        source: "llm",
        sourceRef: "suggestion-1",
        value: "x",
      }),
    ).toThrow("source_ref must be a UUID.");
    for (const source of ["scb", "esef", "wikidata"]) {
      expect(() =>
        validate({ ...base, source, sourceRef: "  ", value: "x" }),
      ).toThrow("source_ref is required.");
      expect(() =>
        validate({ ...base, source, value: "x", sourceRef: undefined }),
      ).toThrow("source_ref is required.");
    }
  });

  // A reviewer's own wording comes from no record, so the column is '' by
  // convention (migration 000371's comment) -- whatever the form posted.
  it("forces an empty source_ref for reviewer rows", () => {
    expect(
      validate({
        ...base,
        source: "reviewer",
        sourceRef: "scb:1",
        value: "x",
      }).source_ref,
    ).toBe("");
  });

  it("defaults source_at to null and note to the empty string", () => {
    const draft = validate({
      ...base,
      source: "reviewer",
      value: "x",
    });
    expect(draft.source_at).toBeNull();
    expect(draft.note).toBe("");
  });

  it("passes an explicit null source_at through", () => {
    expect(
      validate({
        ...base,
        source: "reviewer",
        value: "x",
        sourceAt: null,
      }).source_at,
    ).toBeNull();
    // A blank string is not a timestamp: it reads as "no source_at".
    expect(
      validate({
        ...base,
        source: "reviewer",
        value: "x",
        sourceAt: "   ",
      }).source_at,
    ).toBeNull();
  });

  it("caps the note at 1000 characters", () => {
    expect(
      validate({
        ...base,
        source: "reviewer",
        value: "x",
        note: "n".repeat(1000),
      }).note,
    ).toHaveLength(1000);
    expect(() =>
      validate({
        ...base,
        source: "reviewer",
        value: "x",
        note: "n".repeat(1001),
      }),
    ).toThrow("Note is too long (max 1000 characters).");
  });
});
