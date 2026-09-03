import { describe, expect, it } from "vitest";
import { buildFieldValueInputs } from "~/lib/se-info-field-value-form";
import type { SeCompanyInfoSuggestionRow } from "~/lib/se-company-info.server";

const COMPANY_ID = "5565200028";
const SUGGESTION_ID = "11111111-1111-4111-8111-111111111111";

const form = (entries: Record<string, string>) => {
  const built = new FormData();
  for (const [key, value] of Object.entries(entries)) built.append(key, value);
  return built;
};

const suggestion = (
  over: Partial<SeCompanyInfoSuggestionRow> = {},
): SeCompanyInfoSuggestionRow => ({
  suggestion_id: SUGGESTION_ID,
  input_hash: "a".repeat(64),
  suggestion: JSON.stringify({
    description: "Alpha builds payment software.",
    description_sv: "Alpha bygger betalprogramvara.",
    language: "en",
  }),
  model_provider: "deepseek",
  model_name: "m",
  prompt_version: "v3",
  created_at: "2026-08-22 09:00:00.000",
  is_published: 0,
  is_newest: 1,
  ...over,
});

/** The phase-A page decides only the two description columns; the registry
 * lists more, and the builder must accept whatever list it is handed. */
const PHASE_A_FIELDS = ["description", "description_sv"];

const build = (
  entries: Record<string, string>,
  suggestions: SeCompanyInfoSuggestionRow[] = [],
  fields: string[] = PHASE_A_FIELDS,
) =>
  buildFieldValueInputs(form(entries), {
    companyId: COMPANY_ID,
    suggestions,
    fields,
  });

describe("buildFieldValueInputs -- use-source", () => {
  it("copies one artifact's text into one field, carrying its record and moment", () => {
    expect(
      build({
        intent: "use-source",
        field: "description_sv",
        value: "  Alpha bygger betalprogramvara.  ",
        source: "scb",
        source_ref: "scb:5565200028",
        source_at: "2026-08-01 00:00:00.000",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: "Alpha bygger betalprogramvara.",
          source: "scb",
          sourceRef: "scb:5565200028",
          sourceAt: "2026-08-01 00:00:00.000",
        },
      ],
    });
  });

  it("treats an absent source_at as no moment at all", () => {
    const built = build({
      intent: "use-source",
      field: "description",
      value: "Alpha builds payment software.",
      source: "wikidata",
      source_ref: "wikidata:Q1",
    });
    expect(built).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: "Alpha builds payment software.",
          source: "wikidata",
          sourceRef: "wikidata:Q1",
          sourceAt: null,
        },
      ],
    });
  });

  it("refuses a field that is not one of the two published columns", () => {
    expect(
      build({
        intent: "use-source",
        field: "legal_name",
        value: "Alpha AB",
        source: "scb",
        source_ref: "scb:1",
      }),
    ).toEqual({ ok: false, error: "Unknown field." });
  });

  it("refuses a source that is not an artifact leg", () => {
    // llm and reviewer are real sources of the store, but they are decided by
    // the other intents -- using one here would forge provenance (Dagster
    // publishes an llm row as llm_enhanced with a suggestion link).
    for (const source of ["llm", "reviewer", "hearsay"]) {
      expect(
        build({
          intent: "use-source",
          field: "description",
          value: "x",
          source,
          source_ref: SUGGESTION_ID,
        }),
      ).toEqual({ ok: false, error: "Unknown source." });
    }
  });

  it("refuses empty text and a missing record reference", () => {
    expect(
      build({
        intent: "use-source",
        field: "description",
        value: "   ",
        source: "scb",
        source_ref: "scb:1",
      }),
    ).toEqual({ ok: false, error: "Value cannot be empty." });
    expect(
      build({
        intent: "use-source",
        field: "description",
        value: "x",
        source: "scb",
        source_ref: " ",
      }),
    ).toEqual({ ok: false, error: "source_ref is required." });
  });

  it("accepts any field the registry hands it", () => {
    expect(
      build(
        {
          intent: "use-source",
          field: "legal_name",
          value: "Alpha AB",
          source: "scb",
          source_ref: "scb:1",
        },
        [],
        [...PHASE_A_FIELDS, "legal_name"],
      ),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "legal_name",
          value: "Alpha AB",
          source: "scb",
          sourceRef: "scb:1",
          sourceAt: null,
        },
      ],
    });
  });
});

describe("buildFieldValueInputs -- use-suggestion", () => {
  it("writes both languages of the suggestion, sourced to the model", () => {
    expect(
      build({ intent: "use-suggestion", suggestion_id: SUGGESTION_ID }, [
        suggestion(),
      ]),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: "Alpha builds payment software.",
          source: "llm",
          sourceRef: SUGGESTION_ID,
          sourceAt: "2026-08-22 09:00:00.000",
        },
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: "Alpha bygger betalprogramvara.",
          source: "llm",
          sourceRef: SUGGESTION_ID,
          sourceAt: "2026-08-22 09:00:00.000",
        },
      ],
    });
  });

  it("writes English alone when the suggestion has no usable Swedish half", () => {
    for (const body of [
      '{"description":"Alpha builds payment software."}',
      '{"description":"Alpha builds payment software.","description_sv":""}',
      '{"description":"Alpha builds payment software.","description_sv":{"nested":"object"}}',
    ]) {
      const built = build(
        { intent: "use-suggestion", suggestion_id: SUGGESTION_ID },
        [suggestion({ suggestion: body })],
      );
      expect(built.ok).toBe(true);
      expect(built.ok && built.inputs).toHaveLength(1);
      expect(built.ok && built.inputs[0].field).toBe("description");
    }
  });

  it("refuses a suggestion that is not on this company", () => {
    expect(
      build({ intent: "use-suggestion", suggestion_id: SUGGESTION_ID }, []),
    ).toEqual({ ok: false, error: "That suggestion is not on this company." });
    expect(
      build({ intent: "use-suggestion", suggestion_id: "not-a-uuid" }, [
        suggestion(),
      ]),
    ).toEqual({ ok: false, error: "That suggestion is not on this company." });
  });

  it("refuses a suggestion body with no description to use", () => {
    for (const body of [
      "not json at all",
      "[]",
      '{"language":"en"}',
      '{"description":42}',
      '{"description":"   "}',
    ]) {
      expect(
        build({ intent: "use-suggestion", suggestion_id: SUGGESTION_ID }, [
          suggestion({ suggestion: body }),
        ]),
      ).toEqual({ ok: false, error: "That suggestion has no description." });
    }
  });
});

describe("buildFieldValueInputs -- edit", () => {
  it("writes only the language the reviewer actually changed, with the note", () => {
    expect(
      build({
        intent: "edit",
        description: "  Alpha builds payment rails.  ",
        original_description: "Alpha builds payment software.",
        description_sv: "  Alpha bygger betalprogramvara.  ",
        original_description_sv: "Alpha bygger betalprogramvara.",
        note: "Sharper wording",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: "Alpha builds payment rails.",
          source: "reviewer",
          note: "Sharper wording",
        },
      ],
    });
  });

  it("writes a release row for a ticked clear box, and that box beats edited text", () => {
    expect(
      build({
        intent: "edit",
        description: "Something else entirely",
        original_description: "Alpha builds payment software.",
        clear_description: "yes",
        description_sv: "Alpha bygger betalprogramvara.",
        original_description_sv: "Alpha bygger betalprogramvara.",
        note: "",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: null,
          source: "reviewer",
          note: "",
        },
      ],
    });
  });

  it("writes both languages when both moved", () => {
    const built = build({
      intent: "edit",
      description: "New english",
      original_description: "Old english",
      description_sv: "",
      original_description_sv: "Gammal svenska",
      clear_description_sv: "yes",
      note: "Both",
    });
    expect(built).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: "New english",
          source: "reviewer",
          note: "Both",
        },
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: null,
          source: "reviewer",
          note: "Both",
        },
      ],
    });
  });

  it("writes only the Swedish half when only it moved", () => {
    expect(
      build({
        intent: "edit",
        description: "Alpha builds payment software.",
        original_description: "Alpha builds payment software.",
        description_sv: "Alpha bygger betalningsprogramvara.",
        original_description_sv: "Alpha bygger betalprogramvara.",
        note: "",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: "Alpha bygger betalningsprogramvara.",
          source: "reviewer",
          note: "",
        },
      ],
    });
  });

  it("leaves a field alone when the post carries no original to diff it against", () => {
    // A form that forgot the hidden original_* field would otherwise look like
    // "the reviewer typed all of this", pinning today's computed text as a
    // permanent reviewer value.
    expect(
      build({
        intent: "edit",
        description: "Alpha builds payment software.",
        description_sv: "Alpha bygger betalprogramvara.",
        note: "",
      }),
    ).toEqual({ ok: false, error: "Nothing changed." });
    // The clear box still decides on its own -- it is an instruction, not a diff.
    const cleared = build({
      intent: "edit",
      description: "Alpha builds payment software.",
      clear_description: "yes",
      note: "",
    });
    expect(cleared).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: null,
          source: "reviewer",
          note: "",
        },
      ],
    });
  });

  it("reads a present-but-empty original as 'no text yet'", () => {
    // The company with no Swedish description: the page renders the textarea
    // with an empty original_*, so leaving it empty is unchanged and typing in
    // it is the first value.
    expect(
      build({
        intent: "edit",
        description: "Alpha builds payment software.",
        original_description: "Alpha builds payment software.",
        description_sv: "",
        original_description_sv: "",
        note: "",
      }),
    ).toEqual({ ok: false, error: "Nothing changed." });
    expect(
      build({
        intent: "edit",
        description: "Alpha builds payment software.",
        original_description: "Alpha builds payment software.",
        description_sv: "Alpha bygger betalprogramvara.",
        original_description_sv: "",
        note: "",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: "Alpha bygger betalprogramvara.",
          source: "reviewer",
          note: "",
        },
      ],
    });
  });

  it("emits an empty value for a textarea emptied without ticking its box", () => {
    // Deliberate: clearing a field is the clear box's job, so this travels as
    // a value and validateSeInfoFieldValue refuses the whole decision with
    // "Value cannot be empty." (pinned in tests/se-info-field-values.test.ts)
    // rather than being silently dropped or turned into a release.
    expect(
      build({
        intent: "edit",
        description: "   ",
        original_description: "Alpha builds payment software.",
        description_sv: "Alpha bygger betalprogramvara.",
        original_description_sv: "Alpha bygger betalprogramvara.",
        note: "",
      }),
    ).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description",
          value: "",
          source: "reviewer",
          note: "",
        },
      ],
    });
  });

  it("refuses an edit that changed nothing", () => {
    expect(
      build({
        intent: "edit",
        description: "  Alpha builds payment software.  ",
        original_description: "Alpha builds payment software.",
        description_sv: "Alpha bygger betalprogramvara.",
        original_description_sv: "Alpha bygger betalprogramvara.",
        note: "no-op",
      }),
    ).toEqual({ ok: false, error: "Nothing changed." });
  });
});

describe("buildFieldValueInputs -- release", () => {
  it("hands one field back to the pipeline", () => {
    expect(build({ intent: "release", field: "description_sv" })).toEqual({
      ok: true,
      inputs: [
        {
          companyId: COMPANY_ID,
          field: "description_sv",
          value: null,
          source: "reviewer",
        },
      ],
    });
  });

  it("refuses a field that is not one of the two published columns", () => {
    expect(build({ intent: "release", field: "legal_name" })).toEqual({
      ok: false,
      error: "Unknown field.",
    });
  });
});

describe("buildFieldValueInputs -- intent dispatch", () => {
  it("refuses an unknown or missing intent", () => {
    expect(build({ intent: "approve_suggestion" })).toEqual({
      ok: false,
      error: "Unknown info action.",
    });
    expect(build({})).toEqual({ ok: false, error: "Unknown info action." });
  });
});
