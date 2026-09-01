/**
 * Turns the company-info review page's form posts into field-value rows.
 *
 * Replaces the deleted correction-ledger builder (se-info-review-form.ts):
 * no kinds, no evidence hash and no undo here, because the store's rule is
 * "the latest row written for a field decides it". What is left is four
 * intents, one per way a reviewer can decide a description:
 *
 * - `use-source`      copy one artifact's text (scb / esef / wikidata)
 * - `use-suggestion`  take the model's wording, both languages at once
 * - `edit`            the reviewer's own text, or a tick to release a field
 * - `release`         hand one field back to the pipeline's computed default
 *
 * Client-safe on purpose: React Router only strips `loader`/`action`/
 * `middleware`/`headers` from a route module, so any other export of the route
 * that reaches into a `.server` module drags that module into the client
 * bundle and fails the production build. Only the row-shaping lives here; the
 * write (and the "is this company published?" check) is the route action's,
 * through `appendSeCompanyInfoFieldValues`.
 *
 * The refusals below deliberately reuse `validateSeInfoFieldValue`'s wording
 * ("Unknown field.", "Value cannot be empty.", ...): the same mistake must
 * read the same whether this builder or the validator catches it. Checking
 * here as well is not redundant -- these are the constraints the FORM makes
 * (a use-source post may not claim `llm` provenance, an edit that changed
 * nothing must not reach ClickHouse at all), and catching them purely means
 * they are unit-testable without a live database.
 */
import { ARTIFACT_SOURCES } from "~/lib/se-company-info-payload";
import {
  SE_INFO_FIELDS,
  type SeInfoField,
  type SeInfoFieldValueInput,
} from "~/lib/se-info-field-values";
import type { SeCompanyInfoSuggestionRow } from "~/lib/se-company-info.server";

export type SeInfoFieldValueRequest =
  | { ok: true; inputs: SeInfoFieldValueInput[] }
  | { ok: false; error: string };

/** What the page knows that the form alone cannot carry: whose company this is,
 * and which suggestions are actually on it (a suggestion id names text the
 * reviewer never typed, so it is read from the row rather than the post). */
export interface SeInfoFieldValueContext {
  companyId: string;
  suggestions: SeCompanyInfoSuggestionRow[];
}

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function refuse(error: string): SeInfoFieldValueRequest {
  return { ok: false, error };
}

function isField(value: string): value is SeInfoField {
  return (SE_INFO_FIELDS as readonly string[]).includes(value);
}

/**
 * Best-effort read of a suggestion's JSON body -- the same guard the review
 * workspace renders it with: each half only counts when it really is a string,
 * so an enrichment run that ever emits an object or a number for a description
 * is skipped rather than written into the store as `[object Object]`.
 */
function suggestionText(raw: string): {
  description: string;
  descriptionSv: string;
} {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { description: "", descriptionSv: "" };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { description: "", descriptionSv: "" };
  }
  const body = parsed as Record<string, unknown>;
  const half = (key: string) =>
    typeof body[key] === "string" ? (body[key] as string).trim() : "";
  // Both languages come from one model call (prompt v3); a suggestion recorded
  // before that carries only the English half, and then only that half is used.
  return { description: half("description"), descriptionSv: half("description_sv") };
}

/** One artifact's text, copied verbatim into one field. */
function useSource(
  form: FormData,
  companyId: string,
): SeInfoFieldValueRequest {
  const field = text(form, "field");
  if (!isField(field)) return refuse("Unknown field.");
  const source = text(form, "source");
  // Only the three artifact legs: `llm` and `reviewer` are decided by the other
  // intents, and Dagster reads the source back as provenance, so admitting one
  // here would publish a reviewer's copy-paste as model-enhanced text.
  if (!(ARTIFACT_SOURCES as readonly string[]).includes(source)) {
    return refuse("Unknown source.");
  }
  const value = text(form, "value").trim();
  if (value === "") return refuse("Value cannot be empty.");
  const sourceRef = text(form, "source_ref").trim();
  if (sourceRef === "") return refuse("source_ref is required.");
  const sourceAt = text(form, "source_at").trim();
  return {
    ok: true,
    inputs: [
      {
        companyId,
        field,
        value,
        source,
        sourceRef,
        // Not every artifact card shows an observed_at; no moment is null
        // rather than the empty string the column would otherwise take.
        sourceAt: sourceAt === "" ? null : sourceAt,
      },
    ],
  };
}

/** The model's own wording -- both languages, as one decision. */
function useSuggestion(
  form: FormData,
  context: SeInfoFieldValueContext,
): SeInfoFieldValueRequest {
  const suggestionId = text(form, "suggestion_id").trim().toLowerCase();
  const row = context.suggestions.find(
    (candidate) => candidate.suggestion_id.toLowerCase() === suggestionId,
  );
  // Covers a malformed id too: whatever it is, it names no suggestion of this
  // company, and the text would have to come from somewhere the page never
  // showed.
  if (!row) return refuse("That suggestion is not on this company.");
  const { description, descriptionSv } = suggestionText(row.suggestion);
  if (description === "") return refuse("That suggestion has no description.");
  const common = {
    companyId: context.companyId,
    source: "llm",
    // Dagster parses this back as the published `suggestion_id`, which is how
    // the page later shows which suggestion the live text came from.
    sourceRef: row.suggestion_id,
    sourceAt: row.created_at,
  };
  const inputs: SeInfoFieldValueInput[] = [
    { ...common, field: "description", value: description },
  ];
  if (descriptionSv !== "") {
    inputs.push({ ...common, field: "description_sv", value: descriptionSv });
  }
  return { ok: true, inputs };
}

/**
 * The reviewer's own text. Each language is diffed against the text the page
 * rendered it with (`original_*`), because a field value is permanent until
 * something later replaces it: writing back an untouched description would pin
 * today's computed text for ever, hiding every later pipeline improvement.
 *
 * A ticked `clear_*` box (the literal string `"yes"`, which is what the page's
 * `<Checkbox name="clear_description" value="yes" />` posts) writes NULL -- the
 * release that hands the field back to the pipeline -- and WINS over whatever
 * the textarea holds, so a reviewer who ticks the box without also emptying the
 * text still gets the release they asked for rather than a value they did not.
 * The box is an instruction rather than a diff, so it needs no original.
 *
 * A field whose `original_*` is ABSENT is left alone entirely: there is nothing
 * to diff against, and reading the missing original as "" would turn every
 * rendered textarea into a change and pin today's computed text as a permanent
 * reviewer value. A PRESENT-but-empty original is a real state (a company with
 * no Swedish text yet), and typing into that textarea is the first value.
 * The textarea itself is not guarded the same way: an absent one reads as
 * emptied, which is refused below rather than written.
 *
 * An emptied textarea with the box unticked is a change like any other, so it
 * travels as an empty value and `validateSeInfoFieldValue` refuses the whole
 * decision with "Value cannot be empty." -- clearing a field is the box's job.
 */
function edit(form: FormData, companyId: string): SeInfoFieldValueRequest {
  const note = text(form, "note");
  const inputs: SeInfoFieldValueInput[] = [];
  for (const field of SE_INFO_FIELDS) {
    if (text(form, `clear_${field}`) === "yes") {
      inputs.push({ companyId, field, value: null, source: "reviewer", note });
      continue;
    }
    if (!form.has(`original_${field}`)) continue;
    const value = text(form, field).trim();
    if (value !== text(form, `original_${field}`).trim()) {
      inputs.push({ companyId, field, value, source: "reviewer", note });
    }
  }
  if (inputs.length === 0) return refuse("Nothing changed.");
  return { ok: true, inputs };
}

/** One field, handed back to whatever the pipeline computes for it. */
function release(form: FormData, companyId: string): SeInfoFieldValueRequest {
  const field = text(form, "field");
  if (!isField(field)) return refuse("Unknown field.");
  return {
    ok: true,
    inputs: [{ companyId, field, value: null, source: "reviewer" }],
  };
}

/**
 * Builds the rows one form post decides. A decision may be several rows (the
 * model's two languages, an edit that moved both), which is why this returns a
 * list: `appendSeCompanyInfoFieldValues` writes them under one `created_at`, so
 * they take effect together.
 */
export function buildFieldValueInputs(
  form: FormData,
  context: SeInfoFieldValueContext,
): SeInfoFieldValueRequest {
  switch (text(form, "intent")) {
    case "use-source":
      return useSource(form, context.companyId);
    case "use-suggestion":
      return useSuggestion(form, context);
    case "edit":
      return edit(form, context.companyId);
    case "release":
      return release(form, context.companyId);
    default:
      return refuse("Unknown info action.");
  }
}
