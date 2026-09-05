/**
 * Turns the Info tab's form posts into one decision. Client-safe (no `.server`
 * import): the route's own module must not drag the server module into the
 * client bundle, and the refusals are unit-testable without ClickHouse.
 *
 * Six intents: `use-this` prefers one source for one field (a company rule),
 * `reset` withdraws every company rule for one field so the global precedence
 * applies again, `fold-now` launches the targeted fold, `edit` writes a
 * reviewer draft value for one field, `activate` promotes that draft into the
 * active reviewer row, `discard` drops the draft.
 */
import {
  isBasicInfoField,
  isBasicInfoSource,
  validateSeBasicInfoValue,
  type SeBasicInfoField,
  type SeBasicInfoSource,
} from "~/lib/se-basic-info-fields";

export type SeBasicInfoDecision =
  | { intent: "use-this"; field: SeBasicInfoField; source: Exclude<SeBasicInfoSource, "reviewer" | "reviewer_draft">; note: string }
  | { intent: "reset"; field: SeBasicInfoField; note: string }
  | { intent: "fold-now" }
  | { intent: "edit"; field: SeBasicInfoField; value: string; language: string; note: string }
  | { intent: "activate"; field: SeBasicInfoField; note: string }
  | { intent: "discard"; field: SeBasicInfoField };

export type SeBasicInfoDecisionRequest =
  | { ok: true; decision: SeBasicInfoDecision }
  | { ok: false; error: string };

export const MAX_NOTE_LENGTH = 500;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function refuse(error: string): SeBasicInfoDecisionRequest {
  return { ok: false, error };
}

function noteOrRefuse(form: FormData): { ok: true; note: string } | { ok: false; error: string } {
  const note = text(form, "note").trim();
  if (note.length > MAX_NOTE_LENGTH) return { ok: false, error: `Note is longer than ${MAX_NOTE_LENGTH} characters.` };
  return { ok: true, note };
}

/**
 * Parses one form post into a decision. `options.today` (default: the current
 * UTC date) is the upper bound `edit` checks `incorporation_date` against;
 * tests pass a fixed value for determinism.
 */
export function parseSeBasicInfoDecision(
  form: FormData,
  options?: { today?: string },
): SeBasicInfoDecisionRequest {
  const today = options?.today ?? new Date().toISOString().slice(0, 10);
  const intent = text(form, "intent");

  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };

  if (intent === "discard") {
    const field = text(form, "field");
    if (!isBasicInfoField(field)) return refuse("Unknown field.");
    return { ok: true, decision: { intent: "discard", field } };
  }

  if (intent === "activate") {
    const field = text(form, "field");
    if (!isBasicInfoField(field)) return refuse("Unknown field.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent: "activate", field, note: note.note } };
  }

  if (intent === "edit") {
    const field = text(form, "field");
    if (!isBasicInfoField(field)) return refuse("Unknown field.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    const value = text(form, "value");
    const language = text(form, "language");
    const legalFormCodes =
      field === "legal_form_code"
        ? text(form, "legal_form_codes")
            .split(",")
            .map((code) => code.trim())
        : [];
    const result = validateSeBasicInfoValue(field, value, language, { legalFormCodes, today });
    if (!result.ok) return refuse(result.error);
    return {
      ok: true,
      decision: { intent: "edit", field, value: result.value, language: result.language, note: note.note },
    };
  }

  if (intent !== "use-this" && intent !== "reset") return refuse("Unknown intent.");
  const field = text(form, "field");
  if (!isBasicInfoField(field)) return refuse("Unknown field.");
  const note = noteOrRefuse(form);
  if (!note.ok) return refuse(note.error);
  if (intent === "reset") return { ok: true, decision: { intent, field, note: note.note } };
  const source = text(form, "source");
  if (!isBasicInfoSource(source)) return refuse("Unknown source.");
  if (source === "reviewer" || source === "reviewer_draft") {
    return refuse("Use this needs a source other than the reviewer.");
  }
  return { ok: true, decision: { intent, field, source, note: note.note } };
}
