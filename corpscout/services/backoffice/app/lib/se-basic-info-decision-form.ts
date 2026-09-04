/**
 * Turns the Info tab's form posts into one decision. Client-safe (no `.server`
 * import): the route's own module must not drag the server module into the
 * client bundle, and the refusals are unit-testable without ClickHouse.
 */
import {
  isBasicInfoField,
  isBasicInfoSource,
  type SeBasicInfoField,
  type SeBasicInfoSource,
} from "~/lib/se-basic-info-fields";

export type SeBasicInfoDecision =
  | { intent: "use-this"; field: SeBasicInfoField; source: Exclude<SeBasicInfoSource, "reviewer">; note: string }
  | { intent: "release"; field: SeBasicInfoField; note: string }
  | { intent: "fold-now" };

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

export function parseSeBasicInfoDecision(form: FormData): SeBasicInfoDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent !== "use-this" && intent !== "release") return refuse("Unknown intent.");
  const field = text(form, "field");
  if (!isBasicInfoField(field)) return refuse("Unknown field.");
  const note = text(form, "note").trim();
  if (note.length > MAX_NOTE_LENGTH) return refuse(`Note is longer than ${MAX_NOTE_LENGTH} characters.`);
  if (intent === "release") return { ok: true, decision: { intent, field, note } };
  const source = text(form, "source");
  if (!isBasicInfoSource(source)) return refuse("Unknown source.");
  if (source === "reviewer") return refuse("Use this needs a source other than the reviewer.");
  return { ok: true, decision: { intent, field, source, note } };
}
