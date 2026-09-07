/**
 * Turns the Address tab's form posts into one decision (spec 8). Client-safe.
 * Six intents: `remove` and `reset` act on a published key, `save-draft` writes
 * or edits a reviewer draft (with `replaces_key` for a Correct), `activate` and
 * `discard` act on a draft slot, `fold-now` launches the targeted fold.
 */
import { MAX_NOTE_LENGTH, isAddressKey, validateSeAddressInput, type SeAddressInput } from "~/lib/se-address-fields";

export type SeAddressDecision =
  | { intent: "remove"; addressKey: string; note: string }
  | { intent: "reset"; addressKey: string; note: string }
  | { intent: "fold-now" }
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SeAddressInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string };
export type SeAddressDecisionRequest = { ok: true; decision: SeAddressDecision } | { ok: false; error: string };

const SLOT_PATTERN = /^r[0-9]{17}$/;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}
function refuse(error: string): SeAddressDecisionRequest {
  return { ok: false, error };
}
function noteOrRefuse(form: FormData): { ok: true; note: string } | { ok: false; error: string } {
  const note = text(form, "note").trim();
  if (note.length > MAX_NOTE_LENGTH) return { ok: false, error: `Note is longer than ${MAX_NOTE_LENGTH} characters.` };
  return { ok: true, note };
}

export function parseSeAddressDecision(form: FormData): SeAddressDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent === "remove" || intent === "reset") {
    const addressKey = text(form, "address_key");
    if (!isAddressKey(addressKey)) return refuse("Unknown address.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, addressKey, note: note.note } };
  }
  if (intent === "activate" || intent === "discard") {
    const slot = text(form, "slot");
    if (!SLOT_PATTERN.test(slot)) return refuse("Unknown draft.");
    if (intent === "discard") return { ok: true, decision: { intent, slot } };
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slot, note: note.note } };
  }
  if (intent === "save-draft") {
    const validated = validateSeAddressInput({
      careOf: text(form, "care_of"), streetLine: text(form, "street_line"), postalCode: text(form, "postal_code"),
      city: text(form, "city"), country: text(form, "country"), kind: text(form, "kind"), note: text(form, "note"),
    });
    if (!validated.ok) return refuse(validated.error);
    const slotText = text(form, "slot");
    if (slotText !== "" && !SLOT_PATTERN.test(slotText)) return refuse("Unknown draft.");
    const replacesText = text(form, "replaces_key");
    if (replacesText !== "" && !isAddressKey(replacesText)) return refuse("Unknown address.");
    return {
      ok: true,
      decision: { intent, slot: slotText === "" ? null : slotText, replacesKey: replacesText === "" ? null : replacesText, input: validated.input },
    };
  }
  return refuse("Unknown intent.");
}
