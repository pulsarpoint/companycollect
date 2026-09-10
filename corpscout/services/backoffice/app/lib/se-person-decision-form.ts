/**
 * Turns the People tab's form posts into one decision (spec 2026-09-09 section 7).
 * Client-safe. Eight intents: `save-draft` writes or edits a reviewer draft (with
 * `replaces_key` for a Correct), `activate` and `discard` act on a draft's group slot,
 * `remove` and `reset` on a published key, `merge` on two or more keys, `split` on the
 * checked member slots, `fold-now` launches the targeted fold.
 *
 * `roleCodes` comes from the catalog the loader read (Ruling 3): a client-safe module
 * may not read ClickHouse, and a role code is only valid if the catalog has it.
 */
import {
  PERSON_GROUP_SLOT_PATTERN,
  isPersonKey,
  noteError,
  validateSePersonInput,
  type SePersonInput,
  type SePersonRoleInput,
} from "~/lib/se-person-fields";

export type SePersonDecision =
  | { intent: "save-draft"; slot: string | null; replacesKey: string | null; input: SePersonInput }
  | { intent: "activate"; slot: string; note: string }
  | { intent: "discard"; slot: string }
  | { intent: "remove"; personKey: string; note: string }
  | { intent: "merge"; personKeys: string[]; note: string }
  | { intent: "split"; slots: string[]; note: string }
  | { intent: "reset"; personKey: string; note: string }
  | { intent: "fold-now" };
export type SePersonDecisionRequest =
  | { ok: true; decision: SePersonDecision }
  | { ok: false; error: string };

/** A member slot as its source minted it (`uid:uid`, `Q1:P169:Q2`, `r...01`). */
const MAX_SLOT_LENGTH = 200;
const CONTROL = /[\u0000-\u001f\u007f]/;

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}
function all(form: FormData, name: string): string[] {
  return form.getAll(name).filter((value): value is string => typeof value === "string");
}
function refuse(error: string): SePersonDecisionRequest {
  return { ok: false, error };
}
function noteOrRefuse(form: FormData): { ok: true; note: string } | { ok: false; error: string } {
  const note = text(form, "note").trim();
  // Length AND plainness: a note posted from a dialog reaches `rule.note` or a row's
  // `data.note` without passing through `validateSePersonInput`, and the Global
  // Constraint holds for both paths.
  const error = noteError(note);
  if (error !== null) return { ok: false, error };
  return { ok: true, note };
}
/** The sheet posts one `role_code`, `role_from` and `role_to` per rendered row, so the
 * three lists are index-parallel; a row with nothing in it is dropped by the validator. */
function roleRows(form: FormData): SePersonRoleInput[] {
  const codes = all(form, "role_code");
  const from = all(form, "role_from");
  const to = all(form, "role_to");
  return codes.map((code, index) => ({
    code,
    fromYear: from[index] ?? "",
    toYear: to[index] ?? "",
  }));
}

export function parseSePersonDecision(
  form: FormData,
  roleCodes: readonly string[],
): SePersonDecisionRequest {
  const intent = text(form, "intent");
  if (intent === "fold-now") return { ok: true, decision: { intent: "fold-now" } };
  if (intent === "remove" || intent === "reset") {
    const personKey = text(form, "person_key");
    if (!isPersonKey(personKey)) return refuse("Unknown person.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, personKey, note: note.note } };
  }
  if (intent === "activate" || intent === "discard") {
    const slot = text(form, "slot");
    if (!PERSON_GROUP_SLOT_PATTERN.test(slot)) return refuse("Unknown draft.");
    if (intent === "discard") return { ok: true, decision: { intent, slot } };
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slot, note: note.note } };
  }
  if (intent === "merge") {
    const personKeys = [...new Set(all(form, "person_key"))].sort();
    if (personKeys.some((key) => !isPersonKey(key))) return refuse("Unknown person.");
    if (personKeys.length < 2) return refuse("Pick at least two persons to merge.");
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, personKeys, note: note.note } };
  }
  if (intent === "split") {
    const slots = [...new Set(all(form, "slot").map((slot) => slot.trim()))].sort();
    if (slots.length === 0) return refuse("Pick at least one observation to split off.");
    if (slots.some((slot) => slot === "" || slot.length > MAX_SLOT_LENGTH || CONTROL.test(slot))) {
      return refuse("Unknown observation.");
    }
    const note = noteOrRefuse(form);
    if (!note.ok) return refuse(note.error);
    return { ok: true, decision: { intent, slots, note: note.note } };
  }
  if (intent === "save-draft") {
    const validated = validateSePersonInput(
      {
        firstName: text(form, "first_name"),
        lastName: text(form, "last_name"),
        birthYear: text(form, "birth_year"),
        wikidataId: text(form, "wikidata_id"),
        roles: roleRows(form),
        data: text(form, "data"),
        note: text(form, "note"),
      },
      roleCodes,
    );
    if (!validated.ok) return refuse(validated.error);
    const slotText = text(form, "slot");
    if (slotText !== "" && !PERSON_GROUP_SLOT_PATTERN.test(slotText)) return refuse("Unknown draft.");
    const replacesText = text(form, "replaces_key");
    if (replacesText !== "" && !isPersonKey(replacesText)) return refuse("Unknown person.");
    return {
      ok: true,
      decision: {
        intent,
        slot: slotText === "" ? null : slotText,
        replacesKey: replacesText === "" ? null : replacesText,
        input: validated.input,
      },
    };
  }
  return refuse("Unknown intent.");
}
