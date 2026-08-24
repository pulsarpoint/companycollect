/**
 * Turns the Address tab's form posts into validated ledger rows.
 *
 * Client-safe on purpose: React Router only strips `loader`/`action`/
 * `middleware`/`headers` from a route module, so any other export of the route
 * that reaches into a `.server` module drags that module into the client bundle
 * and fails the production build. These helpers live here instead, and both the
 * route's `action` and the tab's own button-disabling import them. Mirrors
 * se-info-review-form.ts for this ledger's kinds (see se-address-corrections.ts),
 * with one difference the datatype forces: every non-undo payload names the
 * `address_key` it decides, because a company has several published addresses.
 */
import {
  liveOverrideCorrectionId,
  OVERRIDABLE_FIELDS,
  SE_ADDRESS_CORRECTION_KINDS,
  ZERO_EVIDENCE_HASH,
  type SeAddressCorrectionInput,
  type SeAddressOverridableField,
} from "~/lib/se-address-corrections";

/** How each overridable column is named to a reviewer -- on its input, and in
 * the refusal that points at that input's clear box. One list, so the label in
 * the message is always the label above the field it means. */
export const ADDRESS_FIELD_LABELS: Record<SeAddressOverridableField, string> = {
  care_of: "Care of",
  street_address: "Street",
  normalized_address: "Normalized address",
  postal_code: "Postal code",
  city: "City",
  country_code: "Country",
};

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function optionalText(form: FormData, name: string): string | null {
  return form.has(name) ? text(form, name) : null;
}

/** One override input's state, diffed against the text the reviewer was shown. */
interface OverrideField {
  /** The trimmed input contents. */
  value: string;
  /** The trimmed text this field was rendered with. */
  original: string;
  /** Its "clear this" checkbox. */
  cleared: boolean;
  /** Cleared, or edited to something non-empty and different. */
  changed: boolean;
}

function overrideField(form: FormData, name: string): OverrideField {
  const value = text(form, name).trim();
  const original = text(form, `original_${name}`).trim();
  const cleared = text(form, `clear_${name}`) === "yes";
  return { value, original, cleared, changed: cleared || (value !== "" && value !== original) };
}

/**
 * Collects only the payload keys the validator allows for this kind.
 *
 * `override_field` diffs each field against the text the reviewer was shown,
 * because a correction is replayed on every Dagster run: sending an untouched
 * field would pin the computed value for ever, and an ABSENT key is exactly how
 * the pipeline is told to leave that field as computed. A cleared field travels
 * as an explicit null, which is the decision "this address has none".
 *
 * `reject_address` decides nothing but the key -- address_rules.py skips a
 * reject that carries anything else -- and `undo` names a correction rather than
 * a row, so it carries no payload at all.
 */
export function payloadFor(form: FormData, kind: string): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    payload.address_key = text(form, "address_key");
    for (const field of OVERRIDABLE_FIELDS) {
      const state = overrideField(form, field);
      if (!state.changed) continue;
      payload[field] = state.cleared ? null : state.value;
    }
  } else if (kind === "reject_address") {
    payload.address_key = text(form, "address_key");
  }
  return payload;
}

export type SeAddressCorrectionRequest =
  | { ok: true; input: SeAddressCorrectionInput }
  | { ok: false; error: string };

export function buildCorrectionInput(
  form: FormData,
  context: { companyId: string },
): SeAddressCorrectionRequest {
  const kind = text(form, "correction_kind");
  // Refused here as well as in the validator: an unknown kind reaches Dagster's
  // effective_ledger, which ranks by ADDRESS_KIND_ORDER and drops what that list
  // does not name -- a reviewer's decision vanishing in silence.
  if (!(SE_ADDRESS_CORRECTION_KINDS as readonly string[]).includes(kind)) {
    return { ok: false, error: "Unknown correction kind." };
  }
  const payload = payloadFor(form, kind);
  if (kind === "override_field") {
    // An emptied input with its clear box unticked trims to "" and is
    // indistinguishable from "untouched", so it would be dropped in silence.
    // Checked BEFORE the empty-payload test, because a change to another field
    // would otherwise carry the emptied one past this refusal.
    const emptied = OVERRIDABLE_FIELDS.find((field) => {
      const state = overrideField(form, field);
      return state.value === "" && state.original !== "" && !state.cleared;
    });
    if (emptied !== undefined) {
      return { ok: false, error: `To clear ${ADDRESS_FIELD_LABELS[emptied]}, tick its box.` };
    }
    // The key is always in the payload, so an override that moved nothing is a
    // payload of exactly that one entry.
    if (Object.keys(payload).length <= 1) {
      return { ok: false, error: "Nothing changed." };
    }
  }
  return {
    ok: true,
    input: {
      companyId: context.companyId,
      kind,
      payload,
      // Undo supersedes a decision rather than evidence, so it carries the zero
      // hash whatever the form posted -- the validator refuses any other value
      // on that kind.
      evidenceHash: kind === "undo" ? ZERO_EVIDENCE_HASH : text(form, "evidence_hash"),
      reason: text(form, "reason"),
      supersedesCorrectionId:
        kind === "undo" ? optionalText(form, "supersedes_correction_id") : null,
    },
  };
}

/**
 * The message to show when a second `override_field` is attempted on a row that
 * already carries a live one, and null when the decision is fine.
 *
 * Only that case: a reject and an override answer different questions and
 * ADDRESS_KIND_ORDER lets both stand, and an undo is the way OUT of a live
 * override. A second override is different -- the later one wins by created_at
 * and buries the first, so the ledger would carry a decision nobody can see.
 *
 * Pure, so the route action and the tab's own disabling share one answer and
 * the message is unit-testable without a live ClickHouse.
 */
export function liveOverrideRefusal(
  kind: string,
  addressKey: string,
  corrections: Parameters<typeof liveOverrideCorrectionId>[0],
): string | null {
  if (kind !== "override_field") return null;
  return liveOverrideCorrectionId(corrections, addressKey) === null
    ? null
    : "This address already has a live override — undo it before overriding again.";
}
