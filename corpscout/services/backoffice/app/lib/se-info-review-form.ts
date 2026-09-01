/**
 * Turns the company-info review page's form posts into validated ledger rows.
 *
 * Client-safe on purpose: React Router only strips `loader`/`action`/
 * `middleware`/`headers` from a route module, so any other export of the route
 * that reaches into a `.server` module drags that module into the client bundle
 * and fails the production build. These helpers live here instead, and the
 * route's `action` imports them. Mirrors se-person-review-form.ts's shape for
 * the smaller company-info kind set (see se-info-corrections.ts).
 */
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

// TODO(field-values Task 6/7): this whole module is the correction-ledger form
// builder, replaced by se-info-field-value-form.ts in Task 6. The company-info
// ledger's validator is already gone (se-info-corrections.ts kept only the
// shared filter vocabulary), so the two things it used from there -- the input
// shape and liveOverrideCorrectionId -- live here verbatim until Task 6 deletes
// the module. ZERO_EVIDENCE_HASH now comes from its real owner.
type SeInfoCorrectionInput = {
  companyId: string;
  kind: string;
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
};

/**
 * The live (current, non-stale, un-superseded) `override_field` correction, or
 * null. Sorts the rows itself by `created_at DESC, correction_id DESC` rather
 * than trusting the caller's order.
 */
function liveOverrideCorrectionId(
  corrections: ReadonlyArray<{
    correction_id: string;
    correction_kind: string;
    supersedes_correction_id: string | null;
    is_current: number;
    is_stale: number;
    created_at: string;
  }>,
): string | null {
  const supersededIds = new Set(
    corrections
      .filter((row) => row.correction_kind === "undo" && row.supersedes_correction_id)
      .map((row) => row.supersedes_correction_id as string),
  );
  const liveOverrides = corrections.filter(
    (row) =>
      row.correction_kind === "override_field" &&
      row.is_current === 1 &&
      row.is_stale === 0 &&
      !supersededIds.has(row.correction_id),
  );
  if (liveOverrides.length === 0) return null;
  liveOverrides.sort((a, b) => {
    if (a.created_at !== b.created_at) return a.created_at > b.created_at ? -1 : 1;
    return a.correction_id > b.correction_id ? -1 : 1;
  });
  return liveOverrides[0].correction_id;
}

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function optionalText(form: FormData, name: string): string | null {
  return form.has(name) ? text(form, name) : null;
}

/** One override textarea's state, diffed against the text the reviewer was shown. */
interface OverrideField {
  /** The trimmed textarea contents. */
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
 * `override_field` diffs each language against the text the reviewer was shown,
 * because a correction is replayed on every Dagster run: sending an untouched
 * description would pin it forever, and sending an empty description would
 * pin it to null.
 *
 * `description` is required by the ledger even when only the Swedish text moved, so it
 * rides along unchanged in that case -- an override decides the whole published pair,
 * not one column of it. `description_sv` is the other way round: it is sent only when it
 * changed, because Dagster reads an ABSENT key as "leave the Swedish text as computed"
 * (deterministic or model-written) and a present null as "there is none".
 */
export function payloadFor(
  form: FormData,
  kind: string,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    const english = overrideField(form, "description");
    const swedish = overrideField(form, "description_sv");
    if (english.changed || swedish.changed) {
      payload.description = english.cleared || english.value === "" ? null : english.value;
      if (swedish.changed) {
        payload.description_sv = swedish.cleared ? null : swedish.value;
      }
    }
  } else if (kind === "approve_suggestion" || kind === "reject_suggestion") {
    payload.suggestion_id = text(form, "suggestion_id");
    const note = kind === "reject_suggestion" ? text(form, "note").trim() : "";
    if (note !== "") payload.note = note;
  }
  return payload;
}

export type SeInfoCorrectionRequest =
  | { ok: true; input: SeInfoCorrectionInput }
  | { ok: false; error: string };

export function buildCorrectionInput(
  form: FormData,
  params: { companyId: string },
): SeInfoCorrectionRequest {
  const kind = text(form, "correction_kind");
  const payload = payloadFor(form, kind);
  if (kind === "override_field") {
    // An emptied textarea with its clear checkbox left unticked looks identical to
    // "nothing changed" once trimmed -- point the reviewer at the checkbox instead of
    // the generic message so they know why the empty text didn't take. Checked before
    // the empty-payload test, because a change to the OTHER language would otherwise
    // carry the emptied one silently past this refusal.
    const emptied = (["description", "description_sv"] as const)
      .map((name) => overrideField(form, name))
      .findIndex((field) => field.value === "" && field.original !== "" && !field.cleared);
    if (emptied === 0) {
      return { ok: false, error: "To clear the description, tick the box." };
    }
    if (emptied === 1) {
      return { ok: false, error: "To clear the Swedish description, tick its box." };
    }
    if (Object.keys(payload).length === 0) {
      return { ok: false, error: "Nothing changed." };
    }
  }
  return {
    ok: true,
    input: {
      companyId: params.companyId,
      kind,
      payload,
      // Undo supersedes a decision rather than evidence, so it carries the
      // zero hash instead of the evidence-set hash the reviewer saw.
      evidenceHash:
        kind === "undo" ? ZERO_EVIDENCE_HASH : text(form, "evidence_hash"),
      reason: text(form, "reason"),
      supersedesCorrectionId:
        kind === "undo" ? optionalText(form, "supersedes_correction_id") : null,
    },
  };
}

/**
 * Dagster's kind-ranking always lets a live, current, non-stale
 * `override_field` win over any approve/reject regardless of decision order
 * (see liveOverrideCorrectionId above), so offering
 * approve/reject while one stands is misleading: the write would land in the
 * ledger but never change what's published. Both the route action and the
 * workspace's button-disabling share this one check, and it stays pure so
 * the refusal message is unit-testable without a live ClickHouse.
 *
 * The message names the override by its 8-char prefix -- the same form the
 * Ledger card shows on every row -- so the reviewer can actually find it.
 */
export function liveOverrideRefusal(
  kind: string,
  corrections: Parameters<typeof liveOverrideCorrectionId>[0],
): string | null {
  if (kind !== "approve_suggestion" && kind !== "reject_suggestion") {
    return null;
  }
  const liveId = liveOverrideCorrectionId(corrections);
  if (!liveId) return null;
  return `Undo the current override first (${liveId.slice(0, 8)}).`;
}
