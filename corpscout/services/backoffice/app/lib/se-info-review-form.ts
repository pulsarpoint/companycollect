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
import {
  liveOverrideCorrectionId,
  ZERO_EVIDENCE_HASH,
  type SeInfoCorrectionInput,
} from "~/lib/se-info-corrections";

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function optionalText(form: FormData, name: string): string | null {
  return form.has(name) ? text(form, name) : null;
}

/**
 * Collects only the payload keys the validator allows for this kind.
 *
 * `override_field` diffs against the description the reviewer was shown,
 * because a correction is replayed on every Dagster run: sending an untouched
 * description would pin it forever, and sending an empty description would
 * pin it to null.
 */
export function payloadFor(
  form: FormData,
  kind: string,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    const description = text(form, "description").trim();
    if (text(form, "clear_description") === "yes") {
      payload.description = null;
    } else if (
      description !== "" &&
      description !== text(form, "original_description").trim()
    ) {
      payload.description = description;
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
  if (kind === "override_field" && Object.keys(payload).length === 0) {
    // An emptied textarea with the clear checkbox left unticked looks
    // identical to "nothing changed" once trimmed -- point the reviewer at
    // the checkbox instead of the generic message so they know why the
    // empty text didn't take.
    const description = text(form, "description").trim();
    const original = text(form, "original_description").trim();
    if (
      description === "" &&
      original !== "" &&
      text(form, "clear_description") !== "yes"
    ) {
      return { ok: false, error: "To clear the description, tick the box." };
    }
    return { ok: false, error: "Nothing changed." };
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
 * (see se-info-corrections.ts's liveOverrideCorrectionId doc), so offering
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
