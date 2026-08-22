/**
 * Turns the review page's form posts into validated ledger rows.
 *
 * Client-safe on purpose: React Router only strips `loader`/`action`/
 * `middleware`/`headers` from a route module, so any other export of the route
 * that reaches into a `.server` module drags that module into the client bundle
 * and fails the production build. These helpers live here instead, and the
 * route's `action` imports them.
 */
import {
  ZERO_EVIDENCE_HASH,
  type SePersonCorrectionInput,
} from "~/lib/se-person-corrections";
import type { CompanyPersonRoleType } from "~/lib/company-roles.server";

export function activeRoleCodesFrom(
  roleTypes: CompanyPersonRoleType[],
): string[] {
  return roleTypes
    .filter((roleType) => roleType.is_active === 1)
    .map((roleType) => roleType.role_code);
}

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
 * `override_field` diffs against the values the reviewer was shown, because a
 * correction is replayed on every Dagster run: sending an untouched field would
 * pin it forever, and sending an empty description would pin it to null.
 */
export function payloadFor(
  form: FormData,
  kind: string,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    const name = text(form, "name").trim();
    if (name !== text(form, "original_name").trim()) {
      payload.name = name;
    }
    const description = text(form, "description").trim();
    if (text(form, "clear_description") === "yes") {
      payload.description = null;
    } else if (
      description !== "" &&
      description !== text(form, "original_description").trim()
    ) {
      payload.description = description;
    }
  } else if (kind === "split_person") {
    payload.name = text(form, "name");
  } else if (kind === "approve_suggestion" || kind === "reject_suggestion") {
    payload.suggestion_id = text(form, "suggestion_id");
    const note = kind === "reject_suggestion" ? text(form, "note").trim() : "";
    if (note !== "") payload.note = note;
  } else if (kind === "set_role") {
    payload.role_code = text(form, "role_code");
    const year = text(form, "fiscal_year").trim();
    if (year !== "") payload.fiscal_year = Number(year);
  }
  return payload;
}

export type SePersonCorrectionRequest =
  | { ok: true; input: SePersonCorrectionInput }
  | { ok: false; error: string };

export function buildCorrectionInput(
  form: FormData,
  params: { companyId: string; personId: string },
  roleTypes: CompanyPersonRoleType[],
): SePersonCorrectionRequest {
  const kind = text(form, "correction_kind");
  const payload = payloadFor(form, kind);
  if (kind === "override_field" && Object.keys(payload).length === 0) {
    return { ok: false, error: "Nothing changed." };
  }
  return {
    ok: true,
    input: {
      companyId: params.companyId,
      kind,
      subjectPersonId: params.personId,
      targetPersonId: optionalText(form, "target_person_id"),
      draftIds: form.getAll("draft_id").map(String),
      payload,
      // Undo supersedes a decision rather than the evidence, so it carries the
      // zero hash instead of the draft-set hash the reviewer saw.
      evidenceHash:
        kind === "undo" ? ZERO_EVIDENCE_HASH : text(form, "evidence_hash"),
      reason: text(form, "reason"),
      supersedesCorrectionId:
        kind === "undo" ? optionalText(form, "supersedes_correction_id") : null,
      activeRoleCodes: new Set(activeRoleCodesFrom(roleTypes)),
    },
  };
}
