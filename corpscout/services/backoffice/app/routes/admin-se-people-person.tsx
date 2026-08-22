import { data } from "react-router";
import type { Route } from "./+types/admin-se-people-person";
import { SePersonReviewWorkspace } from "~/components/admin/se-person-review-workspace";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";
import {
  appendSeCompanyPersonCorrection,
  getSeCompanyPerson,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-company-person.server";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

function activeRoleCodesFrom(
  roleTypes: Awaited<ReturnType<typeof getCompanyPersonRoleTypes>>,
): string[] {
  return roleTypes
    .filter((roleType) => roleType.is_active === 1)
    .map((roleType) => roleType.role_code);
}

export async function loader({ params }: Route.LoaderArgs) {
  const [detail, roleTypes] = await Promise.all([
    getSeCompanyPerson(params.companyId, params.personId),
    getCompanyPersonRoleTypes(),
  ]);
  if (!detail) {
    throw data("Person not found", { status: 404 });
  }
  return { detail, activeRoleCodes: activeRoleCodesFrom(roleTypes) };
}

function text(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function optionalText(form: FormData, name: string): string | null {
  return form.has(name) ? text(form, name) : null;
}

/**
 * Only the keys the validator allows for this kind are collected; anything else
 * a form happens to carry is dropped here rather than rejected downstream.
 */
function payloadFor(form: FormData, kind: string): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (kind === "override_field") {
    if (text(form, "name").trim() !== "") payload.name = text(form, "name");
    if (form.has("description")) {
      payload.description = text(form, "description") || null;
    }
  } else if (kind === "split_person") {
    payload.name = text(form, "name");
  } else if (kind === "approve_suggestion" || kind === "reject_suggestion") {
    payload.suggestion_id = text(form, "suggestion_id");
    const note = kind === "reject_suggestion" ? optionalText(form, "note") : null;
    if (note) payload.note = note;
  } else if (kind === "set_role") {
    payload.role_code = text(form, "role_code");
    const year = text(form, "fiscal_year").trim();
    if (year !== "") payload.fiscal_year = Number(year);
  }
  return payload;
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const kind = text(form, "correction_kind");
  const roleTypes = await getCompanyPersonRoleTypes();
  try {
    const result = await appendSeCompanyPersonCorrection({
      companyId: params.companyId,
      kind,
      subjectPersonId: params.personId,
      targetPersonId: optionalText(form, "target_person_id"),
      draftIds: form.getAll("draft_id").map(String),
      payload: payloadFor(form, kind),
      // Undo supersedes a decision rather than the evidence, so it carries the
      // zero hash instead of the draft-set hash the reviewer saw.
      evidenceHash:
        kind === "undo" ? ZERO_EVIDENCE_HASH : text(form, "evidence_hash"),
      reason: text(form, "reason"),
      supersedesCorrectionId:
        kind === "undo" ? optionalText(form, "supersedes_correction_id") : null,
      activeRoleCodes: new Set(activeRoleCodesFrom(roleTypes)),
    });
    return { ok: true as const, correctionId: result.correctionId };
  } catch (error) {
    if (error instanceof SePersonCorrectionValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.detail.person.name ?? "Person"} review | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenPersonReview({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <SePersonReviewWorkspace
      detail={loaderData.detail}
      activeRoleCodes={loaderData.activeRoleCodes}
      result={actionData ?? null}
    />
  );
}
