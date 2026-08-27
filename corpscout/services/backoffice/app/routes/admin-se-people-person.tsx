import { data } from "react-router";
import type { Route } from "./+types/admin-se-people-person";
import {
  SePersonNotPublished,
  SePersonReviewWorkspace,
} from "~/components/admin/se-person-review-workspace";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";
import {
  appendSeCompanyPersonCorrection,
  approveMergeSuggestion,
  getSeCompanyPerson,
  keepSeparateMergeSuggestion,
  loadSeCompanyPersonCollisionReview,
} from "~/lib/se-company-person.server";
import {
  activeRoleCodesFrom,
  buildCorrectionInput,
} from "~/lib/se-person-review-form";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

// Only `loader`, `action`, `meta` and the component live here. Any other export
// that touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build.

export async function loader({ params }: Route.LoaderArgs) {
  const [detail, roleTypes, collisionGroups] = await Promise.all([
    getSeCompanyPerson(params.companyId, params.personId),
    getCompanyPersonRoleTypes(),
    loadSeCompanyPersonCollisionReview(params.companyId),
  ]);
  // A person Dagster has not published yet is a normal state of the pipeline,
  // not a broken link: the page says so while the response still carries 404.
  return data(
    { detail, activeRoleCodes: activeRoleCodesFrom(roleTypes), collisionGroups },
    detail ? undefined : { status: 404 },
  );
}

/** Reason is required for every decision, including the two merge-review
 * kinds below, which do not go through the generic single-row validator. */
function requiredReason(form: FormData): string | null {
  const reason = String(form.get("reason") ?? "").trim();
  return reason === "" ? null : reason;
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const kind = String(form.get("correction_kind") ?? "");

  // Collision/merge review decisions: multi-row writes anchored on a group,
  // re-validated against live se_company_person state before anything is
  // written (see se-company-person.server.ts's revalidateMergeSuggestion) --
  // handled here instead of the generic single-row path below because a
  // merge approval can write several merge_persons corrections at once, one
  // per from_person_id.
  if (kind === "approve_merge_suggestion" || kind === "keep_separate_suggestion") {
    const suggestionId = String(form.get("suggestion_id") ?? "");
    const reason = requiredReason(form);
    if (reason === null) {
      return { ok: false as const, error: "Reason is required." };
    }
    try {
      if (kind === "approve_merge_suggestion") {
        const result = await approveMergeSuggestion({
          companyId: params.companyId,
          suggestionId,
          reason,
        });
        return {
          ok: true as const,
          correctionId: result.correctionIds[0] ?? "",
          correctionIds: result.correctionIds,
        };
      }
      const result = await keepSeparateMergeSuggestion({
        companyId: params.companyId,
        suggestionId,
        reason,
      });
      return { ok: true as const, correctionId: result.correctionId };
    } catch (error) {
      if (error instanceof SePersonCorrectionValidationError) {
        return { ok: false as const, error: error.message };
      }
      throw error;
    }
  }

  const roleTypes = await getCompanyPersonRoleTypes();
  const built = buildCorrectionInput(form, params, roleTypes);
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  try {
    const result = await appendSeCompanyPersonCorrection(built.input);
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
      title: `${loaderData?.detail?.person.name ?? "Person"} review | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenPersonReview({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.detail) {
    return (
      <SePersonNotPublished
        companyId={params.companyId}
        personId={params.personId}
      />
    );
  }
  return (
    <SePersonReviewWorkspace
      detail={loaderData.detail}
      activeRoleCodes={loaderData.activeRoleCodes}
      collisionGroups={loaderData.collisionGroups}
      result={actionData ?? null}
    />
  );
}
