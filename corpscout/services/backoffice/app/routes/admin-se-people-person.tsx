import { data } from "react-router";
import type { Route } from "./+types/admin-se-people-person";
import {
  SePersonNotPublished,
  SePersonReviewWorkspace,
} from "~/components/admin/se-person-review-workspace";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";
import {
  appendSeCompanyPersonCorrection,
  getSeCompanyPerson,
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
  const [detail, roleTypes] = await Promise.all([
    getSeCompanyPerson(params.companyId, params.personId),
    getCompanyPersonRoleTypes(),
  ]);
  // A person Dagster has not published yet is a normal state of the pipeline,
  // not a broken link: the page says so while the response still carries 404.
  return data(
    { detail, activeRoleCodes: activeRoleCodesFrom(roleTypes) },
    detail ? undefined : { status: 404 },
  );
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
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
      result={actionData ?? null}
    />
  );
}
