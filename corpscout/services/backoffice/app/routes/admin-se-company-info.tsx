import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import {
  appendSeCompanyInfoFieldValues,
  loadSeCompanyInfoDetail,
} from "~/lib/se-company-info.server";
import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";
import { buildFieldValueInputs } from "~/lib/se-info-field-value-form";

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadSeCompanyInfoDetail(params.companyId);
  // A company Dagster has not published yet is a normal state of the
  // pipeline, not a broken link: the page says so while the response still
  // carries 404.
  return data({ detail }, detail ? undefined : { status: 404 });
}

/**
 * One field-value decision, whatever intent the reviewer used to make it.
 *
 * The current detail is loaded first because a decision can name text the post
 * itself does not carry: `use-suggestion` sends only a suggestion id, and the
 * wording behind it must come from this company's own suggestions rather than
 * from the form. The same load also answers the 404 the loader answers -- a
 * post to a company Dagster has never published has nothing to decide.
 */
export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const detail = await loadSeCompanyInfoDetail(params.companyId);
  if (!detail) {
    throw data({ detail: null }, { status: 404 });
  }
  const built = buildFieldValueInputs(form, {
    companyId: params.companyId,
    suggestions: detail.suggestions,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  try {
    const { valueIds } = await appendSeCompanyInfoFieldValues(built.inputs);
    return { ok: true as const, valueIds };
  } catch (error) {
    // The store's refusals are the reviewer's to read (a company that is not
    // published, an empty value, a field decided twice in one post); anything
    // else is a real failure and must not be dressed up as a form error.
    if (error instanceof SeInfoFieldValueValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.detail?.info.legal_name ?? "Company"} info review | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenCompanyInfo({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.detail) {
    return <SeCompanyInfoNotPublished companyId={params.companyId} />;
  }
  return (
    <SeCompanyInfoReviewWorkspace
      detail={loaderData.detail}
      result={actionData ?? null}
    />
  );
}
