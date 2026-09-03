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
import {
  loadFieldRegistry,
  type FieldRegistry,
} from "~/lib/se-company-field-registry.server";
import {
  fieldVocabulary,
  SeInfoFieldValueValidationError,
} from "~/lib/se-info-field-values";
import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";
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
 *
 * The registry is a NEW dependency of deciding anything (before this branch a
 * decision needed none), so its failure is reported as a form error rather
 * than thrown: the message names the fix ("materialize
 * se_company_field_registry_clickhouse first"), and the error boundary's 500
 * page would hide exactly that -- during the cutover window, when it is the
 * one thing a reviewer needs to read.
 */
export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const detail = await loadSeCompanyInfoDetail(params.companyId);
  if (!detail) {
    throw data({ detail: null }, { status: 404 });
  }
  let registry: FieldRegistry;
  try {
    registry = await loadFieldRegistry();
  } catch (error) {
    return {
      ok: false as const,
      error: error instanceof Error ? error.message : String(error),
    };
  }
  const built = buildFieldValueInputs(form, {
    companyId: params.companyId,
    suggestions: detail.suggestions,
    fields: fieldVocabulary(registry).fields,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  try {
    const { valueIds, resolved, skipped } =
      await appendSeCompanyInfoFieldValues(built.inputs, { registry });
    return { ok: true as const, valueIds, resolved, skipped };
  } catch (error) {
    // The store's refusals are the reviewer's to read (a company that is not
    // published, an empty value, a field decided twice in one post); anything
    // else is a real failure and must not be dressed up as a form error.
    if (error instanceof SeInfoFieldValueValidationError) {
      return { ok: false as const, error: error.message };
    }
    // The decision IS in the store; only the synchronous resolve failed. The
    // ids mark it as saved so the page does not call it "Not saved".
    if (error instanceof SeCompanyFieldResolveError) {
      return {
        ok: false as const,
        error: error.message,
        valueIds: error.valueIds,
      };
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
