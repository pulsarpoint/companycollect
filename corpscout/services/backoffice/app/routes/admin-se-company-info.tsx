import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import { loadSeCompanyInfoDetail } from "~/lib/se-company-info.server";

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

// TODO(field-values Task 6): Task 6 replaces this action -- it dispatches on `intent`
// (use-source / use-suggestion / edit / release), builds the rows with
// se-info-field-value-form.ts and writes them through
// appendSeCompanyInfoFieldValues. Until then every post is refused -- the
// correction ledger it used to write to no longer exists, and a silent
// success would be worse than a refusal.
export async function action(_: Route.ActionArgs) {
  return {
    ok: false as const,
    error: "Info corrections are being replaced by field values.",
  };
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
