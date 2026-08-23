import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import {
  appendSeCompanyInfoCorrection,
  loadSeCompanyInfoDetail,
} from "~/lib/se-company-info.server";
import { SeInfoCorrectionValidationError } from "~/lib/se-info-corrections";
import { buildCorrectionInput, liveOverrideRefusal } from "~/lib/se-info-review-form";

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

export async function action({ request, params }: Route.ActionArgs) {
  const built = buildCorrectionInput(await request.formData(), {
    companyId: params.companyId,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  // Approve/reject are refused while a live override exists: Dagster's
  // kind-ranking always lets that override win, so the write would land in
  // the ledger but never change what's published.
  if (
    built.input.kind === "approve_suggestion" ||
    built.input.kind === "reject_suggestion"
  ) {
    const current = await loadSeCompanyInfoDetail(params.companyId);
    const refusal = liveOverrideRefusal(
      built.input.kind,
      current?.corrections ?? [],
    );
    if (refusal) return { ok: false as const, error: refusal };
  }
  try {
    const result = await appendSeCompanyInfoCorrection(built.input);
    return { ok: true as const, correctionId: result.correctionId };
  } catch (error) {
    if (error instanceof SeInfoCorrectionValidationError) {
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
