import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-info";
import {
  SeBasicInfoNotFolded,
  SeBasicInfoWorkspace,
} from "~/components/admin/se-basic-info-workspace";
import {
  appendSeBasicInfoReviewerDecision,
  launchSeBasicInfoFold,
  loadSeBasicInfoDetail,
  SeBasicInfoDecisionError,
} from "~/lib/se-basic-info.server";
import { parseSeBasicInfoDecision } from "~/lib/se-basic-info-decision-form";
import { selectedFieldFromSearch } from "~/lib/se-basic-info-fields";

/** Swedish org numbers are 10 digits, or 12 with the century prefix. */
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  const detail = await loadSeBasicInfoDetail(params.companyId);
  const selectedField = selectedFieldFromSearch(new URL(request.url).searchParams);
  // A company no extractor has suggested and no fold has published is a
  // normal pipeline state, not a broken link: the page says so under a 404.
  return data({ detail, selectedField }, detail ? undefined : { status: 404 });
}

/**
 * One reviewer decision (a new reviewer-row version) or one Fold now launch.
 * The store's refusals are the reviewer's to read; anything else is a real
 * failure and must not be dressed up as a form error.
 */
export async function action({ request, params }: Route.ActionArgs) {
  if (!COMPANY_ID_PATTERN.test(params.companyId)) {
    return { ok: false as const, error: "Company id must be 10 or 12 digits." };
  }
  const parsed = parseSeBasicInfoDecision(await request.formData());
  if (!parsed.ok) return { ok: false as const, error: parsed.error };
  if (parsed.decision.intent === "fold-now") {
    const launched = await launchSeBasicInfoFold(params.companyId);
    return { ok: true as const, launched };
  }
  try {
    const { suggestedAt } = await appendSeBasicInfoReviewerDecision(params.companyId, parsed.decision);
    return { ok: true as const, suggestedAt };
  } catch (error) {
    if (error instanceof SeBasicInfoDecisionError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.detail?.info?.legal_name ?? params.companyId} basic info | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenCompanyInfo({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.detail) {
    return <SeBasicInfoNotFolded companyId={params.companyId} />;
  }
  return (
    <SeBasicInfoWorkspace
      companyId={params.companyId}
      detail={loaderData.detail}
      selectedField={loaderData.selectedField}
      result={actionData ?? null}
    />
  );
}
