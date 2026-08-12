import type { Route } from "./+types/company-domain-suggestions";
import { CompanyDomainSuggestionsSection } from "~/components/detail/company-domain-suggestions-section";
import {
  COMPANY_DOMAIN_REVIEW_STATUSES,
  CompanyDomainReviewValidationError,
  getUnifiedCompanyDomains,
  recordCompanyDomainReview,
  type CompanyDomainReviewStatus,
} from "~/lib/company-domains.server";
import { domainSuggestionsTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !domainSuggestionsTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }
  return {
    domains: await getUnifiedCompanyDomains(country.code, params.id),
  };
}

export async function action({ params, request }: Route.ActionArgs) {
  const country = getCountry(params.country);
  if (!country || !domainSuggestionsTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }
  const form = await request.formData();
  const reviewStatus = String(form.get("review_status") ?? "");
  if (
    !COMPANY_DOMAIN_REVIEW_STATUSES.includes(
      reviewStatus as CompanyDomainReviewStatus,
    )
  ) {
    return { ok: false as const, error: "Unknown review decision." };
  }

  try {
    const domains = await getUnifiedCompanyDomains(country.code, params.id);
    await recordCompanyDomainReview({
      domains,
      rootDomain: String(form.get("root_domain") ?? ""),
      reviewStatus: reviewStatus as CompanyDomainReviewStatus,
      reviewedBy: process.env.COMPANY_DOMAIN_REVIEWER?.trim() ?? "",
    });
    return { ok: true as const };
  } catch (error) {
    if (error instanceof CompanyDomainReviewValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Domains · ${params.id} – CompanyCollect Backoffice` }];
}

export default function CompanyDomainSuggestions({
  loaderData,
  params,
}: Route.ComponentProps) {
  const basePath = `/company/${params.country}/${params.id}`;
  return (
    <CompanyDomainSuggestionsSection
      domains={loaderData.domains}
      reviewAction={`${basePath}/suggestions`}
      technologyPath={`${basePath}/technology`}
    />
  );
}
