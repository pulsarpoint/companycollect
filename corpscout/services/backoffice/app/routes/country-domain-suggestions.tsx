import type { Route } from "./+types/country-domain-suggestions";
import { CompanyDomainSuggestionReview } from "~/components/domain-suggestions/company-domain-suggestion-review";
import {
  COMPANY_DOMAIN_SOURCES,
  searchCompanyDomainReviewQueue,
  type CompanyDomainSourceFilter,
} from "~/lib/company-domains.server";
import { getCountry } from "~/lib/countries";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || country.code !== "se") {
    throw new Response("Domain review is not available", { status: 404 });
  }
  const searchParams = new URL(request.url).searchParams;
  const requestedSource = searchParams.get("source") ?? "all";
  const source: CompanyDomainSourceFilter = COMPANY_DOMAIN_SOURCES.includes(
    requestedSource as CompanyDomainSourceFilter,
  )
    ? (requestedSource as CompanyDomainSourceFilter)
    : "all";
  const query = (searchParams.get("q") ?? "").trim();
  const result = await searchCompanyDomainReviewQueue(country.code, {
    query,
    source,
    page: Number(searchParams.get("page") ?? "1") || 1,
    pageSize: Number(searchParams.get("pageSize") ?? "50") || 50,
  });
  return { countryCode: country.code, query, source, result };
}

export function meta() {
  return [{ title: "Sweden domain review – CompanyCollect Backoffice" }];
}

export default function CountryDomainSuggestions({
  loaderData,
}: Route.ComponentProps) {
  return <CompanyDomainSuggestionReview {...loaderData} />;
}
