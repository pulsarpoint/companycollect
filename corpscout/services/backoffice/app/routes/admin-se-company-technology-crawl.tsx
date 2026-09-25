import type { Route } from "./+types/admin-se-company-technology-crawl";
import { DomainCrawlView } from "~/components/admin/domain-crawl-view";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { loadDomainCrawlPage } from "~/lib/domain-crawls.server";
export { DomainCrawlErrorBoundary as ErrorBoundary } from "~/components/admin/domain-crawl-view";

export async function loader({params, request}: Route.LoaderArgs) {
  const domains = await getCompanyDomains(getCountry("se")!, params.companyId);
  const requestedDomain = new URL(request.url).searchParams.get("domain")?.trim().toLowerCase().replace(/\.$/, "");
  const selected = domains.find(domain => domain.domain === requestedDomain)
    ?? domains.find(domain => domain.is_primary === 1) ?? domains[0];
  if (!selected) return null;
  return loadDomainCrawlPage(selected.domain, request);
}

export default function CompanyTechnologyCrawl({loaderData}: Route.ComponentProps) {
  if (!loaderData) return <SeCompanyTechnologyNoDomains />;
  return <DomainCrawlView {...loaderData} search={new URLSearchParams({domain: loaderData.domain}).toString()} />;
}
