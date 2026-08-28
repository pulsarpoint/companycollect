import type { Route } from "./+types/admin-se-company-technology-web-intelligence";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { WebIntelligenceSection } from "~/components/detail/web-intelligence-section";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { getDomainWebIntelligence } from "~/lib/web-intelligence.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The admin twin of company-technology-web-intelligence.tsx: same queries,
// same shared section, but no 404 when no source resolved a domain.

export async function loader({ params, request }: Route.LoaderArgs) {
  const domains = await getCompanyDomains(getCountry("se")!, params.companyId);
  const requestedDomain = new URL(request.url).searchParams
    .get("domain")
    ?.trim()
    .toLowerCase()
    .replace(/\.$/, "");
  const selectedDomain =
    domains.find((domain) => domain.domain === requestedDomain) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0];
  if (!selectedDomain) return null;

  return getDomainWebIntelligence(selectedDomain.domain);
}

export default function AdminSwedenCompanyTechnologyWebIntelligence({
  loaderData,
}: Route.ComponentProps) {
  if (!loaderData) {
    return <SeCompanyTechnologyNoDomains />;
  }
  return <WebIntelligenceSection intelligence={loaderData} />;
}
