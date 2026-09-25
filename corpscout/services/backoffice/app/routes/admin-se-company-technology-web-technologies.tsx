import type { Route } from "./+types/admin-se-company-technology-web-technologies";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { DomainWebtechView } from "~/components/admin/domain-webtech-view";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { getDomainWebtech } from "~/lib/webtech.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// Resolve the company's selected domain before showing the shared admin view.

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

  return getDomainWebtech(selectedDomain.domain);
}

export default function AdminSwedenCompanyTechnologyWebTechnologies({
  loaderData,
}: Route.ComponentProps) {
  if (!loaderData) {
    return <SeCompanyTechnologyNoDomains />;
  }
  return <DomainWebtechView data={loaderData} />;
}
