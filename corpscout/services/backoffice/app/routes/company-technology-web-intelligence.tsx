import type { Route } from "./+types/company-technology-web-intelligence";
import { WebIntelligenceSection } from "~/components/detail/web-intelligence-section";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { getDomainWebIntelligence } from "~/lib/web-intelligence.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }

  const domains = await getCompanyDomains(country, params.id);
  const requestedDomain = new URL(request.url).searchParams
    .get("domain")
    ?.trim()
    .toLowerCase()
    .replace(/\.$/, "");
  const selectedDomain =
    domains.find((domain) => domain.domain === requestedDomain) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0];
  if (!selectedDomain) {
    throw new Response("Web intelligence not found", { status: 404 });
  }

  return getDomainWebIntelligence(selectedDomain.domain);
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `Web intelligence · ${params.id} – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyTechnologyWebIntelligence({
  loaderData,
}: Route.ComponentProps) {
  return <WebIntelligenceSection intelligence={loaderData} />;
}
