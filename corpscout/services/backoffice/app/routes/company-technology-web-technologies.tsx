import type { Route } from "./+types/company-technology-web-technologies";
import { WebtechSection } from "~/components/detail/webtech-section";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { getDomainWebtech } from "~/lib/webtech.server";

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
    throw new Response("Web technologies not found", { status: 404 });
  }

  return getDomainWebtech(selectedDomain.domain);
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `Web technologies · ${params.id} – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyTechnologyWebTechnologies({
  loaderData,
}: Route.ComponentProps) {
  return <WebtechSection data={loaderData} />;
}
