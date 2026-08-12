import type { Route } from "./+types/company-technology-infrastructure";
import { TechnologyInfrastructureSection } from "~/components/detail/technology-infrastructure-section";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyInfrastructure } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }

  const url = new URL(request.url);
  const infrastructure = await getCompanyTechnologyInfrastructure(
    country,
    params.id,
    {
      domain: url.searchParams.get("domain") ?? undefined,
      page: Number(url.searchParams.get("page") ?? "1") || 1,
      pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    },
  );
  if (!infrastructure) {
    throw new Response("Technology information not found", { status: 404 });
  }
  return infrastructure;
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `Infrastructure · ${params.id} – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyTechnologyInfrastructure({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <TechnologyInfrastructureSection
      infrastructure={loaderData}
      ipAddressesPath={`/company/${params.country}/${params.id}/technology/ip-addresses`}
    />
  );
}
