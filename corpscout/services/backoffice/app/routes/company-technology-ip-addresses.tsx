import type { Route } from "./+types/company-technology-ip-addresses";
import { TechnologyIpAddressesSection } from "~/components/detail/technology-ip-addresses-section";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyIpInventory } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }

  const url = new URL(request.url);
  const inventory = await getCompanyTechnologyIpInventory(country, params.id, {
    domain: url.searchParams.get("domain") ?? undefined,
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "25") || 25,
  });
  if (!inventory) {
    throw new Response("Technology information not found", { status: 404 });
  }
  return inventory;
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `IP addresses · ${params.id} – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyTechnologyIpAddresses({
  loaderData,
}: Route.ComponentProps) {
  return <TechnologyIpAddressesSection inventory={loaderData} />;
}
