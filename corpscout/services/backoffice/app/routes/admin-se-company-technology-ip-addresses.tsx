import type { Route } from "./+types/admin-se-company-technology-ip-addresses";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { TechnologyIpAddressesSection } from "~/components/detail/technology-ip-addresses-section";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyIpInventory } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The admin twin of company-technology-ip-addresses.tsx: same query, same
// shared section, but no 404 when no source resolved a domain.

export async function loader({ params, request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  return getCompanyTechnologyIpInventory(getCountry("se")!, params.companyId, {
    domain: url.searchParams.get("domain") ?? undefined,
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "25") || 25,
  });
}

export default function AdminSwedenCompanyTechnologyIpAddresses({
  loaderData,
}: Route.ComponentProps) {
  if (!loaderData) {
    return <SeCompanyTechnologyNoDomains />;
  }
  return <TechnologyIpAddressesSection inventory={loaderData} />;
}
