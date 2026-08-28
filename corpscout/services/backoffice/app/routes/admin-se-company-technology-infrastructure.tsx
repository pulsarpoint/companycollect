import type { Route } from "./+types/admin-se-company-technology-infrastructure";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { TechnologyInfrastructureSection } from "~/components/detail/technology-infrastructure-section";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyInfrastructure } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The admin twin of company-technology-infrastructure.tsx: same query, same
// shared section, but no 404 when no source resolved a domain.

export async function loader({ params, request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  return getCompanyTechnologyInfrastructure(
    getCountry("se")!,
    params.companyId,
    {
      domain: url.searchParams.get("domain") ?? undefined,
      page: Number(url.searchParams.get("page") ?? "1") || 1,
      pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    },
  );
}

export default function AdminSwedenCompanyTechnologyInfrastructure({
  loaderData,
  params,
}: Route.ComponentProps) {
  if (!loaderData) {
    return <SeCompanyTechnologyNoDomains />;
  }
  return (
    <TechnologyInfrastructureSection
      infrastructure={loaderData}
      ipAddressesPath={`/admin/se/company/${params.companyId}/technology/ip-addresses`}
    />
  );
}
