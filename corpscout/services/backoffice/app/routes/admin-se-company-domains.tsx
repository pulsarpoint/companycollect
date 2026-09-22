import type { Route } from "./+types/admin-se-company-domains";
import { SeCompanyDomainsTab } from "~/components/admin/se-company-domains";
import { loadSeCompanyDomains, loadSeCompanyDomainRelationships } from "~/lib/se-company-domains.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  const [domains, relationships] = await Promise.all([
    loadSeCompanyDomains(params.companyId),
    loadSeCompanyDomainRelationships(params.companyId),
  ]);
  return { domains, relationships };
}

export default function AdminSwedenCompanyDomains({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyDomainsTab
      companyId={params.companyId}
      domains={loaderData.domains}
      relationships={loaderData.relationships}
    />
  );
}
