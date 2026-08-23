import type { Route } from "./+types/admin-se-company-domains";
import { SeCompanyDomainsTab } from "~/components/admin/se-company-domains";
import { loadSeCompanyDomains } from "~/lib/se-company-domains.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { domains: await loadSeCompanyDomains(params.companyId) };
}

export default function AdminSwedenCompanyDomains({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyDomainsTab
      companyId={params.companyId}
      domains={loaderData.domains}
    />
  );
}
