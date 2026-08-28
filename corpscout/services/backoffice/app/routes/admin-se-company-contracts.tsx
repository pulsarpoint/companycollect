import type { Route } from "./+types/admin-se-company-contracts";
import { SeCompanyContractsTab } from "~/components/admin/se-company-contracts";
import { loadSeCompanyContracts } from "~/lib/se-company-contracts.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { contracts: await loadSeCompanyContracts(params.companyId) };
}

export default function AdminSwedenCompanyContracts({
  loaderData,
}: Route.ComponentProps) {
  return <SeCompanyContractsTab contracts={loaderData.contracts} />;
}
