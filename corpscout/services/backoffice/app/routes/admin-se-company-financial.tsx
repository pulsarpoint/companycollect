import type { Route } from "./+types/admin-se-company-financial";
import { SeCompanyFinancialTab } from "~/components/admin/se-company-financial";
import { loadSeCompanyFinancialDetail } from "~/lib/se-company-financial.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { detail: await loadSeCompanyFinancialDetail(params.companyId) };
}

export default function AdminSwedenCompanyFinancial({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyFinancialTab
      companyId={params.companyId}
      detail={loaderData.detail}
    />
  );
}
