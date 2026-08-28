import type { Route } from "./+types/admin-se-company-listed";
import { SeCompanyListedTab } from "~/components/admin/se-company-listed";
import { loadSeCompanyListed } from "~/lib/se-company-listed.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { listed: await loadSeCompanyListed(params.companyId) };
}

export default function AdminSwedenCompanyListed({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyListedTab
      companyId={params.companyId}
      listed={loaderData.listed}
    />
  );
}
