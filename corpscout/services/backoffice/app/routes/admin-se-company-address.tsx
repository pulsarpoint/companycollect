import type { Route } from "./+types/admin-se-company-address";
import { SeCompanyAddressTab } from "~/components/admin/se-company-address";
import { loadSeCompanyAddresses } from "~/lib/se-company-address.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { detail: await loadSeCompanyAddresses(params.companyId) };
}

export default function AdminSwedenCompanyAddress({
  loaderData,
}: Route.ComponentProps) {
  return <SeCompanyAddressTab detail={loaderData.detail} />;
}
