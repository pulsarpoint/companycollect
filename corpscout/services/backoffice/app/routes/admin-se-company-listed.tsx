import type { Route } from "./+types/admin-se-company-listed";
import { SeCompanyListedTab } from "~/components/admin/se-company-listed";
import { loadSeCompanyListed } from "~/lib/se-company-listed.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params, request }: Route.LoaderArgs) {
  const line = new URL(request.url).searchParams.get("line") ?? undefined;
  return { listed: await loadSeCompanyListed(params.companyId, line) };
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
