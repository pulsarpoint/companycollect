import type { Route } from "./+types/admin-se-company-esef";
import { loadSeCompanyEsef } from "~/lib/se-company-esef.server";

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadSeCompanyEsef(params.companyId);
  if (!detail) throw new Response("No ESEF data for company", { status: 404 });
  return detail;
}

export default function AdminSwedenCompanyEsef({
  loaderData,
}: Route.ComponentProps) {
  return <pre>{JSON.stringify(loaderData.filings, null, 2)}</pre>;
}
