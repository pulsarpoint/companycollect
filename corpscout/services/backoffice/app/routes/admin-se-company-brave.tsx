import type { Route } from "./+types/admin-se-company-brave";
import { BraveResults } from "~/components/admin/brave-results";
import { loadBraveResults } from "~/lib/brave-results.server";
import { seCompanyTabPath } from "~/lib/se-company-tabs";

export async function loader({ params, request }: Route.LoaderArgs) {
  return { results: await loadBraveResults({ countryCode: "SE", companyId: params.companyId }, new URL(request.url).searchParams) };
}

export default function AdminSeCompanyBrave({ loaderData, params }: Route.ComponentProps) {
  return <BraveResults results={loaderData.results} basePath={seCompanyTabPath(params.companyId, "brave")} />;
}
