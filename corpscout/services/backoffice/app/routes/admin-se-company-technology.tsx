import type { Route } from "./+types/admin-se-company-technology";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { TechnologyOverview } from "~/components/detail/technology-overview";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyDetail } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The selected domain uses the same overview as its standalone domain page.
// Company association context stays in the parent selector and Domains tab.

export async function loader({ params, request }: Route.LoaderArgs) {
  return getCompanyTechnologyDetail(
    getCountry("se")!,
    params.companyId,
    new URL(request.url).searchParams.get("domain") ?? undefined,
  );
}

export default function AdminSwedenCompanyTechnology({
  loaderData,
}: Route.ComponentProps) {
  if (loaderData.domains.length === 0) {
    return <SeCompanyTechnologyNoDomains />;
  }

  return <TechnologyOverview domain={loaderData.selectedDomain} data={loaderData} />;
}
