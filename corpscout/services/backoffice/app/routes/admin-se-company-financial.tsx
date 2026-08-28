import type { Route } from "./+types/admin-se-company-financial";
import { SeFinancialsView } from "~/components/financials/se-financials-view";
import { getCountry } from "~/lib/countries";
import { getCompanyFinancialDetail } from "~/lib/queries.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The tab is the public financials experience, not an admin re-reading of the
// raw tables: it loads through the same query the public page uses and renders
// the same shared view, deep-linking into the public facts/report readers.

export async function loader({ params }: Route.LoaderArgs) {
  return getCompanyFinancialDetail(getCountry("se")!, params.companyId);
}

export default function AdminSwedenCompanyFinancial({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { financialSources, filingStatus } = loaderData;
  const factsQuery = getCountry("se")?.detail?.factsQuery;
  return (
    <SeFinancialsView
      financialSources={financialSources}
      filingStatus={filingStatus}
      basePath={`/company/se/${params.companyId}/financials`}
      factsBase={
        factsQuery ? `/company/se/${params.companyId}/facts` : undefined
      }
    />
  );
}
