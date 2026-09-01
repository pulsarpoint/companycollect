import { NavLink, Outlet, useLocation } from "react-router";
import type { Route } from "./+types/admin-se-company-esef-layout";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { loadSeCompanyEsefFilings } from "~/lib/se-company-esef.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The ESEF tab is an area, not a page: "Info" (the aggregated extraction
// view) plus one sub-tab per filed document, each with its own facts, notes,
// and LLM subpages. The sub-tab label is the fiscal year; amendments of the
// same year are disambiguated with the filing version from the fxo id.
export async function loader({ params }: Route.LoaderArgs) {
  return { filings: await loadSeCompanyEsefFilings(params.companyId) };
}

export default function AdminSwedenCompanyEsefLayout({
  loaderData,
  params,
}: Route.ComponentProps) {
  const location = useLocation();
  const basePath = `/admin/se/company/${params.companyId}/esef`;
  const remainder = location.pathname
    .slice(basePath.length)
    .replace(/^\//, "");
  const activeDocument = remainder.split("/")[0] ?? "";
  const yearCounts = new Map<number, number>();
  for (const filing of loaderData.filings) {
    yearCounts.set(
      filing.fiscalYear,
      (yearCounts.get(filing.fiscalYear) ?? 0) + 1,
    );
  }

  return (
    <div className="flex w-full flex-col gap-5">
      <Tabs value={activeDocument === "" ? "info" : activeDocument}>
        <TabsList>
          <TabsTrigger
            value="info"
            render={<NavLink to={basePath} end />}
            nativeButton={false}
          >
            Info
          </TabsTrigger>
          {loaderData.filings.map((filing) => (
            <TabsTrigger
              key={filing.fxoId}
              value={filing.fxoId}
              render={<NavLink to={`${basePath}/${filing.fxoId}`} />}
              nativeButton={false}
            >
              {(yearCounts.get(filing.fiscalYear) ?? 0) > 1
                ? `${filing.fiscalYear} · ${filing.fxoId.slice(-8)}`
                : String(filing.fiscalYear)}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>
      <Outlet />
    </div>
  );
}
