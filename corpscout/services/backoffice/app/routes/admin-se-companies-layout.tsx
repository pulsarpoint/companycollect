import { NavLink, Outlet, useLocation } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import {
  SE_COMPANIES_TABS,
  seCompaniesTabFromPath,
  seCompaniesTabPath,
} from "~/lib/se-companies-tabs";

export function meta() {
  return [{ title: "Companies | CompanyCollect admin" }];
}

export default function AdminSeCompaniesLayout() {
  // The active tab is the URL, not state: a tab is a route here, so a cold load
  // and a client navigation must agree on which one is selected.
  const tab = seCompaniesTabFromPath(useLocation().pathname);
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">Companies</h1>
        </div>
        <p className="text-sm text-muted-foreground">
          Browse Swedish company information, addresses and financial data.
        </p>
        {/* NavLinks wearing the shadcn Tabs skin, the same trick as
            SeCompanyHeader: the active tab is a route, so it must be navigable,
            linkable and correct on a cold load -- not a client-side selection.
            `value={tab}` comes from the URL for exactly that reason. */}
        <Tabs value={tab}>
          <TabsList variant="line">
            {SE_COMPANIES_TABS.map((entry) => (
              <TabsTrigger
                key={entry.value}
                value={entry.value}
                render={
                  <NavLink to={seCompaniesTabPath(entry.value)} end={entry.value === "info"} />
                }
                nativeButton={false}
              >
                {entry.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>
      </header>
      <Outlet />
    </div>
  );
}
