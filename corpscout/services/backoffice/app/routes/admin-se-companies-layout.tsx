import { NavLink, Outlet, useLocation } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import {
  SE_COMPANIES_TABS,
  seCompaniesTabFromPath,
  seCompaniesTabPath,
} from "~/lib/se-companies-tabs";

// The all-companies LIST area: one header + tab bar, one <Outlet/> for the
// active tab. It mirrors admin-se-company-layout.tsx (the single-company DETAIL
// area) but carries NO loader of its own: the header here is static and every
// tab loads only its own data, so there is nothing shared to re-read on a tab
// switch and therefore no shouldRevalidate to write -- switching tabs already
// runs just the tab's loader, never a layout .data request beside it.
//
// Only the component lives here; nothing imports `~/lib/*.server`, so this
// module stays out of the client bundle's server-code path (see CLAUDE.md).

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
        <h1 className="text-2xl font-semibold tracking-tight">Companies</h1>
        <p className="text-sm text-muted-foreground">
          Every Swedish company Dagster publishes, one register read at a time:
          browse the info list, review geocoding, and (soon) financials.
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
