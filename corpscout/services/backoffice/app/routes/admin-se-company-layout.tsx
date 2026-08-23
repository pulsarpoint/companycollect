import { data, Outlet, useLocation } from "react-router";
import type { Route } from "./+types/admin-se-company-layout";
import { SeCompanyHeader } from "~/components/admin/se-company-header";
import { SeCompanyNotFound } from "~/components/admin/se-company-not-found";
import { loadSeCompanyShell } from "~/lib/se-company-shell.server";
import { seCompanyTabFromPath } from "~/lib/se-company-tabs";

// Only `loader`, `shouldRevalidate`, `meta` and the component live here. Any
// other export that touched `~/lib/*.server` would keep that module in the
// client bundle and break the production build.

export async function loader({ params }: Route.LoaderArgs) {
  const shell = await loadSeCompanyShell(params.companyId);
  // A 404 for an id no table knows. A company the register knows but Dagster
  // has not enriched yet is NOT a 404: the header renders from the register
  // with a note, and the Info tab explains the pipeline state.
  return data({ shell }, shell ? undefined : { status: 404 });
}

/**
 * Switching tabs does not change who the company is, so the shell is not
 * re-read: without this, every tab click fired the layout's own `.data`
 * request alongside the tab's, doubling the round trips for a header that
 * cannot have changed. Only a different `:companyId` invalidates it.
 */
export function shouldRevalidate({
  currentParams,
  nextParams,
}: {
  currentParams: { companyId?: string };
  nextParams: { companyId?: string };
}): boolean {
  return currentParams.companyId !== nextParams.companyId;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.shell?.legal_name ?? params.companyId} | CompanyCollect admin`,
    },
  ];
}

export default function AdminSwedenCompanyLayout({
  loaderData,
  params,
}: Route.ComponentProps) {
  // The active tab is the URL, not state: a tab is a route here, so a cold
  // load and a client navigation must agree on which one is selected.
  const tab = seCompanyTabFromPath(useLocation().pathname);
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      {loaderData.shell ? (
        <>
          <SeCompanyHeader shell={loaderData.shell} tab={tab} />
          <Outlet />
        </>
      ) : (
        <SeCompanyNotFound companyId={params.companyId} />
      )}
    </div>
  );
}
