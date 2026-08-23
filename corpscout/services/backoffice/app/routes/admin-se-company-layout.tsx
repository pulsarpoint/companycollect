import { data, Outlet, useLocation } from "react-router";
import type { Route } from "./+types/admin-se-company-layout";
import { SeCompanyHeader } from "~/components/admin/se-company-header";
import { SeCompanyInfoNotPublished } from "~/components/admin/se-company-info-review-workspace";
import { loadSeCompanyShell } from "~/lib/se-company-shell.server";
import { seCompanyTabFromPath } from "~/lib/se-company-tabs";

// Only `loader`, `meta` and the component live here. Any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build.

export async function loader({ params }: Route.LoaderArgs) {
  const shell = await loadSeCompanyShell(params.companyId);
  // Same 404 contract as the Info tab: an id neither the published table nor
  // the register knows is a broken link and says so with a 404, while a
  // company the register knows but Dagster has not enriched yet renders
  // normally with a note.
  return data({ shell }, shell ? undefined : { status: 404 });
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
  if (!loaderData.shell) {
    return (
      <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
        <SeCompanyInfoNotPublished companyId={params.companyId} />
      </div>
    );
  }
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <SeCompanyHeader shell={loaderData.shell} tab={tab} />
      <Outlet />
    </div>
  );
}
