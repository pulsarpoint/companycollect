import { data } from "react-router";
import type { Route } from "./+types/admin-se-people-source";
import { PeopleSourceTable } from "~/components/admin/people-source-table";
import { getSwedenPeopleSourceRows } from "~/lib/people-sources.server";
import { isPeopleSourceName } from "~/lib/people-sources";

export async function loader({ params, request }: Route.LoaderArgs) {
  if (!isPeopleSourceName(params.sourceName)) {
    throw data("Unknown people source", { status: 404 });
  }

  const companyId =
    new URL(request.url).searchParams.get("company_id")?.trim() ?? "";
  return getSwedenPeopleSourceRows(params.sourceName, companyId);
}

export function meta({ loaderData }: Route.MetaArgs) {
  const sourceLabel = loaderData?.definition.label ?? "Source";
  return [{ title: `${sourceLabel} people rows | CompanyCollect` }];
}

export default function AdminSwedenPeopleSource({
  loaderData,
}: Route.ComponentProps) {
  return <PeopleSourceTable result={loaderData} />;
}
