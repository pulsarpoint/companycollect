import type { Route } from "./+types/companies";
import { loadCompanyList } from "~/lib/company-list.server";
import { CompanyListPage } from "~/components/companies/company-list-page";

export function meta(_: Route.MetaArgs) {
  return [{ title: "Companies – CompanyCollect Backoffice" }];
}

export async function loader({ request }: Route.LoaderArgs) {
  return await loadCompanyList(request);
}

export default function Companies({ loaderData }: Route.ComponentProps) {
  return <CompanyListPage data={loaderData} />;
}
