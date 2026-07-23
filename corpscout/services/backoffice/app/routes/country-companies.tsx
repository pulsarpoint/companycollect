import { redirect } from "react-router";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { loadCompanyList } from "~/lib/company-list.server";
import { CompanyListPage } from "~/components/companies/company-list-page";

export async function loader({ request, params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const url = new URL(request.url);
  if (url.searchParams.has("f_country")) {
    url.searchParams.delete("f_country");
    const search = url.searchParams.toString();
    throw redirect(`${url.pathname}${search ? `?${search}` : ""}`);
  }

  return await loadCompanyList(request, country.code);
}

export function meta({ params }: Route.MetaArgs) {
  const country = getCountry(params.country);
  return [{ title: `${country?.name ?? "Country"} companies – CompanyCollect Backoffice` }];
}

export default function CountryCompanies({ loaderData, params }: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  return <CompanyListPage data={loaderData} lockedCountry={country} />;
}
