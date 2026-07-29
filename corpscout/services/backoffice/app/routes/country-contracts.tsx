import type { Route } from "./+types/country-contracts";
import { getCountry } from "~/lib/countries";
import { getCountryContractsPage } from "~/lib/contracts.server";
import { CountryContractsTable } from "~/components/country/contracts-table";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const url = new URL(request.url);
  return getCountryContractsPage(country, {
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
  });
}

export default function CountryContracts({ loaderData, params }: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  return <CountryContractsTable countryCode={country.code} page={loaderData} />;
}
