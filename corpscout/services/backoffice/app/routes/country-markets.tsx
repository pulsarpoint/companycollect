import type { Route } from "./+types/country-markets";
import { getCountry } from "~/lib/countries";
import { getMarketOverview, getTradedCompanies } from "~/lib/markets.server";
import { MarketsPanel } from "~/components/country/markets-panel";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const [overview, companies] = await Promise.all([
    getMarketOverview(country),
    getTradedCompanies(country),
  ]);
  return { overview, companies };
}

export default function CountryMarkets({ loaderData, params }: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  return (
    <MarketsPanel
      countryCode={country.code}
      overview={loaderData.overview}
      companies={loaderData.companies}
    />
  );
}
