import type { Route } from "./+types/country-markets";
import { getCountry } from "~/lib/countries";
import { getMarketOverview, getTradedCompanies } from "~/lib/markets.server";
import { parseYear } from "~/lib/country-year";
import { MarketsPanel } from "~/components/country/markets-panel";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  // The overview resolves the year (defaulting to the last completed one), and
  // the table follows it — so the headline, the highlighted months and the
  // ranking always describe the same period.
  const requested = parseYear(new URL(request.url).searchParams.get("year"));
  const overview = await getMarketOverview(country, requested);
  const companies = overview
    ? await getTradedCompanies(country, 100, overview.year)
    : [];
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
