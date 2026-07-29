import type { Route } from "./+types/country-business";
import { getCountry } from "~/lib/countries";
import { getCountryIndustryGroups } from "~/lib/countries-overview.server";
import { getCountryEurostatBusinessStats } from "~/lib/country-statistics.server";
import { getCountryFinancials, TOP_DIVISIONS_LIMIT } from "~/lib/financial-aggregates.server";
import { BusinessTab } from "~/components/country/business-tab";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const [eurostat, financials, coverageIndustries] = await Promise.all([
    getCountryEurostatBusinessStats(country),
    getCountryFinancials(country.code),
    getCountryIndustryGroups(country.code),
  ]);

  const revenueIndustries = financials?.divisions?.slice(0, TOP_DIVISIONS_LIMIT) ?? null;
  return {
    eurostat,
    industries: revenueIndustries ?? coverageIndustries,
    industryMode: revenueIndustries ? ("revenue" as const) : ("coverage" as const),
    topCompanies: financials?.topCompanies ?? [],
  };
}

export default function CountryBusiness({ loaderData, params }: Route.ComponentProps) {
  const { eurostat, industries, industryMode, topCompanies } = loaderData;
  const country = getCountry(params.country)!;

  return (
    <div className="flex flex-col gap-4">
      <BusinessTab
        countryCode={country.code}
        countryName={country.name}
        eurostat={eurostat}
        industries={industries}
        industryMode={industryMode}
        topCompanies={topCompanies}
      />
    </div>
  );
}
