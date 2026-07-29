import { redirect, useRouteLoaderData } from "react-router";
import type { Route } from "./+types/country-overview";
import type { loader as countryLayoutLoader } from "./country-layout";
import { getCountry } from "~/lib/countries";
import { getCountryIndustryGroups } from "~/lib/countries-overview.server";
import { getCountryFinancials, TOP_DIVISIONS_LIMIT } from "~/lib/financial-aggregates.server";
import { legacyTabPath } from "~/lib/country-tabs";
import { OverviewTab } from "~/components/country/overview-tab";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  // Old bookmarks used /countries/:country?tab=economy|trade|business|contracts
  // and switched panels client-side. Those tabs are now real routes.
  const url = new URL(request.url);
  const redirectTo = legacyTabPath(country.code, url.searchParams.get("tab"));
  if (redirectTo) throw redirect(redirectTo);

  const [financials, coverageIndustries] = await Promise.all([
    getCountryFinancials(country.code),
    getCountryIndustryGroups(country.code),
  ]);

  const revenueIndustries = financials?.divisions?.slice(0, TOP_DIVISIONS_LIMIT) ?? null;
  return {
    industries: revenueIndustries ?? coverageIndustries,
    industryMode: revenueIndustries ? ("revenue" as const) : ("coverage" as const),
    topCompanies: financials?.topCompanies ?? [],
  };
}

export default function CountryOverview({ loaderData, params }: Route.ComponentProps) {
  const { industries, industryMode, topCompanies } = loaderData;
  const country = getCountry(params.country)!;
  const layoutData = useRouteLoaderData<typeof countryLayoutLoader>("routes/country-layout")!;

  return (
    <div className="flex flex-col gap-4">
      <OverviewTab
        countryCode={country.code}
        worldBank={layoutData.worldBank.series}
        trade={layoutData.trade}
        industries={industries}
        industryMode={industryMode}
        topCompanies={topCompanies}
      />
    </div>
  );
}
