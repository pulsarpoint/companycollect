import { Link, NavLink, Outlet, useLocation } from "react-router";
import { ArrowLeft, ArrowRight } from "lucide-react";
import type { Route } from "./+types/country-layout";
import { getCountry } from "~/lib/countries";
import { getCountryDirectory } from "~/lib/countries-overview.server";
import {
  getCountryImfOutlook,
  getCountryTradeStatistics,
  getCountryWorldBankStatistics,
} from "~/lib/country-statistics.server";
import { IMF_INDICATORS, WORLD_BANK_INDICATORS } from "~/lib/country-statistics";
import { hasContracts } from "~/lib/contracts.server";
import { hasMarkets } from "~/lib/markets.server";
import { COUNTRY_TABS, type CountryTab } from "~/lib/country-tabs";
import { cn } from "~/lib/utils";
import { Metric, SourceLink, compactUsd, getWorldBankSeries, nf } from "~/components/country/shared";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { Button } from "~/components/ui/button";
import { Skeleton } from "~/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const [directory, worldBank, imf, trade, showContracts, showMarkets] = await Promise.all([
    getCountryDirectory(),
    getCountryWorldBankStatistics(country.code),
    getCountryImfOutlook(country.iso3),
    getCountryTradeStatistics(country.iso3),
    hasContracts(country),
    hasMarkets(country),
  ]);
  const summary = directory.find((row) => row.country_code === country.code);
  if (!summary) throw new Response("Country data not found", { status: 404 });

  return { summary, worldBank, imf, trade, showContracts, showMarkets };
}

export function meta({ params }: Route.MetaArgs) {
  const country = getCountry(params.country);
  return [{ title: `${country?.name ?? "Country"} – CompanyCollect Backoffice` }];
}

export function HydrateFallback() {
  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5">
      <Skeleton className="h-12 w-64" />
      <Skeleton className="h-40" />
      <Skeleton className="h-9 w-96 max-w-full" />
      <Skeleton className="h-96" />
    </div>
  );
}

/** The active tab is read from the URL, not stored in state — the same
 * pathname always resolves to the same tab, including on first paint. */
function activeTabFromPathname(pathname: string, countryCode: string): CountryTab {
  const base = `/countries/${countryCode}`;
  const rest = pathname.startsWith(base) ? pathname.slice(base.length) : "";
  const segment = rest.replace(/^\/+/, "").split("/")[0];
  return (COUNTRY_TABS as readonly string[]).includes(segment) ? (segment as CountryTab) : "overview";
}

/** Tabs that render their own headline figures in place of the country banner. */
const TABS_WITH_OWN_BANNER = new Set(["contracts", "markets"]);

export default function CountryLayout({ loaderData, params }: Route.ComponentProps) {
  const { summary, worldBank, imf, trade, showContracts, showMarkets } = loaderData;
  const country = getCountry(params.country)!;
  const activeTab = activeTabFromPathname(useLocation().pathname, country.code);
  const coverage =
    summary.total_companies > 0
      ? (summary.companies_with_financials / summary.total_companies) * 100
      : 0;

  const gdp = getWorldBankSeries(worldBank.series, WORLD_BANK_INDICATORS.gdp)?.latest;
  const realGdpGrowth = getWorldBankSeries(
    worldBank.series,
    WORLD_BANK_INDICATORS.realGdpGrowth,
  )?.latest;
  const nextImfGrowth = imf.series
    .find((series) => series.indicatorCode === IMF_INDICATORS.realGdpGrowth)
    ?.points.find((point) => point.isEstimate);
  const tradeBalance = trade.latest?.balanceUsd ?? null;

  return (
    <div
      className={cn(
        "mx-auto flex w-full flex-col gap-5",
        // The contracts table pays for the extra width; every other tab keeps
        // the narrower reading-width layout so they don't reflow.
        activeTab === "contracts" ? "max-w-[100rem]" : "max-w-7xl",
      )}
    >
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to="/countries" />}
        >
          <ArrowLeft data-icon="inline-start" />
          Countries
        </Button>
      </div>

      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="flex items-center gap-3 text-3xl font-semibold tracking-tight">
            <span aria-hidden>{country.flag}</span>
            <span>{country.name}</span>
          </h1>
          <p className="text-muted-foreground mt-1 text-sm uppercase tracking-wide">
            {country.code.toUpperCase()} · country intelligence
          </p>
        </div>
        <Button nativeButton={false} render={<Link to={`/countries/${country.code}/companies`} />}>
          View companies
          <ArrowRight data-icon="inline-end" />
        </Button>
      </header>

      {/* The country banner is the DEFAULT, not a fixture. A tab whose subject has
          its own headline figures renders them instead -- contracts show contract
          counts, a top buyer and a top supplier, which say far more on that page
          than GDP and trade balance do. activeTab is already derived from the
          location, so this needs no new plumbing. */}
      {TABS_WITH_OWN_BANNER.has(activeTab) ? null : (
      <section
        aria-label="Country headline statistics"
        className="grid grid-cols-2 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-3 xl:grid-cols-6"
      >
        <Metric
          label="Companies"
          value={nf.format(summary.total_companies)}
          detail="CompanyCollect registry"
        />
        <Metric
          label="Financial coverage"
          value={`${coverage.toFixed(1)}%`}
          detail={`${nf.format(summary.companies_with_financials)} companies`}
        />
        <Metric
          label="GDP"
          value={gdp ? compactUsd.format(gdp.value) : "Unavailable"}
          detail={gdp ? `World Bank · ${gdp.year}` : "World Bank"}
        />
        <Metric
          label="Real GDP growth"
          value={realGdpGrowth ? `${realGdpGrowth.value.toFixed(1)}%` : "Unavailable"}
          detail={realGdpGrowth ? `World Bank · ${realGdpGrowth.year}` : "World Bank"}
        />
        <Metric
          label="Goods trade balance"
          value={tradeBalance === null ? "Unavailable" : compactUsd.format(tradeBalance)}
          detail={trade.latest ? `UN Comtrade · ${trade.latest.year}` : "UN Comtrade"}
        />
        <Metric
          label="Next IMF estimate"
          value={nextImfGrowth ? `${nextImfGrowth.value.toFixed(1)}%` : "Pending"}
          detail={nextImfGrowth ? `Real GDP · ${nextImfGrowth.year}` : "Awaiting WEO load"}
        />
      </section>
      )}

      <Tabs value={activeTab}>
        <TabsList variant="line" className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger
            value="overview"
            render={<NavLink to={`/countries/${country.code}`} end />}
            nativeButton={false}
          >
            Overview
          </TabsTrigger>
          <TabsTrigger
            value="economy"
            render={<NavLink to={`/countries/${country.code}/economy`} />}
            nativeButton={false}
          >
            Economy
          </TabsTrigger>
          <TabsTrigger
            value="trade"
            render={<NavLink to={`/countries/${country.code}/trade`} />}
            nativeButton={false}
          >
            Trade
          </TabsTrigger>
          <TabsTrigger
            value="business"
            render={<NavLink to={`/countries/${country.code}/business`} />}
            nativeButton={false}
          >
            Business
          </TabsTrigger>
          {showContracts ? (
            <TabsTrigger
              value="contracts"
              render={<NavLink to={`/countries/${country.code}/contracts`} />}
              nativeButton={false}
            >
              Contracts
            </TabsTrigger>
          ) : null}
          {showMarkets ? (
            <TabsTrigger
              value="markets"
              render={<NavLink to={`/countries/${country.code}/markets`} />}
              nativeButton={false}
            >
              Markets
            </TabsTrigger>
          ) : null}
        </TabsList>
      </Tabs>

      <div className="pt-3">
        <Outlet />
      </div>

      <footer className="flex flex-col gap-2 border-t pt-4">
        <MethodologyNote />
        <p className="text-muted-foreground text-xs">
          Macro series:{" "}
          <SourceLink href="https://data.worldbank.org/">World Bank</SourceLink>,{" "}
          <SourceLink href="https://www.imf.org/en/Publications/WEO/weo-database">
            IMF World Economic Outlook
          </SourceLink>
          , <SourceLink href="https://comtradeplus.un.org/">UN Comtrade</SourceLink>, and{" "}
          <SourceLink href="https://ec.europa.eu/eurostat/">Eurostat</SourceLink>. Years and
          source scope are shown per observation; values from different sources are not silently
          combined.
        </p>
      </footer>
    </div>
  );
}
