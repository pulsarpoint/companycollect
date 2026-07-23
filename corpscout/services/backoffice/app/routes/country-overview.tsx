import { useState } from "react";
import { Link, useSearchParams } from "react-router";
import {
  ArrowLeft,
  ArrowRight,
  Building2,
  Database,
} from "lucide-react";
import type { Route } from "./+types/country-overview";
import { getCountry } from "~/lib/countries";
import {
  getCountryDirectory,
  getCountryIndustryGroups,
} from "~/lib/countries-overview.server";
import {
  getCountryEurostatBusinessStats,
  getCountryImfOutlook,
  getCountryTradeStatistics,
  getCountryWorldBankStatistics,
} from "~/lib/country-statistics.server";
import {
  IMF_INDICATORS,
  WORLD_BANK_INDICATORS,
  type CountryImfSeries,
  type CountryWorldBankSeries,
  type EurostatMetricKey,
} from "~/lib/country-statistics";
import {
  getCountryFinancials,
  TOP_DIVISIONS_LIMIT,
} from "~/lib/financial-aggregates.server";
import {
  BusinessSizeChart,
  EconomicPulseChart,
  ImfForecastChart,
  TradeHistoryChart,
} from "~/components/countries/country-statistics-charts";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { RevenueBarChart } from "~/components/financials/revenue-bar-chart";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Skeleton } from "~/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";

const COUNTRY_TABS = ["overview", "economy", "trade", "business"] as const;
type CountryTab = (typeof COUNTRY_TABS)[number];

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const [
    directory,
    financials,
    coverageIndustries,
    worldBank,
    imf,
    trade,
    eurostat,
  ] = await Promise.all([
    getCountryDirectory(),
    getCountryFinancials(country.code),
    getCountryIndustryGroups(country.code),
    getCountryWorldBankStatistics(country.code),
    getCountryImfOutlook(country.iso3),
    getCountryTradeStatistics(country.iso3),
    getCountryEurostatBusinessStats(country),
  ]);
  const summary = directory.find((row) => row.country_code === country.code);
  if (!summary) throw new Response("Country data not found", { status: 404 });

  const revenueIndustries = financials?.divisions?.slice(0, TOP_DIVISIONS_LIMIT) ?? null;
  return {
    summary,
    worldBank,
    imf,
    trade,
    eurostat,
    industries: revenueIndustries ?? coverageIndustries,
    industryMode: revenueIndustries ? ("revenue" as const) : ("coverage" as const),
    topCompanies: financials?.topCompanies ?? [],
  };
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

const nf = new Intl.NumberFormat("en-US");
const compactUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 2,
});
const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 2,
});

function Metric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="min-w-0 px-4 py-4 first:pl-0">
      <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">{label}</p>
      <p className="mt-1 truncate text-xl font-semibold tabular-nums" title={value}>
        {value}
      </p>
      {detail ? <p className="text-muted-foreground mt-1 truncate text-xs">{detail}</p> : null}
    </div>
  );
}

function getWorldBankSeries(
  series: CountryWorldBankSeries[],
  indicatorCode: string,
): CountryWorldBankSeries | undefined {
  return series.find((item) => item.indicatorCode === indicatorCode);
}

function formatWorldBankValue(indicatorCode: string, value: number): string {
  if (
    indicatorCode === WORLD_BANK_INDICATORS.gdp ||
    indicatorCode === WORLD_BANK_INDICATORS.exports ||
    indicatorCode === WORLD_BANK_INDICATORS.imports
  ) {
    return compactUsd.format(value);
  }
  if (
    indicatorCode === WORLD_BANK_INDICATORS.realGdpGrowth ||
    indicatorCode === WORLD_BANK_INDICATORS.inflation ||
    indicatorCode === WORLD_BANK_INDICATORS.unemployment
  ) {
    return `${value.toFixed(1)}%`;
  }
  if (indicatorCode === WORLD_BANK_INDICATORS.population) {
    return compactNumber.format(value);
  }
  return compactUsd.format(value);
}

function formatImfValue(series: CountryImfSeries, value: number): string {
  if (series.indicatorCode === IMF_INDICATORS.nominalGdp) {
    return `$${value.toLocaleString("en-US", { maximumFractionDigits: 1 })}B`;
  }
  return `${value.toFixed(1)}%`;
}

function EmptyData({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <Empty className="min-h-56 border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Database />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

export default function CountryOverview({ loaderData, params }: Route.ComponentProps) {
  const {
    summary,
    worldBank,
    imf,
    trade,
    eurostat,
    industries,
    industryMode,
    topCompanies,
  } = loaderData;
  const country = getCountry(params.country)!;
  const [searchParams, setSearchParams] = useSearchParams();
  const activeTab = parseTab(searchParams.get("tab"));
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
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5">
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

      <Tabs
        value={activeTab}
        onValueChange={(value) => {
          const nextTab = parseTab(String(value));
          const next = new URLSearchParams(searchParams);
          if (nextTab === "overview") next.delete("tab");
          else next.set("tab", nextTab);
          setSearchParams(next, { replace: true, preventScrollReset: true });
        }}
      >
        <TabsList variant="line" className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="economy">Economy</TabsTrigger>
          <TabsTrigger value="trade">Trade</TabsTrigger>
          <TabsTrigger value="business">Business</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="flex flex-col gap-4 pt-3">
          <OverviewTab
            countryCode={country.code}
            worldBank={worldBank.series}
            trade={trade}
            summary={summary}
            industries={industries}
            industryMode={industryMode}
            topCompanies={topCompanies}
          />
        </TabsContent>

        <TabsContent value="economy" className="pt-3">
          <EconomyTab
            worldBank={worldBank.series}
            worldBankUpdatedDate={worldBank.sourceUpdatedDate}
            imf={imf}
          />
        </TabsContent>

        <TabsContent value="trade" className="pt-3">
          <TradeTab worldBank={worldBank.series} trade={trade} />
        </TabsContent>

        <TabsContent value="business" className="flex flex-col gap-4 pt-3">
          <BusinessTab
            countryCode={country.code}
            countryName={country.name}
            eurostat={eurostat}
            industries={industries}
            industryMode={industryMode}
            topCompanies={topCompanies}
          />
        </TabsContent>
      </Tabs>

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

function OverviewTab({
  countryCode,
  worldBank,
  trade,
  summary,
  industries,
  industryMode,
  topCompanies,
}: {
  countryCode: string;
  worldBank: CountryWorldBankSeries[];
  trade: Route.ComponentProps["loaderData"]["trade"];
  summary: Route.ComponentProps["loaderData"]["summary"];
  industries: Route.ComponentProps["loaderData"]["industries"];
  industryMode: Route.ComponentProps["loaderData"]["industryMode"];
  topCompanies: Route.ComponentProps["loaderData"]["topCompanies"];
}) {
  const latestYear = Math.max(
    ...worldBank.flatMap((series) => series.points.map((point) => point.year)),
  );
  const latestTrade = trade.latest;

  return (
    <>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Economic pulse</CardTitle>
            <CardDescription>
              Real growth, consumer-price inflation, and unemployment over the last decade.
            </CardDescription>
            <CardAction>
              <Badge variant="outline">World Bank</Badge>
            </CardAction>
          </CardHeader>
          <CardContent>
            <EconomicPulseChart series={worldBank} minYear={latestYear - 9} />
          </CardContent>
        </Card>

        <div className="grid gap-4">
          <Card size="sm">
            <CardHeader>
              <CardTitle>Trade snapshot</CardTitle>
              <CardDescription>
                Merchandise trade reported to UN Comtrade.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-2">
              <Metric
                label="Exports"
                value={
                  latestTrade?.exportsUsd === null || latestTrade?.exportsUsd === undefined
                    ? "—"
                    : compactUsd.format(latestTrade.exportsUsd)
                }
                detail={latestTrade ? String(latestTrade.year) : undefined}
              />
              <Metric
                label="Imports"
                value={
                  latestTrade?.importsUsd === null || latestTrade?.importsUsd === undefined
                    ? "—"
                    : compactUsd.format(latestTrade.importsUsd)
                }
                detail={latestTrade ? String(latestTrade.year) : undefined}
              />
            </CardContent>
          </Card>

          <Card size="sm">
            <CardHeader>
              <CardTitle>Company reporting</CardTitle>
              <CardDescription>Latest available company financials.</CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-2">
              <Metric
                label="Reported revenue"
                value={formatRevenueUsd(summary.revenue_usd, null)}
              />
              <Metric
                label="Latest FY"
                value={String(summary.latest_fiscal_year ?? "—")}
              />
            </CardContent>
          </Card>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Leading industries</CardTitle>
            <CardDescription>
              {industryMode === "revenue"
                ? "Top NACE divisions by reported revenue."
                : "Top registry industry groups by company count."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {industries.length > 0 ? (
              <IndustryTable
                countryCode={countryCode}
                industries={industries.slice(0, 5)}
                industryMode={industryMode}
              />
            ) : (
              <EmptyData
                title="No industry data"
                description="This registry does not currently expose a usable industry classification."
              />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Most valuable companies</CardTitle>
            <CardDescription>Latest reported company revenue.</CardDescription>
          </CardHeader>
          <CardContent>
            {topCompanies.length > 0 ? (
              <TopCompaniesTable companies={topCompanies.slice(0, 5)} showCountry={false} />
            ) : (
              <EmptyData
                title="No ranked companies"
                description="Financial-company rankings are not available for this country yet."
              />
            )}
          </CardContent>
        </Card>
      </div>
    </>
  );
}

function EconomyTab({
  worldBank,
  worldBankUpdatedDate,
  imf,
}: {
  worldBank: CountryWorldBankSeries[];
  worldBankUpdatedDate: string | null;
  imf: Route.ComponentProps["loaderData"]["imf"];
}) {
  const [range, setRange] = useState<"5" | "10" | "max">("10");
  const latestYear = Math.max(
    ...worldBank.flatMap((series) => series.points.map((point) => point.year)),
  );
  const minYear = range === "max" ? undefined : latestYear - Number(range) + 1;

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Economic history</CardTitle>
          <CardDescription>
            Comparable World Bank annual measures; each series keeps its own observation year.
          </CardDescription>
          <CardAction>
            <ToggleGroup
              value={[range]}
              onValueChange={(values) => {
                const next = values.at(-1) as typeof range | undefined;
                if (next) setRange(next);
              }}
              variant="outline"
              size="sm"
              spacing={0}
              aria-label="Economic history range"
            >
              <ToggleGroupItem value="5">5Y</ToggleGroupItem>
              <ToggleGroupItem value="10">10Y</ToggleGroupItem>
              <ToggleGroupItem value="max">Max</ToggleGroupItem>
            </ToggleGroup>
          </CardAction>
        </CardHeader>
        <CardContent>
          <EconomicPulseChart series={worldBank} minYear={minYear} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Latest World Bank observations</CardTitle>
          <CardDescription>National macro indicators, shown without cross-source fallback.</CardDescription>
          {worldBankUpdatedDate ? (
            <CardAction>
              <Badge variant="outline">Updated {worldBankUpdatedDate}</Badge>
            </CardAction>
          ) : null}
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Indicator</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead className="text-right">Year</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {worldBank.map((series) => (
                <TableRow key={series.indicatorCode}>
                  <TableCell>
                    <p className="font-medium">{series.indicatorName}</p>
                    <p className="text-muted-foreground font-mono text-xs">
                      {series.indicatorCode}
                    </p>
                  </TableCell>
                  <TableCell className="text-right font-medium tabular-nums">
                    {formatWorldBankValue(series.indicatorCode, series.latest.value)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {series.latest.year}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>IMF outlook</CardTitle>
          <CardDescription>
            Latest World Economic Outlook vintage, with estimates kept distinct from actuals.
          </CardDescription>
          {imf.vintageDate ? (
            <CardAction>
              <Badge variant="outline">Vintage {imf.vintageDate}</Badge>
            </CardAction>
          ) : null}
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {imf.series.length > 0 ? (
            <>
              <ImfForecastChart series={imf.series} />
              <ImfOutlookTable series={imf.series} />
            </>
          ) : (
            <EmptyData
              title="IMF outlook is not loaded yet"
              description="The country page remains available while the IMF WEO asset is awaiting its first materialization."
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function ImfOutlookTable({ series }: { series: CountryImfSeries[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Indicator</TableHead>
          <TableHead className="text-right">Latest actual</TableHead>
          <TableHead className="text-right">Next estimate</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {series.map((item) => {
          const actual = item.points.filter((point) => !point.isEstimate).at(-1);
          const estimate = item.points.find((point) => point.isEstimate);
          return (
            <TableRow key={item.indicatorCode}>
              <TableCell>
                <p className="font-medium">{item.indicatorName}</p>
                <p className="text-muted-foreground font-mono text-xs">
                  {item.indicatorCode}
                </p>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {actual ? `${formatImfValue(item, actual.value)} · ${actual.year}` : "—"}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {estimate ? (
                  <span className="inline-flex items-center justify-end gap-2">
                    {formatImfValue(item, estimate.value)} · {estimate.year}
                    <Badge variant="secondary">Estimate</Badge>
                  </span>
                ) : (
                  "—"
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function TradeTab({
  worldBank,
  trade,
}: {
  worldBank: CountryWorldBankSeries[];
  trade: Route.ComponentProps["loaderData"]["trade"];
}) {
  const wbExports = getWorldBankSeries(worldBank, WORLD_BANK_INDICATORS.exports)?.latest;
  const wbImports = getWorldBankSeries(worldBank, WORLD_BANK_INDICATORS.imports)?.latest;
  const latest = trade.latest;

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Merchandise trade history</CardTitle>
          <CardDescription>
            Annual exports, imports, and derived balance in current US dollars.
          </CardDescription>
          <CardAction>
            <Badge variant="outline">UN Comtrade</Badge>
          </CardAction>
        </CardHeader>
        <CardContent>
          {trade.points.length > 0 ? (
            <TradeHistoryChart points={trade.points} />
          ) : (
            <EmptyData
              title="No trade history"
              description="UN Comtrade annual totals are not available for this reporter yet."
            />
          )}
        </CardContent>
      </Card>

      {latest ? (
        <section
          aria-label="Latest trade measures"
          className="grid rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 sm:grid-cols-3"
        >
          <Metric
            label="Goods exports"
            value={latest.exportsUsd === null ? "—" : compactUsd.format(latest.exportsUsd)}
            detail={`UN Comtrade · ${latest.year}`}
          />
          <Metric
            label="Goods imports"
            value={latest.importsUsd === null ? "—" : compactUsd.format(latest.importsUsd)}
            detail={`UN Comtrade · ${latest.year}`}
          />
          <Metric
            label="Balance"
            value={latest.balanceUsd === null ? "—" : compactUsd.format(latest.balanceUsd)}
            detail={
              latest.exportsReported === false || latest.importsReported === false
                ? "Adjusted or estimated total"
                : "Reporter total"
            }
          />
        </section>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Source comparison</CardTitle>
          <CardDescription>
            These measures answer different questions and are intentionally presented side by side.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Source</TableHead>
                <TableHead>Measure</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead className="text-right">Year</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TradeComparisonRow
                source="UN Comtrade"
                measure="Exports"
                scope="Merchandise goods"
                value={latest?.exportsUsd ?? null}
                year={latest?.year ?? null}
              />
              <TradeComparisonRow
                source="World Bank"
                measure="Exports"
                scope="Goods and services"
                value={wbExports?.value ?? null}
                year={wbExports?.year ?? null}
              />
              <TradeComparisonRow
                source="UN Comtrade"
                measure="Imports"
                scope="Merchandise goods"
                value={latest?.importsUsd ?? null}
                year={latest?.year ?? null}
              />
              <TradeComparisonRow
                source="World Bank"
                measure="Imports"
                scope="Goods and services"
                value={wbImports?.value ?? null}
                year={wbImports?.year ?? null}
              />
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}

function TradeComparisonRow({
  source,
  measure,
  scope,
  value,
  year,
}: {
  source: string;
  measure: string;
  scope: string;
  value: number | null;
  year: number | null;
}) {
  return (
    <TableRow>
      <TableCell>
        <Badge variant="outline">{source}</Badge>
      </TableCell>
      <TableCell className="font-medium">{measure}</TableCell>
      <TableCell className="text-muted-foreground">{scope}</TableCell>
      <TableCell className="text-right font-medium tabular-nums">
        {value === null ? "—" : compactUsd.format(value)}
      </TableCell>
      <TableCell className="text-right tabular-nums">{year ?? "—"}</TableCell>
    </TableRow>
  );
}

function BusinessTab({
  countryCode,
  countryName,
  eurostat,
  industries,
  industryMode,
  topCompanies,
}: {
  countryCode: string;
  countryName: string;
  eurostat: Route.ComponentProps["loaderData"]["eurostat"];
  industries: Route.ComponentProps["loaderData"]["industries"];
  industryMode: Route.ComponentProps["loaderData"]["industryMode"];
  topCompanies: Route.ComponentProps["loaderData"]["topCompanies"];
}) {
  const businessSizeYear = Math.max(
    ...eurostat.sizeRows.flatMap((row) =>
      [
        row.enterprisesYear,
        row.employmentYear,
        row.turnoverYear,
        row.valueAddedYear,
      ].filter((year): year is number => year !== null),
    ),
  );

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Business demography</CardTitle>
          <CardDescription>
            Eurostat enterprise dynamics across the business economy.
          </CardDescription>
          <CardAction>
            <Badge variant={eurostat.coverage === "full" ? "secondary" : "outline"}>
              {eurostat.coverage === "none"
                ? "Not covered"
                : `${eurostat.datasetCount} datasets · ${eurostat.coverage}`}
            </Badge>
          </CardAction>
        </CardHeader>
        <CardContent>
          {eurostat.metrics.length > 0 ? (
            <div className="grid sm:grid-cols-2 lg:grid-cols-3">
              {orderedEurostatMetrics(eurostat.metrics).map((metric) => (
                <Metric
                  key={metric.key}
                  label={metric.label}
                  value={
                    metric.unit === "percent"
                      ? `${metric.value.toFixed(1)}%`
                      : nf.format(metric.value)
                  }
                  detail={`Eurostat · ${metric.year}${formatEurostatStatus(metric.status)}`}
                />
              ))}
            </div>
          ) : (
            <EmptyData
              title="No Eurostat business demography"
              description={`${countryName} is not covered by the selected Eurostat business datasets, or no observations have been loaded yet.`}
            />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Business economy by company size</CardTitle>
          <CardDescription>
            Enterprises, employment, turnover, and value added by employee-size class.
          </CardDescription>
          {Number.isFinite(businessSizeYear) ? (
            <CardAction>
              <Badge variant="outline">Eurostat · {businessSizeYear}</Badge>
            </CardAction>
          ) : null}
        </CardHeader>
        <CardContent>
          {eurostat.sizeRows.length > 0 ? (
            <BusinessSizeChart rows={eurostat.sizeRows} />
          ) : (
            <EmptyData
              title="No company-size breakdown"
              description="Eurostat structural business statistics are unavailable for this country."
            />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Industries</CardTitle>
          <CardDescription>
            {industryMode === "revenue"
              ? "Leading NACE divisions ranked by reported revenue."
              : "Leading source industry groups ranked by company count."}
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {industries.length > 0 ? (
            <>
              {industryMode === "revenue" ? (
                <RevenueBarChart
                  items={industries.map((industry) => ({
                    key: "division" in industry ? industry.division : industry.code,
                    label: industry.label,
                    revenue_usd: "revenue_usd" in industry ? industry.revenue_usd : null,
                    href:
                      "division" in industry
                        ? `/financials/industry/${industry.division}?country=${countryCode}`
                        : undefined,
                  }))}
                />
              ) : null}
              <IndustryTable
                countryCode={countryCode}
                industries={industries}
                industryMode={industryMode}
              />
            </>
          ) : (
            <EmptyData
              title="No industry data"
              description="This registry does not currently expose a usable industry classification."
            />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Most valuable companies</CardTitle>
          <CardDescription>Companies ranked by latest reported revenue.</CardDescription>
        </CardHeader>
        <CardContent>
          {topCompanies.length > 0 ? (
            <TopCompaniesTable companies={topCompanies} showCountry={false} />
          ) : (
            <Empty className="min-h-52 border">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <Building2 />
                </EmptyMedia>
                <EmptyTitle>No ranked companies</EmptyTitle>
                <EmptyDescription>
                  Financial-company rankings are not available for this country yet.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
        </CardContent>
      </Card>
    </>
  );
}

function IndustryTable({
  countryCode,
  industries,
  industryMode,
}: {
  countryCode: string;
  industries: Route.ComponentProps["loaderData"]["industries"];
  industryMode: Route.ComponentProps["loaderData"]["industryMode"];
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Industry</TableHead>
          <TableHead className="text-right">Companies</TableHead>
          {industryMode === "revenue" ? (
            <TableHead className="text-right">Revenue (USD)</TableHead>
          ) : null}
        </TableRow>
      </TableHeader>
      <TableBody>
        {industries.map((industry) => {
          const isRevenue = "division" in industry;
          const code = isRevenue ? industry.division : industry.code;
          const href = isRevenue
            ? `/financials/industry/${code}?country=${countryCode}`
            : `/countries/${countryCode}/companies?f_industry=${encodeURIComponent(code)}`;
          return (
            <TableRow key={code}>
              <TableCell>
                <Link to={href} className="font-medium underline-offset-2 hover:underline">
                  {industry.label}
                </Link>
                <span className="text-muted-foreground ml-2 font-mono text-xs">{code}</span>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {nf.format(industry.companies)}
              </TableCell>
              {isRevenue ? (
                <TableCell className="text-right tabular-nums">
                  {formatRevenueUsd(industry.revenue_usd, null)}
                </TableCell>
              ) : null}
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

const EUROSTAT_METRIC_ORDER: EurostatMetricKey[] = [
  "activeEnterprises",
  "birthRate",
  "deathRate",
  "netGrowthRate",
  "oneYearSurvivalRate",
  "highGrowthShare",
];

function orderedEurostatMetrics<T extends { key: EurostatMetricKey }>(metrics: T[]): T[] {
  return [...metrics].sort(
    (a, b) =>
      EUROSTAT_METRIC_ORDER.indexOf(a.key) - EUROSTAT_METRIC_ORDER.indexOf(b.key),
  );
}

function formatEurostatStatus(status: string): string {
  if (!status) return "";
  const labels: Record<string, string> = {
    p: " · provisional",
    b: " · break in series",
    e: " · estimated",
  };
  return labels[status] ?? ` · status ${status}`;
}

function SourceLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="underline underline-offset-2 hover:text-foreground"
    >
      {children}
    </a>
  );
}

function parseTab(value: string | null): CountryTab {
  return COUNTRY_TABS.includes(value as CountryTab) ? (value as CountryTab) : "overview";
}
