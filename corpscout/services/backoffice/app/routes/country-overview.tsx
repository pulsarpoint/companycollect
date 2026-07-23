import { Link } from "react-router";
import { ArrowLeft, ArrowRight, Building2, ChartNoAxesColumnIncreasing } from "lucide-react";
import type { Route } from "./+types/country-overview";
import { getCountry } from "~/lib/countries";
import {
  getCountryDirectory,
  getCountryIndustryGroups,
} from "~/lib/countries-overview.server";
import {
  getCountryFinancials,
  TOP_DIVISIONS_LIMIT,
} from "~/lib/financial-aggregates.server";
import { getCountryMacroIndicators } from "~/lib/world-bank.server";
import { formatRevenueUsd } from "~/components/data-table/unified-columns";
import { MethodologyNote } from "~/components/financials/methodology-note";
import { RevenueBarChart } from "~/components/financials/revenue-bar-chart";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const [directory, financials, coverageIndustries, macro] = await Promise.all([
    getCountryDirectory(),
    getCountryFinancials(country.code),
    getCountryIndustryGroups(country.code),
    getCountryMacroIndicators(country.code),
  ]);
  const summary = directory.find((row) => row.country_code === country.code);
  if (!summary) throw new Response("Country data not found", { status: 404 });

  const revenueIndustries = financials?.divisions?.slice(0, TOP_DIVISIONS_LIMIT) ?? null;
  return {
    summary,
    macro,
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
    <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
      <Skeleton className="h-12 w-64" />
      <Skeleton className="h-44" />
      <Skeleton className="h-44" />
      <Skeleton className="h-80" />
    </div>
  );
}

const nf = new Intl.NumberFormat("en-US");
const macroUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 2,
});

function Metric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="min-w-0 px-4 py-3 first:pl-0 last:pr-0 sm:border-l sm:first:border-l-0">
      <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">{label}</p>
      <p className="mt-1 truncate text-xl font-semibold tabular-nums" title={value}>
        {value}
      </p>
      {detail ? <p className="text-muted-foreground mt-1 text-xs">{detail}</p> : null}
    </div>
  );
}

export default function CountryOverview({ loaderData, params }: Route.ComponentProps) {
  const { summary, macro, industries, industryMode, topCompanies } = loaderData;
  const country = getCountry(params.country)!;
  const coverage =
    summary.total_companies > 0
      ? (summary.companies_with_financials / summary.total_companies) * 100
      : 0;

  const macroItems = [
    { label: "GDP", value: macro.gdp },
    { label: "Exports", value: macro.exports },
    { label: "Imports", value: macro.imports },
  ];

  return (
    <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
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

      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="flex items-center gap-3 text-3xl font-semibold tracking-tight">
            <span aria-hidden>{country.flag}</span>
            <span>{country.name}</span>
          </h1>
          <p className="text-muted-foreground mt-1 text-sm uppercase tracking-wide">
            {country.code} company intelligence
          </p>
        </div>
        <Button nativeButton={false} render={<Link to={`/countries/${country.code}/companies`} />}>
          View companies
          <ArrowRight data-icon="inline-end" />
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Company coverage</CardTitle>
          <CardDescription>Registry records and latest reported financial data.</CardDescription>
        </CardHeader>
        <CardContent className="grid sm:grid-cols-2 lg:grid-cols-4">
          <Metric label="All companies" value={nf.format(summary.total_companies)} />
          <Metric
            label="With financials"
            value={nf.format(summary.companies_with_financials)}
            detail={`${coverage.toFixed(1)}% coverage`}
          />
          <Metric label="Reported revenue" value={formatRevenueUsd(summary.revenue_usd, null)} />
          <Metric label="Latest fiscal year" value={String(summary.latest_fiscal_year ?? "—")} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Economic indicators</CardTitle>
          <CardDescription>Latest available current-US-dollar observations.</CardDescription>
        </CardHeader>
        <CardContent className="grid sm:grid-cols-3">
          {macroItems.map((item) => (
            <Metric
              key={item.label}
              label={item.label}
              value={item.value ? macroUsd.format(item.value.value) : "Unavailable"}
              detail={item.value ? `World Bank · ${item.value.year}` : "World Bank data unavailable"}
            />
          ))}
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
                        ? `/financials/industry/${industry.division}`
                        : undefined,
                  }))}
                />
              ) : null}
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
                      ? `/financials/industry/${code}`
                      : `/countries/${country.code}/companies?f_industry=${encodeURIComponent(code)}`;
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
            </>
          ) : (
            <Empty className="min-h-52 border">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <ChartNoAxesColumnIncreasing />
                </EmptyMedia>
                <EmptyTitle>No industry data</EmptyTitle>
                <EmptyDescription>
                  This registry does not currently expose a usable industry classification.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
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

      <div className="flex flex-col gap-1">
        <MethodologyNote />
        <p className="text-muted-foreground text-xs">
          GDP, exports, and imports are sourced from the{" "}
          <a
            href="https://data.worldbank.org/"
            target="_blank"
            rel="noreferrer"
            className="underline underline-offset-2 hover:text-foreground"
          >
            World Bank
          </a>
          ; each indicator shows its own latest available year.
        </p>
      </div>
    </div>
  );
}
