import { Building2 } from "lucide-react";
import type { CountryEurostatBusinessStats, EurostatMetricKey } from "~/lib/country-statistics";
import type { TopCompany } from "~/lib/financial-aggregates.server";
import { RevenueBarChart } from "~/components/financials/revenue-bar-chart";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Badge } from "~/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { BusinessSizeChart } from "~/components/countries/country-statistics-charts";
import {
  EmptyData,
  IndustryTable,
  Metric,
  nf,
  type IndustryListItem,
  type IndustryMode,
} from "~/components/country/shared";

export function BusinessTab({
  countryCode,
  countryName,
  eurostat,
  industries,
  industryMode,
  topCompanies,
}: {
  countryCode: string;
  countryName: string;
  eurostat: CountryEurostatBusinessStats;
  industries: IndustryListItem[];
  industryMode: IndustryMode;
  topCompanies: TopCompany[];
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
