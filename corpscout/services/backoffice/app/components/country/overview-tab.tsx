import type { CountryTradeStatistics, CountryWorldBankSeries } from "~/lib/country-statistics";
import type { TopCompany } from "~/lib/financial-aggregates.server";
import { TopCompaniesTable } from "~/components/financials/top-companies-table";
import { Badge } from "~/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  EconomicPulseChart,
  TradeSnapshotChart,
} from "~/components/countries/country-statistics-charts";
import {
  EmptyData,
  IndustryTable,
  Metric,
  compactUsd,
  type IndustryListItem,
  type IndustryMode,
} from "~/components/country/shared";

export function OverviewTab({
  countryCode,
  worldBank,
  trade,
  industries,
  industryMode,
  topCompanies,
}: {
  countryCode: string;
  worldBank: CountryWorldBankSeries[];
  trade: CountryTradeStatistics;
  industries: IndustryListItem[];
  industryMode: IndustryMode;
  topCompanies: TopCompany[];
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
            <CardContent className="flex flex-col gap-4">
              <div className="grid grid-cols-2">
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
              </div>
              <TradeSnapshotChart points={trade.points} />
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
