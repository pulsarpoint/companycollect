import type { CountryTradeStatistics, CountryWorldBankSeries } from "~/lib/country-statistics";
import type { TopCompany } from "~/lib/financial-aggregates.server";
import type { TradedCompanyRow } from "~/lib/markets.server";
import { Link, useNavigate } from "react-router";
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
  tradedCompanies = [],
  year = null,
  availableYears = [],
}: {
  countryCode: string;
  worldBank: CountryWorldBankSeries[];
  trade: CountryTradeStatistics;
  industries: IndustryListItem[];
  industryMode: IndustryMode;
  topCompanies: TopCompany[];
  tradedCompanies?: TradedCompanyRow[];
  /** The year every card on this page is describing. */
  year?: number | null;
  availableYears?: number[];
}) {
  const latestYear = Math.max(
    ...worldBank.flatMap((series) => series.points.map((point) => point.year)),
  );
  const navigate = useNavigate();
  // Clicking the pulse chart re-runs the page for that year. In the URL, so the
  // view is linkable and the back button walks the years.
  const selectYear = (next: number) => {
    if (!availableYears.includes(next)) return;
    const latest = Math.max(...availableYears);
    navigate(next === latest ? "?" : `?year=${next}`, { preventScrollReset: true });
  };

  // Every card answers about the SELECTED year. Trade already carries all its
  // years, so it is filtered here rather than re-queried.
  const selectedTrade = year == null
    ? trade.latest
    : (trade.points.find((p) => p.year === year) ?? null);

  return (
    <>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Economic pulse</CardTitle>
            <CardDescription>
              Real growth, consumer-price inflation, and unemployment over the last
              decade.{availableYears.length > 0
                ? " Click a year to show the rest of this page for it."
                : ""}
            </CardDescription>
            <CardAction>
              <Badge variant="outline">World Bank</Badge>
            </CardAction>
          </CardHeader>
          <CardContent>
            <EconomicPulseChart
              series={worldBank}
              minYear={latestYear - 9}
              selectedYear={year}
              onSelectYear={availableYears.length > 0 ? selectYear : undefined}
            />
          </CardContent>
        </Card>

        <div className="grid gap-4">
          <Card size="sm">
            <CardHeader>
              <CardTitle>
                Trade snapshot{selectedTrade ? ` · ${selectedTrade.year}` : ""}
              </CardTitle>
              <CardDescription>
                Merchandise trade reported to UN Comtrade.
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <div className="grid grid-cols-2">
                <Metric
                  label="Exports"
                  value={
                    selectedTrade?.exportsUsd === null || selectedTrade?.exportsUsd === undefined
                      ? "—"
                      : compactUsd.format(selectedTrade.exportsUsd)
                  }
                  detail={selectedTrade ? String(selectedTrade.year) : "no data for this year"}
                />
                <Metric
                  label="Imports"
                  value={
                    selectedTrade?.importsUsd === null || selectedTrade?.importsUsd === undefined
                      ? "—"
                      : compactUsd.format(selectedTrade.importsUsd)
                  }
                  detail={selectedTrade ? String(selectedTrade.year) : "no data for this year"}
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
            <CardTitle>
              Leading industries{year != null && industryMode === "revenue" ? ` · ${year}` : ""}
            </CardTitle>
            <CardDescription>
              {industryMode === "revenue"
                ? `Top NACE divisions by revenue reported for ${year ?? "the latest filed year"}.`
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
            <CardTitle>
              Largest companies by revenue{year != null ? ` · ${year}` : ""}
            </CardTitle>
            <CardDescription>
              Revenue reported for {year ?? "the latest filed year"}, from filed
              standalone accounts — a different question from what a company is
              worth or how much its shares trade.
            </CardDescription>
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

        {/* The market answer to the same instinct, kept separate on purpose.
            Revenue and traded value are different quantities, and putting them
            in one card invites a comparison neither supports. Shown only where
            there are traded companies. */}
        {tradedCompanies.length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>Most traded companies</CardTitle>
              <CardDescription>
                Money changing hands in their shares, across every venue they
                trade on. Turnover, not market capitalisation.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ul className="flex flex-col gap-2">
                {tradedCompanies.map((row) => (
                  <li
                    key={row.company_id}
                    className="flex items-baseline justify-between gap-3 text-sm"
                  >
                    <Link
                      to={`/company/${countryCode}/${row.company_id}`}
                      className="truncate underline-offset-2 hover:underline"
                    >
                      {row.name || row.company_id}
                    </Link>
                    <span className="text-muted-foreground shrink-0 tabular-nums">
                      ${(row.tradedUsd / 1e9).toFixed(1)}bn
                    </span>
                  </li>
                ))}
              </ul>
              <Link
                to={`/countries/${countryCode}/markets`}
                className="text-muted-foreground mt-3 inline-block text-xs underline underline-offset-2"
              >
                All traded companies →
              </Link>
            </CardContent>
          </Card>
        ) : null}
      </div>
    </>
  );
}
