import { Link } from "react-router";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";

import type { MarketOverview, TradedCompanyRow } from "~/lib/markets.server";
import { Metric } from "~/components/country/shared";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "~/components/ui/chart";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Badge } from "~/components/ui/badge";
import { Empty, EmptyDescription, EmptyTitle } from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US");
const money = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

/** Billions, because a month of Stockholm turnover runs to eleven digits. */
function usdBn(value: number): string {
  return `$${money.format(value / 1e9)}bn`;
}

function monthLabel(month: string): string {
  const [year, m] = month.split("-");
  return `${["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][Number(m)]} ${year.slice(2)}`;
}

/**
 * A country's traded companies and what changed hands.
 *
 * The figure is TRADED VALUE, never market capitalisation. Market cap needs
 * shares outstanding, which exists in this warehouse for Brazil alone, so
 * showing "value" here would be inventing a number. Turnover answers a
 * different and honest question: how much money moved.
 *
 * Converted to USD because a country's own companies do not trade in one
 * currency — Sweden's symbols are 819 SEK and 767 EUR, plus USD, NOK, CHF and
 * DKK — so a native-currency total would add unlike numbers.
 */
export function MarketsPanel({
  countryCode,
  overview,
  companies,
}: {
  countryCode: string;
  overview: MarketOverview | null;
  companies: TradedCompanyRow[];
}) {
  if (!overview) {
    return (
      <Empty>
        <EmptyTitle>No traded companies</EmptyTitle>
        <EmptyDescription>
          Listings are resolved through ESMA FIRDS and GLEIF, which requires an
          identity rule for this country. Only registers with such a rule can
          match an issuer to a company here.
        </EmptyDescription>
      </Empty>
    );
  }

  const chart = overview.perMonth.map((m) => ({
    month: monthLabel(m.month),
    traded: m.tradedUsd / 1e9,
    companies: m.companies,
  }));

  return (
    <div className="flex flex-col gap-4">
      <section
        aria-label="Market headline statistics"
        className="bg-muted/35 ring-foreground/10 grid grid-cols-1 gap-4 rounded-xl px-4 ring-1 sm:grid-cols-3"
      >
        <Metric
          label="Traded companies"
          value={nf.format(overview.companies)}
          detail={`${nf.format(overview.symbols)} symbols`}
        />
        <Metric
          label="Traded value"
          value={usdBn(overview.tradedUsd)}
          detail="price × volume, all venues"
        />
        <Metric
          label="Period"
          value={`${monthLabel(overview.firstDay)} – ${monthLabel(overview.lastDay)}`}
          detail={`${overview.perMonth.length} months`}
        />
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Traded value per month</CardTitle>
          <CardDescription>
            How much money changed hands in these companies' shares, in USD,
            summed across every venue where they trade. This is turnover — not
            what the companies are worth. Market capitalisation needs shares
            outstanding, which this warehouse does not hold for {countryCode.toUpperCase()}.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer
            config={{ traded: { label: "Traded value ($bn)", color: "var(--chart-1)" } }}
            className="h-64 w-full"
          >
            <BarChart data={chart} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid vertical={false} strokeDasharray="3 3" />
              <XAxis
                dataKey="month"
                tickLine={false}
                axisLine={false}
                tick={{ fontSize: 11 }}
                interval="preserveStartEnd"
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                width={44}
                tick={{ fontSize: 11 }}
                tickFormatter={(v: number) => `$${v.toFixed(0)}bn`}
              />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Bar dataKey="traded" fill="var(--color-traded)" radius={2} />
            </BarChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Companies</CardTitle>
          <CardDescription>
            Ranked by traded value, because a share price is not a size — a
            company quoted at 2,000 is not bigger than one quoted at 20.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table className="min-w-[48rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Company</TableHead>
                  <TableHead>Tickers</TableHead>
                  <TableHead className="text-right">Venues</TableHead>
                  <TableHead className="text-right">Last close</TableHead>
                  <TableHead className="text-right">Traded value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {companies.map((row) => (
                  <TableRow key={row.company_id}>
                    <TableCell className="align-top">
                      <Link
                        to={`/company/${countryCode}/${row.company_id}`}
                        className="underline underline-offset-2"
                      >
                        {row.name || row.company_id}
                      </Link>
                      <div className="text-muted-foreground text-xs tabular-nums">
                        {row.company_id}
                      </div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex max-w-[18rem] flex-wrap gap-1">
                        {row.tickers.slice(0, 6).map((t) => (
                          <Badge key={t} variant="outline" className="font-mono text-[10px]">
                            {t}
                          </Badge>
                        ))}
                        {row.tickers.length > 6 ? (
                          <span className="text-muted-foreground text-xs">
                            +{row.tickers.length - 6}
                          </span>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell className="text-right align-top tabular-nums">
                      {nf.format(row.venues)}
                    </TableCell>
                    <TableCell className="text-right align-top tabular-nums">
                      {row.lastClose == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <>
                          {money.format(row.lastClose)}
                          {row.currency ? ` ${row.currency}` : ""}
                          <div className="text-muted-foreground text-xs">
                            {row.leadVenue} · {row.lastDay}
                          </div>
                        </>
                      )}
                    </TableCell>
                    <TableCell className="text-right align-top tabular-nums">
                      {usdBn(row.tradedUsd)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
