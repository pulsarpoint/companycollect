import type { ReactNode } from "react";
import { Link } from "react-router";
import { ChevronRight, Info } from "lucide-react";
import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";
import type { FinancialYearRow } from "~/lib/queries.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { EvidencePanel } from "~/components/detail/evidence";

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function money(v: number | null) {
  return v == null ? (
    <span className="text-muted-foreground">—</span>
  ) : (
    nf.format(v)
  );
}

/** Original value on top, USD equivalent muted beneath — both or whichever exists. */
function MoneyPair({
  original,
  usd,
}: {
  original: number | null;
  usd: number | null;
}) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      {original === null ? null : <span>{money(original)}</span>}
      {usd === null ? null : (
        <span className="text-muted-foreground text-xs">${nf.format(usd)}</span>
      )}
    </div>
  );
}

function hasFinancialData(financial: FinancialYearRow): boolean {
  return [
    financial.revenue_amount_original,
    financial.revenue_amount_usd,
    financial.net_result_amount_original,
    financial.net_result_amount_usd,
    financial.total_assets_amount_original,
    financial.total_assets_amount_usd,
    financial.equity_amount_original,
    financial.equity_amount_usd,
    financial.employees,
  ].some((value) => value !== null);
}

function hasChartData(financial: FinancialYearRow): boolean {
  return [
    financial.revenue_amount_original,
    financial.revenue_amount_usd,
    financial.net_result_amount_original,
    financial.net_result_amount_usd,
  ].some((value) => value !== null);
}

function UnavailableFinancialYears({
  financials,
  factsHref,
}: {
  financials: FinancialYearRow[];
  factsHref?: (fiscalYear: string) => string;
}) {
  if (financials.length === 0) return null;

  return (
    <Alert>
      <Info />
      <AlertTitle>Filed documents without financial data</AlertTitle>
      <AlertDescription className="flex flex-col gap-2">
        <p>
          These filings do not contain machine-readable financial values. They
          remain available as source evidence.
        </p>
        <ul className="flex flex-col gap-1">
          {financials.map((financial) => (
            <li
              key={financial.fiscal_year}
              className="flex flex-wrap items-center gap-2"
            >
              <span>
                No financial data available for {financial.fiscal_year}.
              </span>
              {factsHref && financial.observation !== "comparative" ? (
                <Link to={factsHref(financial.fiscal_year)}>
                  View filing facts
                </Link>
              ) : null}
              <EvidencePanel evidence={financial.evidence ?? []} />
            </li>
          ))}
        </ul>
      </AlertDescription>
    </Alert>
  );
}

export function FinancialsSection({
  financials,
  factsHref,
  title = "Financials",
  description,
  children,
}: {
  financials: FinancialYearRow[];
  /** When set, year cells link to the raw source facts for that filing. */
  factsHref?: (fiscalYear: string) => string;
  title?: string;
  description?: string;
  children?: ReactNode;
}) {
  if (financials.length === 0 && children == null) return null;
  const financialRows = financials.filter(hasFinancialData);
  const unavailableRows = financials.filter(
    (financial) => !hasFinancialData(financial),
  );
  const chartRows = financialRows.filter(hasChartData);
  const chartUsesUsd = chartRows.some(
    (row) =>
      row.revenue_amount_usd !== null || row.net_result_amount_usd !== null,
  );
  const chartCurrency = chartUsesUsd
    ? "USD"
    : chartRows[0]?.currency || "original currency";
  const chartConfig = {
    revenue: { label: `Revenue (${chartCurrency})`, color: "var(--chart-1)" },
    result: { label: `Net result (${chartCurrency})`, color: "var(--chart-2)" },
  } satisfies ChartConfig;
  // Chart wants oldest → newest, left to right.
  const chartData = [...chartRows].reverse().map((f) => ({
    year: f.fiscal_year,
    revenue: chartUsesUsd ? f.revenue_amount_usd : f.revenue_amount_original,
    result: chartUsesUsd
      ? f.net_result_amount_usd
      : f.net_result_amount_original,
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
        {description ? <CardDescription>{description}</CardDescription> : null}
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <UnavailableFinancialYears
          financials={unavailableRows}
          factsHref={factsHref}
        />
        {chartRows.length < 2 ? null : (
          <ChartContainer config={chartConfig} className="h-56 w-full">
            <BarChart data={chartData}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="year" tickLine={false} axisLine={false} />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Bar dataKey="revenue" fill="var(--color-revenue)" radius={3} />
              <Bar dataKey="result" fill="var(--color-result)" radius={3} />
            </BarChart>
          </ChartContainer>
        )}

        {financialRows.length === 0 ? null : (
          <div className="overflow-x-auto">
            <Table className="min-w-[40rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Year</TableHead>
                  <TableHead>Currency</TableHead>
                  <TableHead className="text-right">Revenue</TableHead>
                  <TableHead className="text-right">Net result</TableHead>
                  <TableHead className="text-right">Total assets</TableHead>
                  <TableHead className="text-right">Equity</TableHead>
                  <TableHead className="text-right">Employees</TableHead>
                  {factsHref ? (
                    <TableHead className="text-right">Report</TableHead>
                  ) : null}
                </TableRow>
              </TableHeader>
              <TableBody>
                {financialRows.map((f) => (
                  <TableRow key={f.fiscal_year}>
                    <TableCell className="tabular-nums align-top">
                      {f.fiscal_year}
                      {f.observation === "comparative" ? (
                        <div className="text-muted-foreground text-xs whitespace-nowrap">
                          from {f.source_fiscal_year} filing
                        </div>
                      ) : null}
                      <EvidencePanel evidence={f.evidence ?? []} />
                    </TableCell>
                    <TableCell className="align-top">{f.currency}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      <MoneyPair
                        original={f.revenue_amount_original}
                        usd={f.revenue_amount_usd}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <MoneyPair
                        original={f.net_result_amount_original}
                        usd={f.net_result_amount_usd}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <MoneyPair
                        original={f.total_assets_amount_original}
                        usd={f.total_assets_amount_usd}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <MoneyPair
                        original={f.equity_amount_original}
                        usd={f.equity_amount_usd}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums align-top">
                      {money(f.employees)}
                    </TableCell>
                    {factsHref ? (
                      <TableCell className="text-right align-top">
                        {f.observation === "comparative" ? (
                          <span className="text-muted-foreground text-xs">
                            Carried forward
                          </span>
                        ) : (
                          <Button
                            variant="outline"
                            size="sm"
                            nativeButton={false}
                            render={<Link to={factsHref(f.fiscal_year)} />}
                          >
                            All facts
                            <ChevronRight data-icon="inline-end" />
                          </Button>
                        )}
                      </TableCell>
                    ) : null}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
        {children}
      </CardContent>
    </Card>
  );
}
