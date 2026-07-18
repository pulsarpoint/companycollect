import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";
import type { FinancialYearRow } from "~/lib/queries.server";
import {
  Card,
  CardContent,
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

const chartConfig = {
  revenue: { label: "Revenue (USD)", color: "var(--chart-1)" },
  result: { label: "Net result (USD)", color: "var(--chart-2)" },
} satisfies ChartConfig;

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function money(v: number | null) {
  return v == null ? <span className="text-muted-foreground">—</span> : nf.format(v);
}

/** Original value on top, USD equivalent muted beneath — both or whichever exists. */
function MoneyPair({ original, usd }: { original: number | null; usd: number | null }) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      <span>{money(original)}</span>
      <span className="text-muted-foreground text-xs">
        {usd == null ? "—" : `$${nf.format(usd)}`}
      </span>
    </div>
  );
}

export function FinancialsSection({ financials }: { financials: FinancialYearRow[] }) {
  if (financials.length === 0) return null;
  // Chart wants oldest → newest, left to right.
  const chartData = [...financials]
    .reverse()
    .map((f) => ({ year: f.fiscal_year, revenue: f.revenue_amount_usd, result: f.net_result_amount_usd }));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financials</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <ChartContainer config={chartConfig} className="h-56 w-full">
          <BarChart data={chartData}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="year" tickLine={false} axisLine={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Bar dataKey="revenue" fill="var(--color-revenue)" radius={3} />
            <Bar dataKey="result" fill="var(--color-result)" radius={3} />
          </BarChart>
        </ChartContainer>

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
              </TableRow>
            </TableHeader>
            <TableBody>
              {financials.map((f) => (
                <TableRow key={f.fiscal_year}>
                  <TableCell className="tabular-nums align-top">{f.fiscal_year}</TableCell>
                  <TableCell className="align-top">{f.currency}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={f.revenue_amount_original} usd={f.revenue_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={f.net_result_amount_original} usd={f.net_result_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={f.total_assets_amount_original} usd={f.total_assets_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={f.equity_amount_original} usd={f.equity_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums align-top">{money(f.employees)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
