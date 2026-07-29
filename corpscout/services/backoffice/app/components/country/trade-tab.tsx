import type { CountryTradeStatistics, CountryWorldBankSeries } from "~/lib/country-statistics";
import { WORLD_BANK_INDICATORS } from "~/lib/country-statistics";
import { Badge } from "~/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { TradeHistoryChart } from "~/components/countries/country-statistics-charts";
import { EmptyData, Metric, compactUsd, getWorldBankSeries } from "~/components/country/shared";

export function TradeTab({
  worldBank,
  trade,
}: {
  worldBank: CountryWorldBankSeries[];
  trade: CountryTradeStatistics;
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
