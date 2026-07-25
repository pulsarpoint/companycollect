import { ExternalLink } from "lucide-react";
import type { EsefFilingRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const compactUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 2,
});

function Amount({ value }: { value: number | null }) {
  if (value === null || value === undefined) {
    return <span className="text-muted-foreground">—</span>;
  }
  return <span className="font-mono tabular-nums">{compactUsd.format(value)}</span>;
}

/**
 * Consolidated IFRS figures a company reported in its ESEF annual report, one
 * row per fiscal year, each linking back to the filing it came from.
 *
 * These are group accounts, not the standalone legal-entity accounts shown in
 * the registry financials section — the two answer different questions and are
 * deliberately not merged.
 */
export function EsefSection({ filings }: { filings: EsefFilingRow[] }) {
  if (filings.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle>ESEF annual reports</CardTitle>
        <CardDescription>
          Consolidated IFRS figures extracted from the company&apos;s filed ESEF
          reports. Group accounts, distinct from the standalone registry
          filings above. Amounts converted to USD at each period-end rate.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Operating profit</TableHead>
                <TableHead className="text-right">Profit / loss</TableHead>
                <TableHead className="text-right">Total assets</TableHead>
                <TableHead className="text-right">Equity</TableHead>
                <TableHead>Source</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filings.map((filing) => (
                <TableRow key={`${filing.fiscal_year}-${filing.period_end}`}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <span className="font-medium">{filing.fiscal_year}</span>
                      {filing.composed_from_amendment === 1 ? (
                        <Badge
                          variant="outline"
                          title="Some figures come from an earlier version of this filing, because the amendment did not re-report them."
                        >
                          amended
                        </Badge>
                      ) : null}
                    </div>
                    <div className="text-muted-foreground text-xs">
                      to {filing.period_end}
                      {filing.currency ? ` · ${filing.currency}` : ""}
                    </div>
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount value={filing.revenue_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount value={filing.operating_profit_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount value={filing.profit_loss_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount value={filing.total_assets_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount value={filing.equity_amount_usd} />
                  </TableCell>
                  <TableCell>
                    {filing.viewer_url ? (
                      <a
                        href={filing.viewer_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-sm underline underline-offset-4"
                      >
                        Report
                        <ExternalLink className="size-3" />
                      </a>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
