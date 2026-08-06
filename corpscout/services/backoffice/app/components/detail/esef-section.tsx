import { Link } from "react-router";
import { ChevronRight, ExternalLink } from "lucide-react";
import type { EsefFilingRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
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
import { EvidencePanel } from "~/components/detail/evidence";

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

function Amount({
  original,
  usd,
  currency,
}: {
  original: number | null;
  usd: number | null;
  currency: string;
}) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      {original == null ? null : (
        <span className="font-mono tabular-nums">
          {compactNumber.format(original)} {currency}
        </span>
      )}
      {usd == null ? null : (
        <span className="text-muted-foreground font-mono text-xs tabular-nums">
          {compactUsd.format(usd)}
        </span>
      )}
    </div>
  );
}

function EmployeeCount({ value }: { value: number | null }) {
  return value == null ? (
    <span className="text-muted-foreground">—</span>
  ) : (
    <span className="font-mono tabular-nums">
      {compactNumber.format(value)}
    </span>
  );
}

/**
 * Consolidated IFRS figures a company reported in its ESEF annual report, one
 * row per fiscal year, each linking back to the filing it came from.
 *
 * These are group accounts, not the standalone legal-entity accounts shown in
 * the registry financials section — the two answer different questions and are
 * deliberately not merged.
 */
export function EsefSection({
  filings,
  detailsHref,
  title = "Financials · consolidated IFRS",
  description = "Consolidated IFRS figures extracted from the company's filed ESEF reports. Group accounts remain distinct from standalone legal-entity filings.",
}: {
  filings: EsefFilingRow[];
  detailsHref?: (filing: EsefFilingRow) => string;
  title?: string;
  description?: string;
}) {
  if (filings.length === 0) return null;
  const hasOfficialSwedenSource = filings.some((filing) =>
    filing.source_url.toLowerCase().includes("bolagsverket.se"),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>
          {description} Source currency is shown first, with the period-end USD
          conversion below.
          {hasOfficialSwedenSource
            ? " Bolagsverket is the preferred source; the xbrl.org viewer is retained when available."
            : ""}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table className="min-w-[78rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Operating profit</TableHead>
                <TableHead className="text-right">Profit / loss</TableHead>
                <TableHead className="text-right">Total assets</TableHead>
                <TableHead className="text-right">Equity</TableHead>
                <TableHead className="text-right">Liabilities</TableHead>
                <TableHead className="text-right">Cash</TableHead>
                <TableHead className="text-right">Employees</TableHead>
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
                    <div className="text-muted-foreground mt-1 text-xs tabular-nums">
                      {filing.source_fact_count.toLocaleString("en-US")} current
                      facts · {filing.mapped_fact_count.toLocaleString("en-US")}{" "}
                      standardized
                    </div>
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.revenue_amount_original}
                      usd={filing.revenue_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.operating_profit_amount_original}
                      usd={filing.operating_profit_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.profit_loss_amount_original}
                      usd={filing.profit_loss_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.total_assets_amount_original}
                      usd={filing.total_assets_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.equity_amount_original}
                      usd={filing.equity_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.liabilities_amount_original}
                      usd={filing.liabilities_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <Amount
                      original={filing.cash_amount_original}
                      usd={filing.cash_amount_usd}
                      currency={filing.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right">
                    <EmployeeCount value={filing.employees} />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col items-start gap-1">
                      {(() => {
                        const isBolagsverket = filing.source_url
                          .toLowerCase()
                          .includes("bolagsverket.se");
                        const primaryUrl = isBolagsverket
                          ? filing.source_url
                          : filing.viewer_url ||
                            filing.package_url ||
                            filing.source_url;
                        if (!primaryUrl) {
                          return (
                            <span className="text-muted-foreground">—</span>
                          );
                        }
                        return (
                          <a
                            href={primaryUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center gap-1 text-sm underline underline-offset-4"
                          >
                            {isBolagsverket ? "Bolagsverket" : "xbrl.org"}
                            <ExternalLink className="size-3" />
                          </a>
                        );
                      })()}
                      {filing.viewer_url &&
                      filing.source_url
                        .toLowerCase()
                        .includes("bolagsverket.se") &&
                      filing.source_url !== filing.viewer_url ? (
                        <a
                          href={filing.viewer_url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-muted-foreground inline-flex items-center gap-1 text-xs underline underline-offset-4"
                        >
                          Viewer
                          <ExternalLink className="size-3" />
                        </a>
                      ) : null}
                      {detailsHref && filing.primary_fxo_id ? (
                        <Button
                          variant="outline"
                          size="sm"
                          nativeButton={false}
                          render={<Link to={detailsHref(filing)} />}
                        >
                          All facts
                          <ChevronRight data-icon="inline-end" />
                        </Button>
                      ) : null}
                      <EvidencePanel evidence={filing.evidence ?? []} />
                    </div>
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
