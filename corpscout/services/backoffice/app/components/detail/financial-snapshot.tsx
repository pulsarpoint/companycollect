import { Link } from "react-router";
import { ArrowRight } from "lucide-react";
import type { CompanyFinancialSource } from "~/lib/queries.server";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function metricValue(value: number | null, currency: string): string {
  return value === null
    ? "—"
    : `${compactNumber.format(value)} ${currency}`.trim();
}

export function FinancialSnapshot({
  sources,
  href,
}: {
  sources: CompanyFinancialSource[];
  href: string;
}) {
  const summaries = sources.map((source) => {
    if (source.kind === "registry") {
      const latest = source.financials.find((row) =>
        [
          row.revenue_amount_original,
          row.revenue_amount_usd,
          row.net_result_amount_original,
          row.net_result_amount_usd,
          row.total_assets_amount_original,
          row.total_assets_amount_usd,
          row.equity_amount_original,
          row.equity_amount_usd,
          row.employees,
        ].some((value) => value !== null),
      );
      const documentCount = Math.max(
        source.financials.length,
        source.documents.length,
      );
      return {
        id: source.id,
        label: source.title,
        period: latest
          ? `${latest.fiscal_year} · ${latest.currency}`
          : `${documentCount} filed ${documentCount === 1 ? "document" : "documents"}`,
        summary: latest
          ? `Revenue ${metricValue(latest.revenue_amount_original, latest.currency)} · assets ${metricValue(latest.total_assets_amount_original, latest.currency)}`
          : "No standardized financial values in these filings",
      };
    }
    const latest = source.filings.find((row) =>
      [
        row.revenue_amount_original,
        row.profit_loss_amount_original,
        row.total_assets_amount_original,
        row.equity_amount_original,
      ].some((value) => value !== null),
    );
    return {
      id: source.id,
      label: source.title,
      period: latest
        ? `${latest.fiscal_year} · ${latest.currency}`
        : `${source.filings.length} filed ${source.filings.length === 1 ? "report" : "reports"}`,
      summary: latest
        ? `Revenue ${metricValue(latest.revenue_amount_original, latest.currency)} · assets ${metricValue(latest.total_assets_amount_original, latest.currency)}`
        : "No standardized financial values in these reports",
    };
  });

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-4">
        <div>
          <CardTitle className="text-base">Financial sources</CardTitle>
          <p className="text-muted-foreground mt-1 text-sm">
            Each source and accounting scope remains separate.
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          nativeButton={false}
          render={<Link to={href} />}
        >
          View financials
          <ArrowRight data-icon="inline-end" />
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {summaries.length > 0 ? (
          <dl className="grid gap-4 md:grid-cols-2">
            {summaries.map((source) => (
              <div key={source.id} className="border-l pl-3">
                <dt className="font-medium">{source.label}</dt>
                <dd className="text-muted-foreground mt-1 text-xs tabular-nums">
                  {source.period}
                </dd>
                <dd className="mt-2 text-sm tabular-nums">{source.summary}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="text-muted-foreground text-sm">
            No financial source records are connected yet.
          </p>
        )}
        <p className="text-muted-foreground text-xs">
          Open Financials for complete source histories, tagged report facts,
          and evidence.
        </p>
      </CardContent>
    </Card>
  );
}
