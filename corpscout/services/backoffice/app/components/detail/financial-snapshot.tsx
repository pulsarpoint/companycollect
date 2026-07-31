import { Link } from "react-router";
import { ArrowRight } from "lucide-react";
import type { FinancialYearRow } from "~/lib/queries.server";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function metricValue(value: number | null, currency: string): string {
  return value === null ? "—" : `${compactNumber.format(value)} ${currency}`.trim();
}

export function FinancialSnapshot({
  financials,
  href,
}: {
  financials: FinancialYearRow[];
  href: string;
}) {
  const latest = financials[0];
  const metrics = latest
    ? [
        ["Operating revenue", latest.revenue_amount_original],
        ["Net result", latest.net_result_amount_original],
        ["Total assets", latest.total_assets_amount_original],
        ["Equity", latest.equity_amount_original],
      ]
    : [];

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-4">
        <div>
          <CardTitle className="text-base">Financial snapshot</CardTitle>
          <p className="text-muted-foreground mt-1 text-sm">
            {latest ? `Latest registry figures · ${latest.fiscal_year}` : "Annual account records"}
          </p>
        </div>
        <Button variant="outline" size="sm" nativeButton={false} render={<Link to={href} />}>
          View financials
          <ArrowRight data-icon="inline-end" />
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {latest ? (
          <dl className="grid grid-cols-2 gap-x-6 gap-y-4 lg:grid-cols-4">
            {metrics.map(([label, value]) => (
              <div key={String(label)} className="border-l pl-3">
                <dt className="text-muted-foreground text-xs">{label}</dt>
                <dd className="mt-1 text-sm font-semibold tabular-nums">
                  {metricValue(value as number | null, latest.currency)}
                </dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="text-muted-foreground text-sm">
            No normalized registry figures are available yet.
          </p>
        )}
        <p className="text-muted-foreground text-xs">
          Open Financials for annual report PDFs, extraction details, and source-faithful report
          facts.
        </p>
      </CardContent>
    </Card>
  );
}
