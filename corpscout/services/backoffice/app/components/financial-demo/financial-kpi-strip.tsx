import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import type { FinancialDemoPoint } from "~/components/financial-demo/data";
import {
  formatDecimal,
  formatMoneyPair,
  formatSignedPercentage,
  formatSignedPoints,
  type FinancialDemoLocale,
} from "~/components/financial-demo/formatters";

function change(current: number, previous: number): number {
  return ((current - previous) / Math.abs(previous)) * 100;
}

export function FinancialKpiStrip({
  latest,
  previous,
  locale,
  copy,
}: {
  latest: FinancialDemoPoint;
  previous: FinancialDemoPoint;
  locale: FinancialDemoLocale;
  copy: {
    netTurnover: string;
    operatingResult: string;
    netResult: string;
    equityRatio: string;
    yearOverYear: string;
    margin: string;
    points: string;
  };
}) {
  const equityRatio = (latest.equity / latest.totalAssets) * 100;
  const previousEquityRatio = (previous.equity / previous.totalAssets) * 100;
  const operatingMargin = (latest.operatingResult / latest.revenue) * 100;
  const kpis = [
    {
      label: copy.netTurnover,
      value: latest.revenue,
      format: "money" as const,
      change: `${formatSignedPercentage(change(latest.revenue, previous.revenue), locale)} ${copy.yearOverYear}`,
    },
    {
      label: copy.operatingResult,
      value: latest.operatingResult,
      format: "money" as const,
      change: `${formatDecimal(operatingMargin, locale)}% ${copy.margin}`,
    },
    {
      label: copy.netResult,
      value: latest.netResult,
      format: "money" as const,
      change: `${formatSignedPercentage(change(latest.netResult, previous.netResult), locale)} ${copy.yearOverYear}`,
    },
    {
      label: copy.equityRatio,
      value: equityRatio,
      format: "percentage" as const,
      change: formatSignedPoints(
        equityRatio - previousEquityRatio,
        locale,
        copy.points,
      ),
    },
  ];

  return (
    <div className="flex flex-col gap-5">
      <Separator />
      <div className="grid grid-cols-2 gap-x-6 gap-y-8 lg:grid-cols-4">
        {kpis.map((kpi) => (
          <div key={kpi.label} className="flex min-w-0 flex-col gap-2">
            <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
              {kpi.label}
            </p>
            {kpi.format === "money" ? (
              <div className="flex min-w-0 flex-col gap-0.5 tabular-nums">
                <p className="truncate text-2xl font-semibold tracking-tight">
                  {formatMoneyPair(kpi.value, locale).sek}
                </p>
                <p className="text-muted-foreground truncate text-sm font-medium">
                  {formatMoneyPair(kpi.value, locale).usd}
                </p>
              </div>
            ) : (
              <p className="truncate text-2xl font-semibold tracking-tight tabular-nums">
                {formatDecimal(kpi.value, locale)}%
              </p>
            )}
            <Badge variant="outline">{kpi.change}</Badge>
          </div>
        ))}
      </div>
      <Separator />
    </div>
  );
}
