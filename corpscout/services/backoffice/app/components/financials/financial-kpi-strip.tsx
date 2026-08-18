import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import type { FinancialLocale } from "~/components/financials/copy";
import {
  formatDecimal,
  formatSignedPercentage,
  formatSignedPoints,
  moneyPairLines,
} from "~/components/financials/formatters";
import {
  calculationValue,
  financialMoney,
  percentage,
  percentageChange,
} from "~/components/financials/metrics";
import type { FinancialYearRow } from "~/lib/queries.server";

type KpiCopy = {
  revenue: string;
  operatingResult: string;
  netResult: string;
  equityRatio: string;
  yearOverYear: string;
  margin: string;
  points: string;
  unavailable: string;
};

export function FinancialKpiStrip({
  latest,
  previous,
  locale,
  copy,
}: {
  latest: FinancialYearRow;
  previous?: FinancialYearRow;
  locale: FinancialLocale;
  copy: KpiCopy;
}) {
  const revenue = calculationValue(latest, "revenue");
  const operatingResult = calculationValue(latest, "operatingResult");
  const netResult = calculationValue(latest, "netResult");
  const equityRatio = percentage(
    calculationValue(latest, "equity"),
    calculationValue(latest, "totalAssets"),
  );
  const previousEquityRatio = previous
    ? percentage(
        calculationValue(previous, "equity"),
        calculationValue(previous, "totalAssets"),
      )
    : null;
  const operatingMargin = percentage(operatingResult, revenue);
  const revenueChange = previous
    ? percentageChange(revenue, calculationValue(previous, "revenue"))
    : null;
  const resultChange = previous
    ? percentageChange(netResult, calculationValue(previous, "netResult"))
    : null;

  const kpis = [
    {
      label: copy.revenue,
      pair: financialMoney(latest, "revenue"),
      percentage: null,
      detail:
        revenueChange === null
          ? copy.unavailable
          : `${formatSignedPercentage(revenueChange, locale)} ${copy.yearOverYear}`,
    },
    {
      label: copy.operatingResult,
      pair: financialMoney(latest, "operatingResult"),
      percentage: null,
      detail:
        operatingMargin === null
          ? copy.unavailable
          : `${formatDecimal(operatingMargin, locale)}% ${copy.margin}`,
    },
    {
      label: copy.netResult,
      pair: financialMoney(latest, "netResult"),
      percentage: null,
      detail:
        resultChange === null
          ? copy.unavailable
          : `${formatSignedPercentage(resultChange, locale)} ${copy.yearOverYear}`,
    },
    {
      label: copy.equityRatio,
      pair: null,
      percentage: equityRatio,
      detail:
        equityRatio === null || previousEquityRatio === null
          ? copy.unavailable
          : formatSignedPoints(
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
        {kpis.map((kpi) => {
          const pair = kpi.pair ? moneyPairLines(kpi.pair, locale) : null;
          return (
            <div key={kpi.label} className="flex min-w-0 flex-col gap-2">
              <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
                {kpi.label}
              </p>
              {pair ? (
                <div className="flex min-w-0 flex-col gap-0.5 tabular-nums">
                  <p className="truncate text-2xl font-semibold tracking-tight">
                    {pair.original ?? "—"}
                  </p>
                  <p className="text-muted-foreground truncate text-sm font-medium">
                    {pair.usd ?? "—"}
                  </p>
                </div>
              ) : (
                <p className="truncate text-2xl font-semibold tracking-tight tabular-nums">
                  {kpi.percentage === null
                    ? "—"
                    : `${formatDecimal(kpi.percentage, locale)}%`}
                </p>
              )}
              <Badge variant="outline">{kpi.detail}</Badge>
            </div>
          );
        })}
      </div>
      <Separator />
    </div>
  );
}
