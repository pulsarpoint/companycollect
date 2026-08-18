import { useState } from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import type { FinancialLocale } from "~/components/financials/copy";
import {
  formatCompactMoney,
  moneyPairLines,
} from "~/components/financials/formatters";
import {
  financialMoney,
  type FinancialMoneyMetric,
} from "~/components/financials/metrics";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import type { FinancialYearRow } from "~/lib/queries.server";

type TrendMetric = Extract<
  FinancialMoneyMetric,
  "revenue" | "operatingResult" | "netResult" | "equity"
>;

type TrendCopy = {
  title: string;
  description: string;
  financialYear: string;
  measureLabel: string;
  revenue: string;
  operatingResult: string;
  netResult: string;
  equity: string;
};

export function FinancialTrendChart({
  data,
  locale,
  copy,
}: {
  data: FinancialYearRow[];
  locale: FinancialLocale;
  copy: TrendCopy;
}) {
  const allTrendMetrics: Array<{ value: TrendMetric; label: string }> = [
    { value: "revenue", label: copy.revenue },
    { value: "operatingResult", label: copy.operatingResult },
    { value: "netResult", label: copy.netResult },
    { value: "equity", label: copy.equity },
  ];
  const trendMetrics = allTrendMetrics.filter(({ value }) =>
    data.some((financial) => {
      const pair = financialMoney(financial, value);
      return pair.original != null || pair.usd != null;
    }),
  );
  const [selectedMetric, setSelectedMetric] = useState<TrendMetric>("revenue");
  const activeMetric = trendMetrics.some(
    (item) => item.value === selectedMetric,
  )
    ? selectedMetric
    : trendMetrics[0]?.value;

  if (!activeMetric || data.length < 2) return null;
  const selected = trendMetrics.find((item) => item.value === activeMetric)!;
  const chartConfig = {
    value: {
      label: `${selected.label} (SEK / USD)`,
      color: "var(--chart-1)",
    },
  } satisfies ChartConfig;
  const chartData = [...data].reverse().map((financial) => {
    const pair = financialMoney(financial, activeMetric);
    return {
      year: financial.fiscal_year,
      value: pair.original ?? pair.usd,
      original: pair.original,
      usd: pair.usd,
      currency: pair.original == null ? "USD" : pair.currency,
    };
  });

  return (
    <section className="flex flex-col gap-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="flex flex-col gap-1">
          <h2 className="text-xl font-semibold tracking-tight">{copy.title}</h2>
          <p className="text-muted-foreground max-w-2xl text-sm">
            {copy.description}
          </p>
        </div>
        <ToggleGroup
          value={[activeMetric]}
          onValueChange={(values) => {
            const next = values.at(-1) as TrendMetric | undefined;
            if (next) setSelectedMetric(next);
          }}
          variant="outline"
          size="sm"
          spacing={0}
          aria-label={copy.measureLabel}
          className="flex-wrap"
        >
          {trendMetrics.map((metric) => (
            <ToggleGroupItem key={metric.value} value={metric.value}>
              {metric.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </div>

      <ChartContainer config={chartConfig} className="h-72 w-full">
        <AreaChart
          accessibilityLayer
          data={chartData}
          margin={{ left: 8, right: 20, top: 12, bottom: 0 }}
        >
          <defs>
            <linearGradient
              id="company-financial-area"
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop
                offset="0%"
                stopColor="var(--color-value)"
                stopOpacity={0.24}
              />
              <stop
                offset="100%"
                stopColor="var(--color-value)"
                stopOpacity={0.02}
              />
            </linearGradient>
          </defs>
          <CartesianGrid vertical={false} />
          <XAxis
            dataKey="year"
            tickLine={false}
            axisLine={false}
            tickMargin={10}
          />
          <YAxis
            tickLine={false}
            axisLine={false}
            tickMargin={8}
            width={72}
            tickFormatter={(value: number) => formatCompactMoney(value, locale)}
          />
          <ChartTooltip
            content={
              <ChartTooltipContent
                labelFormatter={(_label, payload) => {
                  const point = payload[0]?.payload as
                    { year?: string } | undefined;
                  return point?.year
                    ? `${copy.financialYear} ${point.year}`
                    : copy.financialYear;
                }}
                formatter={(_value, _name, item) => {
                  const point = item.payload as {
                    original: number | null;
                    usd: number | null;
                    currency: string;
                  };
                  const pair = moneyPairLines(point, locale);
                  return (
                    <div className="flex min-w-48 flex-1 items-center justify-between gap-4">
                      <span className="text-muted-foreground">
                        {selected.label}
                      </span>
                      <span className="flex flex-col items-end font-mono font-medium tabular-nums">
                        <span>{pair.original ?? "—"}</span>
                        <span className="text-muted-foreground">
                          {pair.usd ?? "—"}
                        </span>
                      </span>
                    </div>
                  );
                }}
              />
            }
          />
          <Area
            type="monotone"
            dataKey="value"
            stroke="var(--color-value)"
            strokeWidth={2.5}
            fill="url(#company-financial-area)"
            activeDot={{ r: 4 }}
            animationDuration={450}
            connectNulls={false}
          />
        </AreaChart>
      </ChartContainer>
    </section>
  );
}
