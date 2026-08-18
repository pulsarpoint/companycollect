import { useState } from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import type { FinancialDemoPoint } from "~/components/financial-demo/data";
import {
  formatCompactMoneyPair,
  formatMoneyPair,
  type FinancialDemoLocale,
} from "~/components/financial-demo/formatters";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";

type TrendMetric = "revenue" | "operatingResult" | "netResult" | "equity";

export function FinancialTrendChart({
  data,
  locale,
  copy,
}: {
  data: FinancialDemoPoint[];
  locale: FinancialDemoLocale;
  copy: {
    title: string;
    description: string;
    financialYear: string;
    measureLabel: string;
    revenue: string;
    operatingResult: string;
    netResult: string;
    equity: string;
  };
}) {
  const [metric, setMetric] = useState<TrendMetric>("revenue");
  const trendMetrics: Array<{ value: TrendMetric; label: string }> = [
    { value: "revenue", label: copy.revenue },
    { value: "operatingResult", label: copy.operatingResult },
    { value: "netResult", label: copy.netResult },
    { value: "equity", label: copy.equity },
  ];
  const selectedMetric = trendMetrics.find((item) => item.value === metric)!;
  const chartConfig = {
    value: {
      label: `${selectedMetric.label} (SEK / USD)`,
      color: "var(--chart-1)",
    },
  } satisfies ChartConfig;
  const chartData = data.map((point) => ({
    year: point.year,
    value: point[metric],
  }));

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="flex flex-col gap-1">
          <h2 className="text-xl font-semibold tracking-tight">{copy.title}</h2>
          <p className="text-muted-foreground max-w-2xl text-sm">
            {copy.description}
          </p>
        </div>
        <ToggleGroup
          value={[metric]}
          onValueChange={(values) => {
            const next = values.at(-1) as TrendMetric | undefined;
            if (next) setMetric(next);
          }}
          variant="outline"
          size="sm"
          spacing={0}
          aria-label={copy.measureLabel}
          className="flex-wrap"
        >
          {trendMetrics.map((item) => (
            <ToggleGroupItem key={item.value} value={item.value}>
              {item.label}
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
              id="financial-demo-area"
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
            width={96}
            tickFormatter={(value: number) =>
              formatCompactMoneyPair(value, locale)
            }
          />
          <ChartTooltip
            content={
              <ChartTooltipContent
                labelFormatter={(_label, payload) => {
                  const point = payload[0]?.payload as
                    { year?: number } | undefined;
                  return point?.year
                    ? `${copy.financialYear} ${point.year}`
                    : copy.financialYear;
                }}
                formatter={(value) => {
                  const money = formatMoneyPair(Number(value), locale);
                  return (
                    <div className="flex min-w-48 flex-1 items-center justify-between gap-4">
                      <span className="text-muted-foreground">
                        {selectedMetric.label}
                      </span>
                      <span className="flex flex-col items-end font-mono font-medium tabular-nums">
                        <span>{money.sek}</span>
                        <span className="text-muted-foreground">
                          {money.usd}
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
            fill="url(#financial-demo-area)"
            activeDot={{ r: 4 }}
            animationDuration={450}
          />
        </AreaChart>
      </ChartContainer>
    </div>
  );
}
