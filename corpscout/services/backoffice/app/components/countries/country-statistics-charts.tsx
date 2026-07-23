import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Line,
  LineChart,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";
import type {
  CountryEurostatSizeRow,
  CountryImfSeries,
  CountryTradePoint,
  CountryWorldBankSeries,
} from "~/lib/country-statistics";
import {
  IMF_INDICATORS,
  WORLD_BANK_INDICATORS,
} from "~/lib/country-statistics";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";

const percentConfig = {
  realGdpGrowth: { label: "Real GDP growth", color: "var(--chart-1)" },
  inflation: { label: "Inflation", color: "var(--chart-2)" },
  unemployment: { label: "Unemployment", color: "var(--chart-3)" },
} satisfies ChartConfig;

const tradeConfig = {
  exportsUsd: { label: "Exports", color: "var(--chart-1)" },
  importsUsd: { label: "Imports", color: "var(--chart-2)" },
  balanceUsd: { label: "Trade balance", color: "var(--chart-4)" },
} satisfies ChartConfig;

const imfConfig = {
  actual: { label: "Actual", color: "var(--chart-1)" },
  forecast: { label: "IMF estimate", color: "var(--chart-2)" },
} satisfies ChartConfig;

const businessConfig = {
  value: { label: "Enterprises", color: "var(--chart-2)" },
} satisfies ChartConfig;

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const compactUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 1,
});

const fullNumber = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
});

function PercentTooltipValue(value: unknown, name: unknown) {
  return (
    <>
      <span className="text-muted-foreground">{String(name)}</span>
      <span className="ml-auto font-mono font-medium tabular-nums">
        {Number(value).toFixed(1)}%
      </span>
    </>
  );
}

function CurrencyTooltipValue(value: unknown, name: unknown) {
  return (
    <>
      <span className="text-muted-foreground">{String(name)}</span>
      <span className="ml-auto font-mono font-medium tabular-nums">
        {compactUsd.format(Number(value))}
      </span>
    </>
  );
}

export function EconomicPulseChart({
  series,
  minYear,
}: {
  series: CountryWorldBankSeries[];
  minYear?: number;
}) {
  const rowsByYear = new Map<number, Record<string, number>>();
  const definitions = [
    [WORLD_BANK_INDICATORS.realGdpGrowth, "realGdpGrowth"],
    [WORLD_BANK_INDICATORS.inflation, "inflation"],
    [WORLD_BANK_INDICATORS.unemployment, "unemployment"],
  ] as const;

  for (const [indicatorCode, dataKey] of definitions) {
    const selectedSeries = series.find((item) => item.indicatorCode === indicatorCode);
    for (const point of selectedSeries?.points ?? []) {
      if (minYear !== undefined && point.year < minYear) continue;
      const row = rowsByYear.get(point.year) ?? {};
      row[dataKey] = point.value;
      rowsByYear.set(point.year, row);
    }
  }

  const data = [...rowsByYear.entries()]
    .sort(([a], [b]) => a - b)
    .map(([year, values]) => ({ year, ...values }));

  return (
    <ChartContainer config={percentConfig} className="h-72 w-full">
      <LineChart data={data} margin={{ top: 8, right: 12, left: -16, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="year" tickLine={false} axisLine={false} minTickGap={28} />
        <YAxis
          tickLine={false}
          axisLine={false}
          tickFormatter={(value: number) => `${value}%`}
        />
        <ChartTooltip
          content={
            <ChartTooltipContent
              indicator="line"
              labelFormatter={(_, payload) => String(payload[0]?.payload?.year ?? "")}
              formatter={PercentTooltipValue}
            />
          }
        />
        <ChartLegend content={<ChartLegendContent />} />
        <Line
          type="monotone"
          dataKey="realGdpGrowth"
          stroke="var(--color-realGdpGrowth)"
          strokeWidth={2}
          dot={false}
          connectNulls
        />
        <Line
          type="monotone"
          dataKey="inflation"
          stroke="var(--color-inflation)"
          strokeWidth={2}
          dot={false}
          connectNulls
        />
        <Line
          type="monotone"
          dataKey="unemployment"
          stroke="var(--color-unemployment)"
          strokeWidth={2}
          dot={false}
          connectNulls
        />
      </LineChart>
    </ChartContainer>
  );
}

export function TradeHistoryChart({ points }: { points: CountryTradePoint[] }) {
  return (
    <ChartContainer config={tradeConfig} className="h-80 w-full">
      <ComposedChart data={points} margin={{ top: 8, right: 12, left: 4, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="year" tickLine={false} axisLine={false} minTickGap={24} />
        <YAxis
          tickLine={false}
          axisLine={false}
          width={58}
          tickFormatter={(value: number) => compactUsd.format(value)}
        />
        <ReferenceLine y={0} stroke="var(--border)" />
        <ChartTooltip
          content={
            <ChartTooltipContent
              labelFormatter={(_, payload) => String(payload[0]?.payload?.year ?? "")}
              formatter={CurrencyTooltipValue}
            />
          }
        />
        <ChartLegend content={<ChartLegendContent />} />
        <Bar
          dataKey="balanceUsd"
          fill="var(--color-balanceUsd)"
          fillOpacity={0.3}
          radius={3}
        />
        <Line
          type="monotone"
          dataKey="exportsUsd"
          stroke="var(--color-exportsUsd)"
          strokeWidth={2}
          dot={false}
        />
        <Line
          type="monotone"
          dataKey="importsUsd"
          stroke="var(--color-importsUsd)"
          strokeWidth={2}
          dot={false}
        />
      </ComposedChart>
    </ChartContainer>
  );
}

export function ImfForecastChart({ series }: { series: CountryImfSeries[] }) {
  const growth = series.find(
    (item) => item.indicatorCode === IMF_INDICATORS.realGdpGrowth,
  );
  if (!growth) return null;

  const points = growth.points;
  const firstEstimateIndex = points.findIndex((point) => point.isEstimate);
  const firstEstimateYear =
    firstEstimateIndex >= 0 ? points[firstEstimateIndex].year : undefined;
  const data = points.map((point, index) => ({
    year: point.year,
    actual: point.isEstimate ? null : point.value,
    forecast:
      point.isEstimate || index === firstEstimateIndex - 1 ? point.value : null,
  }));

  return (
    <ChartContainer config={imfConfig} className="h-72 w-full">
      <LineChart data={data} margin={{ top: 8, right: 12, left: -16, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="year" tickLine={false} axisLine={false} minTickGap={28} />
        <YAxis
          tickLine={false}
          axisLine={false}
          tickFormatter={(value: number) => `${value}%`}
        />
        {firstEstimateYear ? (
          <ReferenceLine
            x={firstEstimateYear}
            stroke="var(--border)"
            strokeDasharray="4 4"
          />
        ) : null}
        <ChartTooltip
          content={
            <ChartTooltipContent
              indicator="line"
              labelFormatter={(_, payload) => String(payload[0]?.payload?.year ?? "")}
              formatter={PercentTooltipValue}
            />
          }
        />
        <ChartLegend content={<ChartLegendContent />} />
        <Line
          type="monotone"
          dataKey="actual"
          stroke="var(--color-actual)"
          strokeWidth={2}
          dot={false}
        />
        <Line
          type="monotone"
          dataKey="forecast"
          stroke="var(--color-forecast)"
          strokeWidth={2}
          strokeDasharray="5 4"
          dot={false}
        />
      </LineChart>
    </ChartContainer>
  );
}

type BusinessMeasure = "enterprises" | "employment" | "turnoverEur" | "valueAddedEur";

const BUSINESS_MEASURES: Array<{
  value: BusinessMeasure;
  label: string;
  shortLabel: string;
  money: boolean;
}> = [
  { value: "enterprises", label: "Enterprises", shortLabel: "Enterprises", money: false },
  { value: "employment", label: "Employment", shortLabel: "Employment", money: false },
  { value: "turnoverEur", label: "Turnover", shortLabel: "Turnover", money: true },
  { value: "valueAddedEur", label: "Value added", shortLabel: "Value added", money: true },
];

export function BusinessSizeChart({ rows }: { rows: CountryEurostatSizeRow[] }) {
  const [measure, setMeasure] = useState<BusinessMeasure>("enterprises");
  const definition = BUSINESS_MEASURES.find((item) => item.value === measure)!;
  const data = rows.map((row) => ({
    label: row.label,
    value: row[measure],
  }));

  return (
    <div className="flex flex-col gap-4">
      <ToggleGroup
        value={[measure]}
        onValueChange={(values) => {
          const next = values.at(-1) as BusinessMeasure | undefined;
          if (next) setMeasure(next);
        }}
        variant="outline"
        size="sm"
        spacing={0}
        aria-label="Business size measure"
        className="max-w-full overflow-x-auto"
      >
        {BUSINESS_MEASURES.map((item) => (
          <ToggleGroupItem
            key={item.value}
            value={item.value}
            aria-label={item.label}
          >
            {item.shortLabel}
          </ToggleGroupItem>
        ))}
      </ToggleGroup>
      <ChartContainer config={businessConfig} className="h-72 w-full">
        <BarChart data={data} margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="label" tickLine={false} axisLine={false} />
          <YAxis
            tickLine={false}
            axisLine={false}
            width={58}
            tickFormatter={(value: number) =>
              `${definition.money ? "€" : ""}${compactNumber.format(value)}`
            }
          />
          <ChartTooltip
            cursor={{ fill: "var(--muted)" }}
            content={
              <ChartTooltipContent
                hideLabel
                formatter={(value) => (
                  <>
                    <span className="text-muted-foreground">{definition.label}</span>
                    <span className="ml-auto font-mono font-medium tabular-nums">
                      {definition.money ? "€" : ""}
                      {fullNumber.format(Number(value))}
                    </span>
                  </>
                )}
              />
            }
          />
          <Bar dataKey="value" fill="var(--color-value)" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ChartContainer>
    </div>
  );
}
