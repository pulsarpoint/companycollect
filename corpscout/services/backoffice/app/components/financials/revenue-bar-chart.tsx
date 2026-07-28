import { useNavigate } from "react-router";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import type { YAxisTickContentProps } from "recharts";
import { formatRevenueUsd } from "~/lib/money";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "~/components/ui/chart";

export type RevenueBarChartItem = {
  key: string;
  label: string;
  revenue_usd: number | null;
  href?: string;
};

type ChartRow = {
  key: string;
  label: string;
  value: number;
  revenue_usd: number | null;
  href?: string;
};

const chartConfig = {
  value: { label: "Revenue (USD)", color: "var(--chart-2)" },
} satisfies ChartConfig;

// Each category gets a fixed-height row so 15-20 bars stay readable instead
// of squeezing into a fixed aspect-ratio box (recharts' ChartContainer default).
const ROW_HEIGHT = 32;
const CHART_VERTICAL_PADDING = 40; // room for the x-axis line + tick labels
const Y_AXIS_WIDTH = 170;
const MAX_LABEL_CHARS = 22;

const fullUsd = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function truncateLabel(label: string): string {
  return label.length > MAX_LABEL_CHARS ? `${label.slice(0, MAX_LABEL_CHARS - 1)}…` : label;
}

/**
 * Custom Y-axis tick: truncates long labels and, when the row has an href,
 * doubles as a click target so the label itself navigates like the bar does.
 */
function makeLabelTick(data: ChartRow[], onNavigate: (href: string) => void) {
  return function LabelTick({ x, y, index }: YAxisTickContentProps) {
    const row = data[index];
    if (!row) return null;
    const clickable = Boolean(row.href);
    return (
      <text
        x={x}
        y={y}
        dy={4}
        textAnchor="end"
        className={
          clickable
            ? "cursor-pointer fill-muted-foreground text-xs hover:fill-foreground hover:underline"
            : "fill-muted-foreground text-xs"
        }
        onClick={clickable ? () => onNavigate(row.href!) : undefined}
      >
        {truncateLabel(row.label)}
        <title>{row.label}</title>
      </text>
    );
  };
}

/** Tooltip value formatter: full (non-compact) USD, or an em dash when revenue is unknown. */
function formatTooltipValue(_value: unknown, _name: unknown, item: { payload?: unknown }) {
  const row = item.payload as ChartRow | undefined;
  if (row?.revenue_usd == null) return "—";
  return `$${fullUsd.format(row.revenue_usd)}`;
}

/**
 * Horizontal revenue bar chart shared by the financials landing, country, and
 * industry pages. Renders nothing for fewer than two bars — a single bar is
 * noise better left to the table below it.
 */
export function RevenueBarChart({ items }: { items: RevenueBarChartItem[] }) {
  const navigate = useNavigate();

  if (items.length < 2) return null;

  const data: ChartRow[] = items.map((item) => ({
    key: item.key,
    label: item.label,
    value: item.revenue_usd ?? 0,
    revenue_usd: item.revenue_usd,
    href: item.href,
  }));
  const hasLinks = data.some((row) => row.href);
  const LabelTick = makeLabelTick(data, navigate);

  return (
    <ChartContainer
      config={chartConfig}
      className="w-full"
      style={{ height: data.length * ROW_HEIGHT + CHART_VERTICAL_PADDING }}
    >
      <BarChart data={data} layout="vertical" margin={{ top: 8, right: 24, bottom: 8, left: 0 }}>
        <CartesianGrid horizontal={false} />
        <XAxis
          type="number"
          tickLine={false}
          axisLine={false}
          tickFormatter={(value: number) => formatRevenueUsd(value, null)}
        />
        <YAxis
          type="category"
          dataKey="label"
          tickLine={false}
          axisLine={false}
          width={Y_AXIS_WIDTH}
          interval={0}
          tick={LabelTick}
        />
        <ChartTooltip cursor={{ fill: "var(--muted)" }} content={<ChartTooltipContent formatter={formatTooltipValue} />} />
        <Bar
          dataKey="value"
          fill="var(--color-value)"
          radius={[0, 4, 4, 0]}
          barSize={20}
          className="opacity-100 transition-opacity duration-150 hover:opacity-70"
          cursor={hasLinks ? "pointer" : undefined}
          onClick={(bar) => {
            // recharts types Bar's onClick datum as a BarRectangleItem with a
            // nested `payload`, but at runtime (recharts 3.8) the callback
            // receives our original row object directly.
            const row = bar as unknown as ChartRow;
            if (row.href) navigate(row.href);
          }}
        />
      </BarChart>
    </ChartContainer>
  );
}
