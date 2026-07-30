import { Bar, BarChart, XAxis } from "recharts";

import type { ContractHeadlineStats } from "~/lib/contracts.server";
import { Metric } from "~/components/country/shared";
import { ChartContainer, ChartTooltip } from "~/components/ui/chart";

const nf = new Intl.NumberFormat("en-US");

/**
 * Headline figures for a country's contracts, replacing the shared country
 * banner on this tab.
 *
 * Everything here is ranked and charted by CONTRACT COUNT, never by value.
 * Brazil's register carries 34 source typos holding 98.2% of its total, so a
 * value-ranked "top winner" would headline a data-entry error — CENTRO
 * OFTALMOLOGICO DE BELEM LTDA, $103.48bn, from one contract. By count the same
 * question answers usefully, and every country's answer is a recognisable
 * institution: Statens vegvesen, Trafikverket, Puolustusvoimat.
 *
 * Value ranking needs the plausibility exclusion brazil_pncp-design.md §9a
 * specifies. Until that exists, count is the axis that cannot mislead.
 */
export function ContractStatsBanner({ stats }: { stats: ContractHeadlineStats }) {
  const years = stats.perYear;
  // One year is not a trend, so the chart is dropped rather than drawn as a lone
  // bar pretending to be one. Brazil is exactly this until its 36-month recovery
  // runs, and the year span beside the total says so.
  const showChart = years.length > 1;
  const span =
    years.length === 0
      ? "no dated contracts"
      : years.length === 1
        ? `${years[0].year} only`
        : `${years[0].year}–${years[years.length - 1].year}`;

  return (
    <section
      aria-label="Contract headline statistics"
      className="grid grid-cols-1 gap-4 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-[repeat(3,minmax(0,1fr))_2fr]"
    >
      <Metric
        label="Contracts"
        value={nf.format(stats.totalContracts)}
        detail={span}
      />
      <Metric
        label="Top buyer"
        value={stats.topBuyer?.name ?? "—"}
        detail={
          stats.topBuyer
            ? `${nf.format(stats.topBuyer.contracts)} contracts`
            : "no buyer named"
        }
      />
      <Metric
        label="Top supplier"
        value={stats.topWinner?.name ?? "—"}
        detail={
          stats.topWinner
            ? `${nf.format(stats.topWinner.contracts)} contracts`
            : "no supplier named"
        }
      />
      {showChart ? (
        <div className="min-w-0 px-4 py-4">
          <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            Contracts per year
          </p>
          <ChartContainer
            config={{ contracts: { label: "Contracts", color: "var(--chart-1)" } }}
            className="mt-1 h-16 w-full"
          >
            <BarChart data={years} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
              <XAxis
                dataKey="year"
                tickLine={false}
                axisLine={false}
                tick={{ fontSize: 10 }}
                interval="preserveStartEnd"
              />
              <ChartTooltip />
              <Bar dataKey="contracts" fill="var(--color-contracts)" radius={2} />
            </BarChart>
          </ChartContainer>
        </div>
      ) : null}
    </section>
  );
}
