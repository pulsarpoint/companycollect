import { Link } from "react-router";
import { Database } from "lucide-react";
import type { CountryIndustryGroup } from "~/lib/countries-overview.server";
import type { DivisionRevenue } from "~/lib/financial-aggregates.server";
import {
  IMF_INDICATORS,
  WORLD_BANK_INDICATORS,
  type CountryImfSeries,
  type CountryWorldBankSeries,
} from "~/lib/country-statistics";
import { formatRevenueUsd } from "~/lib/money";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

/**
 * Formatters and small building blocks shared across the country page's tab
 * components (overview/economy/trade/business) and the layout that hosts
 * them. Kept here rather than duplicated per tab, and out of any route file
 * so they stay usable from the layout too.
 */

export const nf = new Intl.NumberFormat("en-US");
export const compactUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 2,
});
export const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 2,
});

export function Metric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="min-w-0 px-4 py-4 first:pl-0">
      <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">{label}</p>
      <p className="mt-1 truncate text-xl font-semibold tabular-nums" title={value}>
        {value}
      </p>
      {detail ? <p className="text-muted-foreground mt-1 truncate text-xs">{detail}</p> : null}
    </div>
  );
}

export function EmptyData({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <Empty className="min-h-56 border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Database />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

export function getWorldBankSeries(
  series: CountryWorldBankSeries[],
  indicatorCode: string,
): CountryWorldBankSeries | undefined {
  return series.find((item) => item.indicatorCode === indicatorCode);
}

export function formatWorldBankValue(indicatorCode: string, value: number): string {
  if (
    indicatorCode === WORLD_BANK_INDICATORS.gdp ||
    indicatorCode === WORLD_BANK_INDICATORS.exports ||
    indicatorCode === WORLD_BANK_INDICATORS.imports
  ) {
    return compactUsd.format(value);
  }
  if (
    indicatorCode === WORLD_BANK_INDICATORS.realGdpGrowth ||
    indicatorCode === WORLD_BANK_INDICATORS.inflation ||
    indicatorCode === WORLD_BANK_INDICATORS.unemployment
  ) {
    return `${value.toFixed(1)}%`;
  }
  if (indicatorCode === WORLD_BANK_INDICATORS.population) {
    return compactNumber.format(value);
  }
  return compactUsd.format(value);
}

export function formatImfValue(series: CountryImfSeries, value: number): string {
  if (series.indicatorCode === IMF_INDICATORS.nominalGdp) {
    return `$${value.toLocaleString("en-US", { maximumFractionDigits: 1 })}B`;
  }
  return `${value.toFixed(1)}%`;
}

export function SourceLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="underline underline-offset-2 hover:text-foreground"
    >
      {children}
    </a>
  );
}

/**
 * The leading-industries table shown on both the overview and business tabs,
 * driven by whichever industry axis the country has: reported revenue by
 * NACE division where financials are loaded, else the registry's own
 * industry-group coverage. `"division" in industry` distinguishes the two
 * row shapes at render time.
 */
export type IndustryListItem = DivisionRevenue | CountryIndustryGroup;
export type IndustryMode = "revenue" | "coverage";

export function IndustryTable({
  countryCode,
  industries,
  industryMode,
}: {
  countryCode: string;
  industries: IndustryListItem[];
  industryMode: IndustryMode;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Industry</TableHead>
          <TableHead className="text-right">Companies</TableHead>
          {industryMode === "revenue" ? (
            <TableHead className="text-right">Revenue (USD)</TableHead>
          ) : null}
        </TableRow>
      </TableHeader>
      <TableBody>
        {industries.map((industry) => {
          const isRevenue = "division" in industry;
          const code = isRevenue ? industry.division : industry.code;
          const href = isRevenue
            ? `/financials/industry/${code}?country=${countryCode}`
            : `/countries/${countryCode}/companies?f_industry=${encodeURIComponent(code)}`;
          return (
            <TableRow key={code}>
              <TableCell>
                <Link to={href} className="font-medium underline-offset-2 hover:underline">
                  {industry.label}
                </Link>
                <span className="text-muted-foreground ml-2 font-mono text-xs">{code}</span>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {nf.format(industry.companies)}
              </TableCell>
              {isRevenue ? (
                <TableCell className="text-right tabular-nums">
                  {formatRevenueUsd(industry.revenue_usd, null)}
                </TableCell>
              ) : null}
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
