import type { ReactNode } from "react";
import { Link } from "react-router";
import type { FinancialLocale } from "~/components/financials/copy";
import {
  formatNumber,
  formatPercentage,
  moneyPairLines,
  type MoneyPair,
} from "~/components/financials/formatters";
import { buttonVariants } from "~/components/ui/button";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { cn } from "~/lib/utils";
import type { FinancialYearRow } from "~/lib/queries.server";

type ValueFormat = "money" | "percentage" | "count";

export type FinancialComparisonRow = {
  label: string;
  value: (
    financial: FinancialYearRow,
    index: number,
  ) => MoneyPair | number | null;
  format?: ValueFormat;
  emphasis?: boolean;
  indent?: boolean;
};

function formatValue(
  value: MoneyPair | number | null,
  format: ValueFormat,
  locale: FinancialLocale,
): ReactNode {
  if (value === null) return <span className="text-muted-foreground">—</span>;
  if (format === "percentage") {
    return formatPercentage(value as number, locale);
  }
  if (format === "count") return formatNumber(value as number, locale);

  const pair = moneyPairLines(value as MoneyPair, locale);
  if (pair.original === null && pair.usd === null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end gap-0.5">
      {pair.original ? <span>{pair.original}</span> : null}
      {pair.usd ? (
        <span className="text-muted-foreground text-xs font-normal">
          {pair.usd}
        </span>
      ) : null}
    </div>
  );
}

export function FinancialComparisonTable({
  id,
  title,
  description,
  data,
  rows,
  locale,
  factsHref,
  factsLabel,
  comparativeLabel,
}: {
  id: string;
  title: string;
  description: string;
  data: FinancialYearRow[];
  rows: FinancialComparisonRow[];
  locale: FinancialLocale;
  factsHref?: (year: string) => string;
  factsLabel: string;
  comparativeLabel: (sourceYear: string) => string;
}) {
  return (
    <section id={id} className="scroll-mt-6">
      <div className="mb-5 flex flex-col gap-1">
        <h2 className="text-xl font-semibold tracking-tight">{title}</h2>
        <p className="text-muted-foreground max-w-3xl text-sm">{description}</p>
      </div>

      <div className="overflow-x-auto">
        <Table className="min-w-[64rem]">
          <TableHeader>
            <TableRow>
              <TableHead className="min-w-64">{title}</TableHead>
              {data.map((financial) => (
                <TableHead
                  key={financial.fiscal_year}
                  className="min-w-36 text-right align-top"
                >
                  {factsHref && financial.observation !== "comparative" ? (
                    <Link
                      to={factsHref(financial.fiscal_year)}
                      aria-label={`${factsLabel} ${financial.fiscal_year}`}
                      className={buttonVariants({
                        variant: "link",
                        size: "sm",
                        className: "-mr-2",
                      })}
                    >
                      {financial.fiscal_year}
                    </Link>
                  ) : (
                    <span className="inline-flex h-7 items-center">
                      {financial.fiscal_year}
                    </span>
                  )}
                  {financial.observation === "comparative" &&
                  financial.source_fiscal_year ? (
                    <span className="text-muted-foreground block text-[0.7rem] font-normal whitespace-nowrap">
                      {comparativeLabel(financial.source_fiscal_year)}
                    </span>
                  ) : null}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.label}>
                <TableCell
                  className={cn(
                    "min-w-64",
                    row.emphasis && "font-semibold",
                    row.indent && "pl-6",
                  )}
                >
                  {row.label}
                </TableCell>
                {data.map((financial, index) => (
                  <TableCell
                    key={financial.fiscal_year}
                    className={cn(
                      "text-right tabular-nums",
                      row.emphasis && "font-semibold",
                    )}
                  >
                    {formatValue(
                      row.value(financial, index),
                      row.format ?? "money",
                      locale,
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <Separator className="mt-8" />
    </section>
  );
}
