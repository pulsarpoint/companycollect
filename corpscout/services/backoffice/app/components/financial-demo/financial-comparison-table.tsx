import type { ReactNode } from "react";
import { Link } from "react-router";
import type { FinancialDemoPoint } from "~/components/financial-demo/data";
import {
  formatMoneyPair,
  formatNumber,
  formatPercentage,
  type FinancialDemoLocale,
} from "~/components/financial-demo/formatters";
import { Separator } from "~/components/ui/separator";
import { Button } from "~/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { cn } from "~/lib/utils";

type ValueFormat = "money" | "percentage" | "count";

export type FinancialComparisonRow = {
  label: string;
  value: (point: FinancialDemoPoint, index: number) => number | null;
  format?: ValueFormat;
  emphasis?: boolean;
  indent?: boolean;
};

function formatValue(
  value: number | null,
  format: ValueFormat,
  locale: FinancialDemoLocale,
): ReactNode {
  if (value === null) return "—";
  if (format === "percentage") return formatPercentage(value, locale);
  if (format === "count") return formatNumber(value, locale);

  const money = formatMoneyPair(value, locale);
  return (
    <div className="flex flex-col items-end gap-0.5">
      <span>{money.sek}</span>
      <span className="text-muted-foreground text-xs font-normal">
        {money.usd}
      </span>
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
}: {
  id: string;
  title: string;
  description: string;
  data: FinancialDemoPoint[];
  rows: FinancialComparisonRow[];
  locale: FinancialDemoLocale;
  factsHref?: (year: number) => string;
  factsLabel?: string;
}) {
  const newestFirst = [...data].reverse();

  return (
    <section id={id} className="scroll-mt-6">
      <div className="mb-5 flex flex-col gap-1">
        <h2 className="text-xl font-semibold tracking-tight">{title}</h2>
        <p className="text-muted-foreground max-w-3xl text-sm">{description}</p>
      </div>

      <Table className="min-w-[64rem]">
        <TableHeader>
          <TableRow>
            <TableHead className="min-w-64">{title}</TableHead>
            {newestFirst.map((point) => (
              <TableHead key={point.year} className="min-w-36 text-right">
                {factsHref ? (
                  <Button
                    variant="link"
                    size="sm"
                    nativeButton={false}
                    render={
                      <Link
                        to={factsHref(point.year)}
                        aria-label={`${factsLabel ?? "View source facts for"} ${point.year}`}
                      />
                    }
                  >
                    {point.year}
                  </Button>
                ) : (
                  point.year
                )}
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
              {newestFirst.map((point) => {
                const originalIndex = data.findIndex(
                  (candidate) => candidate.year === point.year,
                );
                return (
                  <TableCell
                    key={point.year}
                    className={cn(
                      "text-right tabular-nums",
                      row.emphasis && "font-semibold",
                    )}
                  >
                    {formatValue(
                      row.value(point, originalIndex),
                      row.format ?? "money",
                      locale,
                    )}
                  </TableCell>
                );
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <Separator className="mt-8" />
    </section>
  );
}
