import type { FrFinancialRow } from "~/lib/queries.server";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const rf = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});
const bf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

/** INPI's type de bilan. Unknown codes pass through rather than being renamed
 * into something that looks authoritative and is not. */
export function balanceLabel(code: string): string {
  if (code === "C") return "Complete";
  if (code === "S") return "Simplified";
  if (code === "K") return "Consolidated";
  return code;
}

/** Whether this filing may legally omit lines. 1,590,754 of 6,542,232 rows
 * are one of the three non-public statuses, so a blank figure on those is
 * WITHHELD rather than zero, and the badge is what tells a reader which. */
export function isWithheld(row: FrFinancialRow): boolean {
  return row.confidentiality !== "Public";
}

/** A null is a dash, never a zero -- and a genuine zero stays a zero. */
export function formatRatio(
  value: number | null,
  unit: "percent" | "days" | "ratio",
): string {
  if (value == null) return "—";
  if (unit === "percent") return `${rf.format(value)}%`;
  if (unit === "days") return `${rf.format(value)} d`;
  return bf.format(value);
}

function money(value: number | null) {
  return value == null ? (
    <span className="text-muted-foreground">—</span>
  ) : (
    nf.format(value)
  );
}

/** Original on top, USD beneath. Currency is EUR for every French row, so the
 * USD figure is a conversion rather than a second reporting currency. */
function MoneyPair({
  original,
  usd,
  currency,
}: {
  original: number | null;
  usd: number | null;
  currency: string;
}) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      <span>
        {money(original)}
        {original == null ? "" : ` ${currency}`}
      </span>
      <span className="text-muted-foreground text-xs">
        {usd == null ? "—" : `$${nf.format(usd)}`}
      </span>
    </div>
  );
}

type RatioKey = {
  [K in keyof FrFinancialRow]: FrFinancialRow[K] extends number | null ? K : never;
}[keyof FrFinancialRow];

const RATIOS: {
  key: RatioKey;
  label: string;
  unit: "percent" | "days" | "ratio";
}[] = [
  { key: "ebitda_margin_percent", label: "EBITDA margin", unit: "percent" },
  { key: "debt_ratio_percent", label: "Debt ratio", unit: "percent" },
  { key: "financial_autonomy_percent", label: "Financial autonomy", unit: "percent" },
  { key: "liquidity_ratio_percent", label: "Liquidity", unit: "percent" },
  { key: "interest_coverage_percent", label: "Interest coverage", unit: "percent" },
  { key: "customer_payment_days", label: "Customer payment", unit: "days" },
  { key: "supplier_payment_days", label: "Supplier payment", unit: "days" },
  { key: "inventory_turnover_days", label: "Inventory turnover", unit: "days" },
];

/**
 * France's filed accounts (INPI), with the ratio suite the register publishes.
 *
 * Its own section rather than the canonical FinancialsSection because France
 * carries gross margin, EBITDA, EBIT and fourteen ratio and working-capital
 * columns that the five-column shape has no room for -- and carries neither
 * equity nor total assets, which it has. Fill is essentially complete: revenue
 * and EBITDA 100%, financial autonomy 99.96%, debt ratio 99.93%.
 *
 * One row per fiscal year; where a company filed under two bases in one year
 * the query already picked one, and the basis is named on the row.
 */
export function FrFinancialsSection({
  financials,
}: {
  financials: FrFinancialRow[];
}) {
  if (financials.length === 0) return null;
  const anyWithheld = financials.some(isWithheld);

  return (
    <Card id="fr-financials">
      <CardHeader>
        <CardTitle className="text-base">Financials</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="overflow-x-auto">
          <Table className="min-w-[46rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead>Basis</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Gross margin</TableHead>
                <TableHead className="text-right">EBITDA</TableHead>
                <TableHead className="text-right">EBIT</TableHead>
                <TableHead className="text-right">Net income</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {financials.map((r) => (
                <TableRow key={r.fiscal_year}>
                  <TableCell className="tabular-nums whitespace-nowrap">
                    {r.fiscal_year}
                  </TableCell>
                  <TableCell className="whitespace-nowrap">
                    {balanceLabel(r.balance_type)}
                    {isWithheld(r) ? (
                      <span
                        className="text-muted-foreground ml-1 text-xs"
                        title={r.confidentiality}
                      >
                        · partly confidential
                      </span>
                    ) : null}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.revenue_original}
                      usd={r.revenue_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.gross_margin_original}
                      usd={r.gross_margin_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.ebitda_original}
                      usd={r.ebitda_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.ebit_original}
                      usd={r.ebit_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair
                      original={r.net_income_original}
                      usd={r.net_income_usd}
                      currency={r.currency}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        <div className="overflow-x-auto">
          <Table className="min-w-[46rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Ratio</TableHead>
                {financials.map((r) => (
                  <TableHead key={r.fiscal_year} className="text-right">
                    {r.fiscal_year}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {RATIOS.map((ratio) => (
                <TableRow key={ratio.key}>
                  <TableCell className="whitespace-nowrap">{ratio.label}</TableCell>
                  {financials.map((r) => (
                    <TableCell
                      key={r.fiscal_year}
                      className="text-right tabular-nums"
                    >
                      {formatRatio(r[ratio.key], ratio.unit)}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        <p className="text-muted-foreground text-xs">
          Filed accounts from INPI. Equity and total assets are not published in
          this dataset.
          {anyWithheld
            ? " A company may legally restrict publication, so a blank figure on a partly confidential filing means withheld, not zero."
            : ""}
        </p>
      </CardContent>
    </Card>
  );
}
