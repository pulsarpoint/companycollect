import type { TaxRecordRow } from "~/lib/queries.server";
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

function money(v: number | null) {
  return v == null ? <span className="text-muted-foreground">—</span> : nf.format(v);
}

/** Original value on top, USD equivalent muted beneath — both or whichever exists. */
function MoneyPair({ original, usd }: { original: number | null; usd: number | null }) {
  if (original == null && usd == null) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col items-end">
      <span>{money(original)}</span>
      <span className="text-muted-foreground text-xs">
        {usd == null ? "—" : `$${nf.format(usd)}`}
      </span>
    </div>
  );
}

/** Vero dropped the prepayments column from the published files for tax years
 * ≥ 2023, so hide the column when no shown year carries it. */
function hasPrepayments(taxRecords: TaxRecordRow[]): boolean {
  return taxRecords.some(
    (r) =>
      r.prepayments_total_amount_original != null ||
      r.prepayments_total_amount_usd != null,
  );
}

/**
 * Public corporate income tax data (Verohallinto). Tax-base figures, not
 * financial statements — taxable income diverges from accounting profit
 * (loss carryforwards, group contributions), so this stays a separate
 * section and is never merged into Financials.
 */
export function FiTaxRecordsSection({ taxRecords }: { taxRecords: TaxRecordRow[] }) {
  if (taxRecords.length === 0) return null;
  const showPrepayments = hasPrepayments(taxRecords);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Tax records</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="overflow-x-auto">
          <Table className="min-w-[44rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Tax year</TableHead>
                <TableHead>Currency</TableHead>
                <TableHead>Municipality</TableHead>
                <TableHead className="text-right">Taxable income</TableHead>
                <TableHead className="text-right">Taxes assessed</TableHead>
                {showPrepayments ? (
                  <TableHead className="text-right">Prepayments</TableHead>
                ) : null}
                <TableHead className="text-right">Refund</TableHead>
                <TableHead className="text-right">Residual tax</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {taxRecords.map((r) => (
                <TableRow key={r.tax_year}>
                  <TableCell className="tabular-nums align-top">{r.tax_year}</TableCell>
                  <TableCell className="align-top">{r.currency}</TableCell>
                  <TableCell className="align-top">{r.municipality_name}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={r.taxable_income_amount_original} usd={r.taxable_income_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={r.taxes_total_amount_original} usd={r.taxes_total_amount_usd} />
                  </TableCell>
                  {showPrepayments ? (
                    <TableCell className="text-right tabular-nums">
                      <MoneyPair original={r.prepayments_total_amount_original} usd={r.prepayments_total_amount_usd} />
                    </TableCell>
                  ) : null}
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={r.tax_refund_amount_original} usd={r.tax_refund_amount_usd} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <MoneyPair original={r.residual_tax_amount_original} usd={r.residual_tax_amount_usd} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="text-muted-foreground text-xs">
          Public corporate income taxation data. Taxable income is a tax-base
          figure and can differ from accounting profit. Source: Finnish Tax
          Administration (Verohallinto), CC BY 4.0.
        </p>
      </CardContent>
    </Card>
  );
}
