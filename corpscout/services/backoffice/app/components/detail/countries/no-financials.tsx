import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { FieldGrid, formatFieldValue, isLineageKey } from "~/components/detail/fields";

const anf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

const INCOME_KEYS = [
  "operating_revenue_amount_original", "operating_revenue_amount_usd",
  "operating_costs_amount_original", "operating_costs_amount_usd",
  "operating_result_amount_original", "operating_result_amount_usd",
  "net_financial_items_amount_original", "net_financial_items_amount_usd",
  "pretax_result_amount_original", "pretax_result_amount_usd",
  "net_result_amount_original", "net_result_amount_usd",
];
const BALANCE_KEYS = [
  "total_assets_amount_original", "total_assets_amount_usd",
  "fixed_assets_amount_original", "fixed_assets_amount_usd",
  "current_assets_amount_original", "current_assets_amount_usd",
  "equity_amount_original", "equity_amount_usd",
  "total_debt_amount_original", "total_debt_amount_usd",
  "long_term_liabilities_amount_original", "long_term_liabilities_amount_usd",
  "current_liabilities_amount_original", "current_liabilities_amount_usd",
];
const META_KEYS = [
  "period_start_date", "period_end_date", "accounts_type", "is_parent_company",
  "statement_layout", "accounting_rules", "liquidation_accounts",
  "is_not_audited", "opted_out_audit", "is_small_enterprise",
  "journal_number", "filing_id", "last_submitted_accounts_year",
  "legal_name", "legal_form_code",
  "fx_rate_to_usd", "fx_rate_date", "fx_source", "source_url",
];
const HEADER_KEYS = ["fiscal_year", "currency", "org_number", "quality_flag"];

/** Human wording for pipeline quality flags on a statement row. Exported for
 * tests. Unknown flag values fall back to the raw flag text. */
export function qualityFlagLabel(row: Record<string, unknown>): string | null {
  const flag = row.quality_flag == null ? "" : String(row.quality_flag);
  if (flag === "") return null;
  if (flag === "implausible_magnitude") {
    return "implausible values — likely source filing error";
  }
  return flag;
}
const PLACED = new Set([...INCOME_KEYS, ...BALANCE_KEYS, ...META_KEYS, ...HEADER_KEYS]);

/** Exported for the fidelity test: keys a statement row may contain that are
 * neither placed in a group nor lineage end up in the "Other fields" grid. */
export function restKeys(row: Record<string, unknown>): string[] {
  return Object.keys(row).filter((k) => !PLACED.has(k) && !isLineageKey(k));
}

/** Exported for the fidelity test: the keys explicitly grouped into a
 * visible section (income/balance/meta/header), as opposed to lineage
 * or "Other fields". */
export function placedKeys(): string[] {
  return [...PLACED];
}

function pick(row: Record<string, unknown>, keys: string[]): [string, unknown][] {
  return keys.filter((k) => k in row).map((k) => [k, row[k]]);
}

export function buildAmountFields(
  row: Record<string, unknown>,
  keys: string[],
): [string, string | null][] {
  return keys
    .filter((k) => k in row)
    .map((k) => {
      if (k.endsWith("_amount_original")) {
        const v = row[k];
        if (typeof v !== "number") return [k, null];
        return [k, `${anf.format(v)} ${String(row.currency ?? "")}`.trim()];
      }
      if (k.endsWith("_amount_usd")) {
        const stored = row[k];
        if (typeof stored === "number") return [k, `${anf.format(stored)} USD`];
        const original = row[k.replace("_amount_usd", "_amount_original")];
        const fx = row.fx_rate_to_usd;
        if (typeof original === "number" && typeof fx === "number") {
          return [k, `≈ ${anf.format(Math.round(original * fx * 100) / 100)} USD`];
        }
        return [k, null];
      }
      return [k, formatFieldValue(k, row[k])];
    });
}

export function NoFinancialsSection({
  statements,
}: {
  statements: Record<string, unknown>[];
}) {
  if (statements.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financial statements (Brønnøysund)</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {statements.map((row, i) => (
          <div key={`${row.fiscal_year}-${row.filing_id ?? ""}-${i}`} className="space-y-4">
            <p className="text-sm font-semibold">
              {formatFieldValue("fiscal_year", row.fiscal_year) ?? "?"}
              {" · "}
              {formatFieldValue("accounts_type", row.accounts_type) ?? "—"}
              {Number(row.is_parent_company) === 1 ? " (parent/group accounts)" : ""}
              {" · "}
              {String(row.currency ?? "")}
              {qualityFlagLabel(row) ? (
                <Badge variant="destructive" className="ml-2 align-middle">
                  {qualityFlagLabel(row)}
                </Badge>
              ) : null}
            </p>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Income statement</p>
              <FieldGrid fields={buildAmountFields(row, INCOME_KEYS)} />
            </div>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Balance sheet</p>
              <FieldGrid fields={buildAmountFields(row, BALANCE_KEYS)} />
            </div>
            <div>
              <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Filing details</p>
              <FieldGrid fields={pick(row, META_KEYS)} />
            </div>
            {restKeys(row).length > 0 ? (
              <div>
                <p className="text-muted-foreground mb-2 text-xs font-medium uppercase tracking-wide">Other fields</p>
                <FieldGrid fields={pick(row, restKeys(row))} />
              </div>
            ) : null}
            {(() => {
              const lineageFields = Object.entries(row).filter(([k]) => isLineageKey(k));
              if (lineageFields.length === 0) return null;
              return (
                <details>
                  <summary className="text-muted-foreground cursor-pointer text-xs font-medium uppercase tracking-wide">
                    Source &amp; lineage
                  </summary>
                  <div className="pt-3">
                    <FieldGrid fields={lineageFields} />
                  </div>
                </details>
              );
            })()}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

/** Fallback for countries with statementsQuery but no dedicated component. */
export function StatementsFallback({
  statements,
}: {
  statements: Record<string, unknown>[];
}) {
  if (statements.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Financial statements</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {statements.map((row, i) => {
          const visible = Object.entries(row).filter(([k]) => !isLineageKey(k));
          const lineage = Object.entries(row).filter(([k]) => isLineageKey(k));
          return (
            <div key={i} className="space-y-4">
              <FieldGrid fields={visible} />
              {lineage.length > 0 ? (
                <details>
                  <summary className="text-muted-foreground cursor-pointer text-xs font-medium uppercase tracking-wide">
                    Source &amp; lineage
                  </summary>
                  <div className="pt-3">
                    <FieldGrid fields={lineage} />
                  </div>
                </details>
              ) : null}
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}
