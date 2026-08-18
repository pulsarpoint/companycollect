import type { MoneyPair } from "~/components/financials/formatters";
import type { FinancialYearRow } from "~/lib/queries.server";

export type FinancialMoneyMetric =
  | "revenue"
  | "operatingResult"
  | "netResult"
  | "totalAssets"
  | "equity"
  | "liabilities"
  | "cashAndBank"
  | "currentAssets"
  | "currentLiabilities"
  | "personnelExpenses"
  | "wagesAndSalaries";

const metricFields = {
  revenue: ["revenue_amount_original", "revenue_amount_usd"],
  operatingResult: [
    "operating_result_amount_original",
    "operating_result_amount_usd",
  ],
  netResult: ["net_result_amount_original", "net_result_amount_usd"],
  totalAssets: ["total_assets_amount_original", "total_assets_amount_usd"],
  equity: ["equity_amount_original", "equity_amount_usd"],
  liabilities: ["liabilities_amount_original", "liabilities_amount_usd"],
  cashAndBank: ["cash_and_bank_amount_original", "cash_and_bank_amount_usd"],
  currentAssets: [
    "current_assets_amount_original",
    "current_assets_amount_usd",
  ],
  currentLiabilities: [
    "current_liabilities_amount_original",
    "current_liabilities_amount_usd",
  ],
  personnelExpenses: [
    "personnel_expenses_amount_original",
    "personnel_expenses_amount_usd",
  ],
  wagesAndSalaries: [
    "wages_and_salaries_amount_original",
    "wages_and_salaries_amount_usd",
  ],
} as const satisfies Record<
  FinancialMoneyMetric,
  readonly [keyof FinancialYearRow, keyof FinancialYearRow]
>;

export function financialMoney(
  row: FinancialYearRow,
  metric: FinancialMoneyMetric,
): MoneyPair {
  const [originalField, usdField] = metricFields[metric];
  return {
    original: row[originalField] as number | null | undefined,
    usd: row[usdField] as number | null | undefined,
    currency: row.currency || "SEK",
  };
}

export function calculationValue(
  row: FinancialYearRow,
  metric: FinancialMoneyMetric,
): number | null {
  const pair = financialMoney(row, metric);
  return pair.original ?? pair.usd ?? null;
}

export function percentage(
  numerator: number | null,
  denominator: number | null,
): number | null {
  if (numerator === null || denominator === null || denominator === 0) {
    return null;
  }
  return (numerator / denominator) * 100;
}

export function percentageChange(
  current: number | null,
  previous: number | null,
): number | null {
  if (current === null || previous === null || previous === 0) return null;
  return ((current - previous) / Math.abs(previous)) * 100;
}

export function hasFinancialData(row: FinancialYearRow): boolean {
  return (
    (Object.keys(metricFields) as FinancialMoneyMetric[]).some((metric) => {
      const pair = financialMoney(row, metric);
      return pair.original != null || pair.usd != null;
    }) || row.employees != null
  );
}

export function latestFinancialRows(
  financials: FinancialYearRow[],
  limit = 5,
): FinancialYearRow[] {
  return financials
    .filter(hasFinancialData)
    .sort((left, right) => Number(right.fiscal_year) - Number(left.fiscal_year))
    .slice(0, limit);
}
