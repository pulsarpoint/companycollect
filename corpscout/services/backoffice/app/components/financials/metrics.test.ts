import { describe, expect, it } from "vitest";
import { sourceFinancialRows } from "~/components/financials/metrics";
import type { FinancialYearRow } from "~/lib/queries.server";

function financialRow(
  fiscalYear: string,
  observation: FinancialYearRow["observation"],
): FinancialYearRow {
  return {
    fiscal_year: fiscalYear,
    currency: "SEK",
    revenue_amount_original: 1,
    revenue_amount_usd: 0.1,
    net_result_amount_original: null,
    net_result_amount_usd: null,
    total_assets_amount_original: null,
    total_assets_amount_usd: null,
    equity_amount_original: null,
    equity_amount_usd: null,
    employees: null,
    observation,
  };
}

describe("sourceFinancialRows", () => {
  it("returns every direct filing and excludes comparative observations", () => {
    const rows = sourceFinancialRows([
      financialRow("2023", "filed"),
      financialRow("2025", "filed"),
      financialRow("2024", "filed"),
      financialRow("2022", "comparative"),
    ]);

    expect(rows.map((row) => row.fiscal_year)).toEqual([
      "2025",
      "2024",
      "2023",
    ]);
  });
});
