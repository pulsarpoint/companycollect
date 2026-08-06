import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { FinancialsSection } from "~/components/detail/financials-section";
import type { FinancialYearRow } from "~/lib/queries.server";

function financialRow(
  fiscalYear: string,
  values: Partial<FinancialYearRow> = {},
): FinancialYearRow {
  return {
    fiscal_year: fiscalYear,
    currency: "SEK",
    revenue_amount_original: null,
    revenue_amount_usd: null,
    net_result_amount_original: null,
    net_result_amount_usd: null,
    total_assets_amount_original: null,
    total_assets_amount_usd: null,
    equity_amount_original: null,
    equity_amount_usd: null,
    employees: null,
    ...values,
  };
}

describe("FinancialsSection", () => {
  it("shows empty filing years as information instead of table rows", () => {
    const html = renderToStaticMarkup(
      <FinancialsSection
        financials={[financialRow("2025"), financialRow("2024")]}
      />,
    );

    expect(html).toContain("No financial data available for 2025.");
    expect(html).toContain("No financial data available for 2024.");
    expect(html).not.toContain("<table");
  });

  it("keeps populated years in the table and empty years outside it", () => {
    const html = renderToStaticMarkup(
      <FinancialsSection
        financials={[
          financialRow("2025"),
          financialRow("2024", { revenue_amount_original: 45_052_000_000 }),
        ]}
      />,
    );
    const tableBody = html.match(/<tbody[^>]*>([\s\S]*?)<\/tbody>/)?.[1] ?? "";

    expect(html).toContain("No financial data available for 2025.");
    expect(tableBody).toContain("2024");
    expect(tableBody).not.toContain("2025");
  });
});
