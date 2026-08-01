import { describe, expect, it } from "vitest";
import {
  balanceLabel,
  formatRatio,
  isWithheld,
} from "~/components/detail/countries/fr-financials";
import type { FrFinancialRow } from "~/lib/queries.server";

function row(over: Partial<FrFinancialRow> = {}): FrFinancialRow {
  return {
    fiscal_year: "2024",
    balance_type: "C",
    confidentiality: "Public",
    currency: "EUR",
    revenue_original: 121958,
    revenue_usd: 130714.58,
    gross_margin_original: 121958,
    ebitda_original: -21641,
    ebit_original: -21643,
    net_income_original: 1041528,
    net_income_usd: 1116309.71,
    ebitda_margin_percent: -17.745,
    debt_ratio_percent: 49.275,
    financial_autonomy_percent: 66.042,
    liquidity_ratio_percent: 478.205,
    interest_coverage_percent: -1122.212,
    customer_payment_days: 48.287,
    supplier_payment_days: 232.315,
    inventory_turnover_days: 0,
    ...over,
  };
}

describe("balanceLabel", () => {
  it("names the three filing bases", () => {
    expect(balanceLabel("C")).toBe("Complete");
    expect(balanceLabel("S")).toBe("Simplified");
    expect(balanceLabel("K")).toBe("Consolidated");
  });

  it("passes an unknown code through rather than inventing a name", () => {
    expect(balanceLabel("X")).toBe("X");
  });
});

describe("isWithheld", () => {
  it("is false for a public filing", () => {
    expect(isWithheld(row())).toBe(false);
  });

  it("is true for every non-public status", () => {
    // 23.5% of French filings are partially confidential and may legally omit
    // lines. A blank there means withheld, and the badge is what says so.
    expect(isWithheld(row({ confidentiality: "Partiellement confidentiel" }))).toBe(true);
    expect(
      isWithheld(row({ confidentiality: "Partiellement confidentiel (RAPCAC)" })),
    ).toBe(true);
    expect(isWithheld(row({ confidentiality: "Publication simplifiee" }))).toBe(true);
  });
});

describe("formatRatio", () => {
  // Values chosen to sit clear of a half-way point. 49.275 at one decimal is
  // decided by whether the double is a hair above or below .275, so asserting
  // on it tests the floating-point representation rather than the formatter.
  it("renders a percentage to one decimal", () => {
    expect(formatRatio(49.2, "percent")).toBe("49.2%");
  });

  it("rounds to one decimal", () => {
    expect(formatRatio(48.26, "days")).toBe("48.3 d");
  });

  it("renders days with a unit", () => {
    expect(formatRatio(48.3, "days")).toBe("48.3 d");
  });

  it("renders a bare ratio without a unit", () => {
    expect(formatRatio(1.42, "ratio")).toBe("1.42");
  });

  it("keeps a genuine zero visible", () => {
    // inventory_turnover_days is legitimately 0 for a service company. If this
    // renders as the withheld dash, the page claims the figure is missing.
    expect(formatRatio(0, "days")).toBe("0.0 d");
  });

  it("renders null as a dash, never as zero", () => {
    expect(formatRatio(null, "percent")).toBe("—");
    expect(formatRatio(null, "days")).toBe("—");
  });

  it("keeps negative figures signed", () => {
    expect(formatRatio(-17.7, "percent")).toBe("-17.7%");
  });
});
