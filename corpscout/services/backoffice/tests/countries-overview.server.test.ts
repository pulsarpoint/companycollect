import { describe, expect, it } from "vitest";
import { COUNTRIES } from "~/lib/countries";
import { getCountryDirectory, getCountryIndustryGroups } from "~/lib/countries-overview.server";

describe("country directory", () => {
  it("merges registry coverage and financial totals for all configured countries", async () => {
    const rows = await getCountryDirectory();
    expect(rows.map((row) => row.country_code).sort()).toEqual(
      COUNTRIES.map((country) => country.code).sort(),
    );
    for (const row of rows) {
      expect(row.total_companies).toBeGreaterThan(0);
      expect(row.companies_with_financials).toBeGreaterThanOrEqual(0);
      expect(row.companies_with_financials).toBeLessThanOrEqual(row.total_companies);
    }

    const norway = rows.find((row) => row.country_code === "no")!;
    expect(norway.revenue_usd).toBeGreaterThan(100e9);
    expect(norway.latest_fiscal_year).toBeGreaterThanOrEqual(2023);
    expect(rows.find((row) => row.country_code === "fr")!.revenue_usd).toBeNull();
  }, 60_000);

  it("returns country-scoped fallback industry groups", async () => {
    const groups = await getCountryIndustryGroups("fr");
    expect(groups.length).toBeGreaterThan(0);
    expect(groups.length).toBeLessThanOrEqual(15);
    expect(groups[0].companies).toBeGreaterThan(0);
  }, 30_000);
});
