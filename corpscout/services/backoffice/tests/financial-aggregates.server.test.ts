import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import {
  getCountryFinancials,
  getDivisionLabels,
  getGlobalFinancialOverview,
  getIndustryFinancials,
} from "~/lib/financial-aggregates.server";

/** Wraps a page-level call and logs its wall time for the timing report. */
async function timed<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const start = Date.now();
  try {
    return await fn();
  } finally {
    console.log(`[timing] ${label}: ${Date.now() - start}ms`);
  }
}

describe("division labels", () => {
  it("loads ~87 divisions with code-stripped labels", async () => {
    const labels = await timed("getDivisionLabels", () => getDivisionLabels());
    expect(labels.size).toBeGreaterThanOrEqual(85);
    expect(labels.get("62")).toMatch(/programming|computer/i);
    expect(labels.get("62")).not.toMatch(/^62 /);
  }, 30_000);
});

describe("getGlobalFinancialOverview", () => {
  it("covers all summary countries and puts Equinor in the global top", async () => {
    const overview = await timed("getGlobalFinancialOverview", () => getGlobalFinancialOverview());
    expect(overview.countries.map((c) => c.country_code).sort()).toEqual(
      ["br", "ee", "fi", "gb", "lv", "no", "se", "sk"],
    );
    const names = overview.topCompanies.map((c) => c.name);
    expect(names.some((n) => /EQUINOR/i.test(n))).toBe(true);
    expect(overview.topDivisions.length).toBeGreaterThan(5);
  }, 60_000);

  it("returns countries in a deterministic order across repeated calls", async () => {
    const [first, second] = await Promise.all([
      getGlobalFinancialOverview(),
      getGlobalFinancialOverview(),
    ]);
    expect(second.countries.map((c) => c.country_code)).toEqual(
      first.countries.map((c) => c.country_code),
    );
    // Sorted by revenue_usd desc (nulls last), country_code asc tiebreak.
    for (let i = 1; i < first.countries.length; i++) {
      const prev = first.countries[i - 1];
      const cur = first.countries[i];
      if (prev.revenue_usd == null) {
        expect(cur.revenue_usd).toBeNull();
      } else if (cur.revenue_usd != null) {
        expect(prev.revenue_usd).toBeGreaterThanOrEqual(cur.revenue_usd);
      }
    }
  }, 60_000);
});

describe("getCountryFinancials", () => {
  it("norway: NUF exclusion reduces the sum and divisions cover most companies", async () => {
    const [no, naiveCountRows] = await Promise.all([
      timed("getCountryFinancials(no)", () => getCountryFinancials("no")),
      chQuery<{ naive_count: number | string }>(
        "SELECT count() AS naive_count FROM no_company_financials_latest",
      ),
    ]);
    const naiveCount = Number(naiveCountRows[0].naive_count);
    expect(no).not.toBeNull();
    // 3,423 NUF companies carry ~$65bn — the excluded total must be well below
    // the naive sum (which would exceed $300bn with branches included).
    expect(no!.totals.revenue_usd).toBeGreaterThan(100e9);
    // NUF exclusion must shrink the count below the naive total-row count,
    // but only by NUF-scale (a few thousand rows) — not by some unrelated,
    // much larger amount that would signal a different bug.
    expect(no!.totals.companies).toBeLessThan(naiveCount);
    expect(no!.totals.companies).toBeGreaterThan(naiveCount - 10_000);
    const mapped = no!.divisions!.reduce((s, d) => s + d.companies, 0);
    expect(mapped + (no!.unmapped?.companies ?? 0)).toBe(no!.totals.companies);
    // NUF companies still appear in lists when they rank (AWS EMEA does):
    const aws = no!.topCompanies.find((c) => /AMAZON WEB SERVICES/i.test(c.name));
    expect(aws?.excluded_from_sums).toBe(true);
  }, 60_000);

  it("finland has totals and top companies but no division breakdown", async () => {
    const fi = await timed("getCountryFinancials(fi)", () => getCountryFinancials("fi"));
    expect(fi).not.toBeNull();
    expect(fi!.divisions).toBeNull();
    expect(fi!.topCompanies.length).toBeGreaterThan(10);
  }, 30_000);

  it("returns null for countries without financials", async () => {
    expect(await getCountryFinancials("fr")).toBeNull();
    expect(await getCountryFinancials("nope")).toBeNull();
  });
});

describe("getIndustryFinancials", () => {
  it("real estate (68) spans multiple countries with real revenue", async () => {
    const re = await timed("getIndustryFinancials(68)", () => getIndustryFinancials("68"));
    expect(re).not.toBeNull();
    expect(re!.label).toMatch(/real estate/i);
    const codes = re!.countries.map((c) => c.country_code);
    expect(codes).toContain("no");
    expect(codes).toContain("se");
    expect(re!.topCompanies.length).toBeGreaterThan(10);
    expect(re!.topCompanies[0].revenue_usd).toBeGreaterThan(1e8);
  }, 60_000);

  it("scopes industry totals and companies to the requested country", async () => {
    const wholesale = await timed("getIndustryFinancials(46, se)", () =>
      getIndustryFinancials("46", "se"),
    );

    expect(wholesale).not.toBeNull();
    expect(wholesale!.countryCode).toBe("se");
    expect(wholesale!.countries.map((country) => country.country_code)).toEqual(["se"]);
    expect(wholesale!.countries[0].companies).toBeGreaterThan(0);
    expect(wholesale!.topCompanies.length).toBeGreaterThan(0);
    expect(
      wholesale!.topCompanies.every((company) => company.country_code === "se"),
    ).toBe(true);
  }, 60_000);

  it("rejects garbage division codes", async () => {
    expect(await getIndustryFinancials("9x")).toBeNull();
    expect(await getIndustryFinancials("999")).toBeNull();
    expect(await getIndustryFinancials("46", "xx")).toBeNull();
  });
});
