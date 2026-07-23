import { describe, expect, it } from "vitest";
import { COUNTRIES } from "~/lib/countries";
import {
  getCountryEurostatBusinessStats,
  getCountryImfOutlook,
  getCountryTradeStatistics,
  getCountryWorldBankStatistics,
} from "~/lib/country-statistics.server";
import {
  WORLD_BANK_INDICATORS,
} from "~/lib/country-statistics";

describe("country statistics", () => {
  it("defines source identifiers for every configured country", () => {
    expect(new Set(COUNTRIES.map((country) => country.iso3)).size).toBe(COUNTRIES.length);
    for (const country of COUNTRIES) {
      expect(country.code).toMatch(/^[a-z]{2}$/);
      expect(country.iso3).toMatch(/^[A-Z]{3}$/);
    }
  });

  it("returns World Bank time series with independent latest years", async () => {
    const statistics = await getCountryWorldBankStatistics("fr");
    const gdp = statistics.series.find(
      (series) => series.indicatorCode === WORLD_BANK_INDICATORS.gdp,
    );
    const unemployment = statistics.series.find(
      (series) => series.indicatorCode === WORLD_BANK_INDICATORS.unemployment,
    );

    expect(gdp?.points.length).toBeGreaterThan(20);
    expect(gdp?.latest.year).toBeGreaterThanOrEqual(2023);
    expect(gdp?.latest.value).toBeGreaterThan(1e12);
    expect(unemployment?.latest.year).toBeGreaterThanOrEqual(2023);
    expect(statistics.sourceUpdatedDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  }, 30_000);

  it("returns annual UN Comtrade totals and derives the balance", async () => {
    const trade = await getCountryTradeStatistics("FRA");

    expect(trade.points.length).toBeGreaterThanOrEqual(10);
    expect(trade.latest?.exportsUsd).toBeGreaterThan(100e9);
    expect(trade.latest?.importsUsd).toBeGreaterThan(100e9);
    expect(trade.latest?.balanceUsd).toBeCloseTo(
      trade.latest!.exportsUsd! - trade.latest!.importsUsd!,
      2,
    );
  }, 30_000);

  it("reports Eurostat business coverage without inventing non-European data", async () => {
    const france = COUNTRIES.find((country) => country.code === "fr")!;
    const brazil = COUNTRIES.find((country) => country.code === "br")!;
    const [franceStats, brazilStats] = await Promise.all([
      getCountryEurostatBusinessStats(france),
      getCountryEurostatBusinessStats(brazil),
    ]);

    expect(franceStats.coverage).toBe("full");
    expect(franceStats.metrics.find((metric) => metric.key === "activeEnterprises")?.value)
      .toBeGreaterThan(1_000_000);
    expect(franceStats.sizeRows.length).toBeGreaterThan(0);
    expect(brazilStats).toEqual({
      coverage: "none",
      datasetCount: 0,
      metrics: [],
      sizeRows: [],
      latestYear: null,
    });
  }, 30_000);

  it("keeps IMF unavailable as a valid empty state", async () => {
    const outlook = await getCountryImfOutlook("FRA");

    if (outlook.series.length === 0) {
      expect(outlook.vintageDate).toBeNull();
      return;
    }

    expect(outlook.vintageDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(outlook.series.every((series) => series.points.length > 0)).toBe(true);
    expect(
      outlook.series.flatMap((series) => series.points).some((point) => point.isEstimate),
    ).toBe(true);
  }, 30_000);
});
