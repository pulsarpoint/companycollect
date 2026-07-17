import { describe, expect, it } from "vitest";
import { getUnifiedFacetOptions, searchUnifiedCompanies, searchUnifiedFacetOptions } from "~/lib/unified.server";
import { getFacetOptions } from "~/lib/facets.server";
import { getCountry } from "~/lib/countries";

describe("searchUnifiedCompanies", () => {
  it("default page: 50 rows, registry order, total spans all countries", async () => {
    const result = await searchUnifiedCompanies({});
    expect(result.rows).toHaveLength(50);
    expect(result.sort).toBe("country");
    expect(result.total).toBeGreaterThan(100_000_000);
    expect(result.rows[0].country_code).toBe("br"); // 'br' sorts first, 68.6M rows
    for (const row of result.rows) {
      expect(row).toHaveProperty("industry_code");
      expect(row).not.toHaveProperty("__ik");
    }
  }, 30_000);

  it("country filter restricts branches", async () => {
    const result = await searchUnifiedCompanies({ filters: { country: ["ee"] } });
    expect(result.rows.every((r) => r.country_code === "ee")).toBe(true);
    expect(result.total).toBeGreaterThan(300_000);
    expect(result.total).toBeLessThan(500_000);
  }, 30_000);

  it("capability exclusion: a size filter restricts to brazil implicitly", async () => {
    const sizes = await getFacetOptions(getCountry("br")!, "size");
    const result = await searchUnifiedCompanies({ filters: { size: [sizes[0].value] }, pageSize: 25 });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((r) => r.country_code === "br")).toBe(true);
  }, 60_000);

  it("column filter + country filter compose", async () => {
    const statuses = await getFacetOptions(getCountry("ee")!, "status");
    const result = await searchUnifiedCompanies({
      filters: { country: ["ee"], status: [statuses[0].value] },
      pageSize: 25,
    });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((r) => r.country_code === "ee")).toBe(true);
    expect(result.total).toBeLessThan(400_000);
  }, 30_000);

  it("q search hits across countries", async () => {
    const result = await searchUnifiedCompanies({ q: "petrobras", pageSize: 25 });
    expect(result.total).toBeGreaterThan(0);
    expect(result.rows.some((r) => r.country_code === "br")).toBe(true);
  }, 60_000);

  it("no matching branch returns empty", async () => {
    // size is BR-only; country ee cannot answer it
    const result = await searchUnifiedCompanies({ filters: { country: ["ee"], size: ["x"] } });
    expect(result.rows).toEqual([]);
    expect(result.total).toBe(0);
  });

  it("revenue sort surfaces real USD revenues descending, empties last", async () => {
    const result = await searchUnifiedCompanies({ sort: "revenue", dir: "desc", pageSize: 25 });
    expect(result.rows.length).toBe(25);
    const revs = result.rows.map((r) => r.revenue_usd);
    expect(revs[0]).toBeGreaterThan(1_000_000);
    for (let i = 1; i < revs.length; i++) {
      if (revs[i] != null && revs[i - 1] != null) expect(revs[i - 1]! >= revs[i]!).toBe(true);
    }
  }, 60_000);

  it("has_financials filter restricts to companies with summary rows", async () => {
    const result = await searchUnifiedCompanies({ filters: { has_financials: ["true"], country: ["no"] } });
    expect(result.total).toBeGreaterThan(400_000);
    expect(result.total).toBeLessThan(500_000);
  }, 30_000);

  it("has_financials excludes countries without a summary table", async () => {
    const result = await searchUnifiedCompanies({ filters: { has_financials: ["true"], country: ["fr"] } });
    expect(result.total).toBe(0);
  });

  it("default sort still returns revenue fields on rows", async () => {
    const result = await searchUnifiedCompanies({ pageSize: 25 });
    for (const row of result.rows) {
      expect(row).toHaveProperty("revenue_usd");
    }
  }, 30_000);
});

describe("unified facets", () => {
  it("country facet lists all 10 with live counts", async () => {
    const options = await getUnifiedFacetOptions("country");
    expect(options).toHaveLength(10);
    const br = options.find((o) => o.value === "br");
    expect(br!.count).toBeGreaterThan(60_000_000);
    expect(br!.label).toBe("Brazil");
  }, 30_000);

  it("merged status options sum counts across countries", async () => {
    const options = await getUnifiedFacetOptions("status");
    expect(options.length).toBeGreaterThan(0);
    const counts = options.map((o) => o.count);
    expect([...counts].sort((a, b) => b - a)).toEqual(counts);
  }, 60_000);

  it("typeahead ranks merged options", async () => {
    const options = await searchUnifiedFacetOptions("country", "esto");
    expect(options[0]?.value).toBe("ee");
  }, 30_000);

  it("rejects unknown facet keys", async () => {
    await expect(getUnifiedFacetOptions("name")).rejects.toThrow(/unknown facet/i);
  });
});
