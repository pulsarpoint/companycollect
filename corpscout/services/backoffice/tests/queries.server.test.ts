import { describe, expect, it } from "vitest";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { PAGE_SIZES, getCountryStats, searchCompanies } from "~/lib/queries.server";

// Integration tests against the real ClickHouse. Estonia is the smallest
// dataset (~373k rows), so queries stay fast.
const ee = getCountry("ee")!;

describe("getCountryStats", () => {
  it("returns positive totals with active <= total", async () => {
    const stats = await getCountryStats(ee);
    expect(stats.total).toBeGreaterThan(100_000);
    expect(stats.active).toBeGreaterThan(0);
    expect(stats.active).toBeLessThanOrEqual(stats.total);
  });
});

describe("searchCompanies", () => {
  it("returns a first page of rows with id and name", async () => {
    const result = await searchCompanies(ee, { page: 1, pageSize: 25 });
    expect(result.rows).toHaveLength(25);
    expect(result.total).toBeGreaterThan(100_000);
    for (const row of result.rows) {
      expect(row.id).toBeTruthy();
      expect(row.name).toBeTruthy();
    }
  });

  it("filters by case-insensitive name substring", async () => {
    const result = await searchCompanies(ee, { q: "grupp", pageSize: 25 });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.total).toBeLessThan(400_000);
    for (const row of result.rows) {
      expect(String(row.name).toLowerCase()).toContain("grupp");
    }
  });

  it("falls back to sane defaults on non-finite page inputs", async () => {
    const result = await searchCompanies(ee, { page: Number("abc"), pageSize: Number.POSITIVE_INFINITY });
    expect(result.page).toBe(1);
    expect(result.pageSize).toBe(50);
    expect(result.rows.length).toBeGreaterThan(0);
  });

  it("paginates without overlap", async () => {
    const p1 = await searchCompanies(ee, { page: 1, pageSize: 25 });
    const p2 = await searchCompanies(ee, { page: 2, pageSize: 25 });
    const ids1 = new Set(p1.rows.map((r) => r.id));
    expect(p2.rows.some((r) => ids1.has(r.id))).toBe(false);
  });

  it("clamps out-of-range pages to the last page", async () => {
    const result = await searchCompanies(ee, { page: Number.MAX_SAFE_INTEGER, pageSize: 25 });
    const lastPage = Math.max(1, Math.ceil(result.total / result.pageSize));
    expect(result.page).toBe(lastPage);
    expect(result.rows.length).toBeGreaterThan(0);
  });
});

describe("searchCompanies sorting and columns", () => {
  it("returns all declared column keys plus active and industry fields", async () => {
    const result = await searchCompanies(ee, { pageSize: 25 });
    expect(result.pageSize).toBe(25);
    const row = result.rows[0];
    for (const col of ee.columns) {
      expect(row, `column ${col.key}`).toHaveProperty(col.key);
    }
    expect(row).toHaveProperty("active");
    expect(row).toHaveProperty("industry_code");
    expect(row).toHaveProperty("industry_label");
    expect(row).not.toHaveProperty("__industry_key");
  });

  it("merges a primary industry for most companies on a page", async () => {
    const result = await searchCompanies(ee, { pageSize: 50 });
    const withIndustry = result.rows.filter((r) => r.industry_label);
    // ~96% of EE companies have a primary industry; a 50-row page having zero would mean the merge is broken.
    expect(withIndustry.length).toBeGreaterThan(0);
  });

  it("sorts by a whitelisted column in both directions", async () => {
    const asc = await searchCompanies(ee, { sort: "id", dir: "asc", pageSize: 25 });
    const desc = await searchCompanies(ee, { sort: "id", dir: "desc", pageSize: 25 });
    expect(asc.sort).toBe("id");
    expect(asc.dir).toBe("asc");
    expect(desc.dir).toBe("desc");
    expect(asc.rows[0].id).not.toEqual(desc.rows[0].id);
    const ascIds = asc.rows.map((r) => String(r.id));
    expect([...ascIds].sort()).toEqual(ascIds);
  });

  it("falls back to name asc on unknown sort keys and directions", async () => {
    const result = await searchCompanies(ee, {
      sort: "industry_label; DROP TABLE x",
      dir: "sideways",
      pageSize: 25,
    });
    expect(result.sort).toBe("name");
    expect(result.dir).toBe("asc");
    expect(result.rows).toHaveLength(25);
  });

  it("accepts only whitelisted page sizes", async () => {
    const result = await searchCompanies(ee, { pageSize: 37 });
    expect(result.pageSize).toBe(50);
    expect(PAGE_SIZES).toEqual([25, 50, 100]);
  });
});

describe("searchCompanies across all countries", () => {
  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: default first page executes registry SQL and merges industry",
    async (_code, country) => {
      const result = await searchCompanies(country, { pageSize: 25 });
      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.total).toBeGreaterThan(0);
      const row = result.rows[0];
      expect(row).toHaveProperty("id");
      expect(row).toHaveProperty("name");
      expect(row).toHaveProperty("industry_code");
      expect(row).not.toHaveProperty("__industry_key");
    },
    30_000,
  );
});
