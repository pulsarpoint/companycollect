import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { getFacetOptions } from "~/lib/facets.server";
import { filterableFacetKeys } from "~/lib/filters";
import { PAGE_SIZES, getCompanyDetail, getCountryStats, searchCompanies } from "~/lib/queries.server";

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

  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: first facet loads and filters companies",
    async (_code, country) => {
      const keys = filterableFacetKeys(country).filter((k) => k !== "industry");
      const facetKey = keys[0];
      const options = await getFacetOptions(country, facetKey);
      expect(options.length).toBeGreaterThan(0);
      const filtered = await searchCompanies(country, {
        pageSize: 25,
        filters: { [facetKey]: [options[0].value] },
      });
      expect(filtered.total).toBeGreaterThan(0);
    },
    60_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.industryFilterExpr).map((c) => [c.code, c] as const),
  )(
    "%s: industry facet loads and filters companies",
    async (_code, country) => {
      const options = await getFacetOptions(country, "industry");
      expect(options.length).toBeGreaterThan(0);
      const filtered = await searchCompanies(country, {
        pageSize: 25,
        filters: { industry: [options[0].value] },
      });
      expect(filtered.total).toBeGreaterThan(0);
    },
    120_000,
  );
});

describe("searchCompanies with filters", () => {
  it("applies a single-value filter to rows and total", async () => {
    const unfiltered = await searchCompanies(ee, { pageSize: 25 });
    const statusOptions = await getFacetOptions(ee, "status");
    const top = statusOptions[0].value;
    const filtered = await searchCompanies(ee, {
      pageSize: 25,
      filters: { status: [top] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThanOrEqual(unfiltered.total);
    for (const row of filtered.rows) {
      expect(String(row.status)).toBe(top);
    }
  });

  it("multi-value filter is a union (IN)", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    if (statusOptions.length < 2) return; // data-dependent guard
    const [a, b] = statusOptions;
    const fa = await searchCompanies(ee, { filters: { status: [a.value] } });
    const fb = await searchCompanies(ee, { filters: { status: [b.value] } });
    const both = await searchCompanies(ee, {
      filters: { status: [a.value, b.value] },
    });
    expect(both.total).toBe(fa.total + fb.total);
  });

  it("filters compose with q search", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    const result = await searchCompanies(ee, {
      q: "grupp",
      filters: { status: [statusOptions[0].value] },
    });
    for (const row of result.rows) {
      expect(String(row.name).toLowerCase()).toContain("grupp");
      expect(String(row.status)).toBe(statusOptions[0].value);
    }
  });

  it("ignores filter keys not in the registry whitelist", async () => {
    const result = await searchCompanies(ee, {
      pageSize: 25,
      filters: { "bogus; DROP": ["x"], name: ["y"] } as never,
    });
    expect(result.rows.length).toBe(25); // filters silently ignored
  });
});

describe("industry filter (Estonia)", () => {
  it("filters companies by primary industry code", async () => {
    const options = await getFacetOptions(ee, "industry");
    const top = options[0];
    const filtered = await searchCompanies(ee, {
      filters: { industry: [top.value] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThan(400_000);
    for (const row of filtered.rows) {
      expect(row.industry_code).toBe(top.value);
    }
  });
});

describe("getCompanyDetail (Estonia)", () => {
  it("returns null for an unknown id", async () => {
    const detail = await getCompanyDetail(ee, "does-not-exist-000");
    expect(detail).toBeNull();
  });

  it("returns company row with industry for a real id", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const id = String(page.rows[0].id);
    const detail = await getCompanyDetail(ee, id);
    expect(detail).not.toBeNull();
    expect(String(detail!.company.id)).toBe(id);
    expect(detail!.company).toHaveProperty("name");
    expect(detail!.company).toHaveProperty("active");
    expect(detail!.company).toHaveProperty("industry_code");
    expect(detail!.company).not.toHaveProperty("__industry_key");
  });

  it("returns canonical financials for a company that has them", async () => {
    const [row] = await chQuery<{ id: string }>(
      "SELECT reg_code AS id FROM ee_financial_metrics WHERE revenue_amount_usd IS NOT NULL LIMIT 1",
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.financials.length).toBeGreaterThan(0);
    const year = detail!.financials[0];
    expect(typeof year.fiscal_year).toBe("string");
    expect(typeof year.revenue_amount_usd).toBe("number"); // toFloat64 → JSON number, not string
    const years = detail!.financials.map((f) => f.fiscal_year);
    expect([...years].sort().reverse()).toEqual(years); // newest first
  });

  it("returns contacts and domains for companies that have them", async () => {
    const [c] = await chQuery<{ id: string }>(
      "SELECT registry_id AS id FROM ee_company_contacts WHERE is_current = 1 LIMIT 1",
    );
    const withContacts = await getCompanyDetail(ee, c.id);
    expect(withContacts!.contacts.length).toBeGreaterThan(0);
    expect(withContacts!.contacts[0]).toHaveProperty("contact_type");
    expect(withContacts!.contacts[0]).toHaveProperty("contact_value");

    const [d] = await chQuery<{ id: string }>(
      "SELECT registry_id AS id FROM ee_company_domains WHERE is_current = 1 LIMIT 1",
    );
    const withDomains = await getCompanyDetail(ee, d.id);
    expect(withDomains!.domains.length).toBeGreaterThan(0);
    expect(withDomains!.domains[0].domain).toBeTruthy();
  });
});
