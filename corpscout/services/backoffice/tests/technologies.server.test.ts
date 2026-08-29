/**
 * SQL-shape tests for `/admin/technologies`'s query builders: FINAL on every
 * catalog read, named params only, LIMITs everywhere, every weekly rollup
 * tolerated empty (a first population can be in flight), and the two detail
 * tab reads deduping their ReplacingMergeTree keys with argMax/GROUP BY.
 * ClickHouse is faked at the module boundary.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import * as technologiesServer from "~/lib/technologies.server";
import {
  countTechnologies,
  countTechnologyCompanies,
  countTechnologyDomains,
  listTechnologiesPage,
  loadTechnologyAdoption,
  loadTechnologyCategoryOptions,
  loadTechnologyCompaniesComputedAt,
  loadTechnologyCompaniesPage,
  loadTechnologyCompanyCountries,
  loadTechnologyDetail,
  loadTechnologyDomainsComputedAt,
  loadTechnologyDomainsPage,
  TECHNOLOGY_ADOPTION_SQL,
  TECHNOLOGY_ADOPTION_TABLE,
  TECHNOLOGY_CATALOG_TABLE,
  TECHNOLOGY_CATEGORY_OPTIONS_SQL,
  TECHNOLOGY_COMPANIES_COMPUTED_AT_SQL,
  TECHNOLOGY_COMPANIES_COUNT_SQL,
  TECHNOLOGY_COMPANIES_SELECT_SQL,
  TECHNOLOGY_COMPANIES_TABLE,
  TECHNOLOGY_COMPANY_COUNTRIES_SQL,
  TECHNOLOGY_DETAIL_SQL,
  TECHNOLOGY_LIST_SELECT_SQL,
  TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL,
  TECHNOLOGY_SE_COMPANY_NAMES_SQL,
  TECHNOLOGY_TOP_DOMAINS_COMPUTED_AT_SQL,
  TECHNOLOGY_TOP_DOMAINS_COUNT_SQL,
  TECHNOLOGY_TOP_DOMAINS_SQL,
  TECHNOLOGY_TOP_DOMAINS_TABLE,
} from "~/lib/technologies.server";

const NO_FILTERS = { q: "", category: "" };

beforeEach(() => clickhouse.query.mockReset());

describe("the catalog list page", () => {
  it("reads the catalog FINAL, LEFT JOINs the newest adoption row per technology, and pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const rows = await listTechnologiesPage(NO_FILTERS, 1, 50);

    expect(rows).toEqual([]);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(`FROM ${TECHNOLOGY_CATALOG_TABLE} AS catalog FINAL`);
    expect(sql).toContain("LEFT JOIN");
    expect(sql).toContain(`FROM ${TECHNOLOGY_ADOPTION_TABLE}`);
    expect(sql).toContain("argMax(domain_count, computed_at)");
    expect(sql).not.toContain("WHERE");
    expect(sql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(params).toEqual({ limit: 50, offset: 0 });
  });

  it("distinguishes 'rollup has no row' ('') from a real count, so an empty rollup renders as absent, not zero", () => {
    expect(TECHNOLOGY_LIST_SELECT_SQL).toContain(
      "if(adoption.technology != '', toString(adoption.domain_count), '') AS domain_count",
    );
  });

  it("searches the name with a parameterized ILIKE and filters categories with has(), never interpolating values", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await listTechnologiesPage({ q: "word", category: "CMS" }, 2, 100);

    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("catalog.technology ILIKE {q:String}");
    expect(sql).toContain("has(catalog.categories, {category:String})");
    expect(sql).not.toContain("word");
    expect(sql).not.toContain("CMS");
    expect(params).toEqual({ q: "%word%", category: "CMS", limit: 100, offset: 100 });
  });

  it("counts with the same WHERE against the catalog FINAL, without the adoption join", async () => {
    clickhouse.query.mockResolvedValueOnce([{ total: "7929" }]);
    const total = await countTechnologies({ q: "wp", category: "" });

    expect(total).toBe(7929);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(`FROM ${TECHNOLOGY_CATALOG_TABLE} AS catalog FINAL`);
    expect(sql).toContain("catalog.technology ILIKE {q:String}");
    expect(sql).not.toContain("JOIN");
    expect(params).toEqual({ q: "%wp%" });
  });

  it("offers the category filter from the catalog's own values, FINAL and capped", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { category: "CMS" },
      { category: "" },
      { category: "Ecommerce" },
    ]);
    const categories = await loadTechnologyCategoryOptions();

    expect(categories).toEqual(["CMS", "Ecommerce"]);
    expect(TECHNOLOGY_CATEGORY_OPTIONS_SQL).toContain(
      `FROM ${TECHNOLOGY_CATALOG_TABLE} FINAL`,
    );
    expect(TECHNOLOGY_CATEGORY_OPTIONS_SQL).toContain("arrayJoin(categories)");
    expect(TECHNOLOGY_CATEGORY_OPTIONS_SQL).toContain("LIMIT 500");
  });
});

describe("the detail read", () => {
  it("resolves one catalog row by slug, FINAL, LIMIT 1, slug as a named param", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        technology: "WordPress",
        slug: "wordpress",
        description: "A CMS.",
        website: "https://wordpress.org",
        categories: ["CMS", "Blogs"],
        has_icon: 1,
        saas: 0,
        oss: 1,
        pricing: ["freemium"],
        source: "wappalyzer",
        source_version: "2026.08",
        updated_at: "2026-08-20 00:00:00",
      },
    ]);
    const detail = await loadTechnologyDetail("wordpress");

    expect(TECHNOLOGY_DETAIL_SQL).toContain(
      `FROM ${TECHNOLOGY_CATALOG_TABLE} FINAL`,
    );
    expect(TECHNOLOGY_DETAIL_SQL).toContain("WHERE slug = {slug:String}");
    expect(TECHNOLOGY_DETAIL_SQL).toContain("LIMIT 1");
    expect(clickhouse.query).toHaveBeenCalledWith(TECHNOLOGY_DETAIL_SQL, {
      slug: "wordpress",
    });
    expect(detail).toEqual({
      technology: "WordPress",
      slug: "wordpress",
      description: "A CMS.",
      website: "https://wordpress.org",
      categories: ["CMS", "Blogs"],
      icon: true,
      saas: false,
      oss: true,
      pricing: ["freemium"],
      source: "wappalyzer",
      source_version: "2026.08",
      updated_at: "2026-08-20 00:00:00",
    });
  });

  it("returns null for an unknown slug -- the route's 404", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadTechnologyDetail("no-such-slug")).toBeNull();
  });
});

describe("the adoption rollup read", () => {
  it("GROUP BYs so an empty rollup yields no row -- null, never a fake zero", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const adoption = await loadTechnologyAdoption("WordPress");

    expect(adoption).toBeNull();
    expect(TECHNOLOGY_ADOPTION_SQL).toContain(
      `FROM ${TECHNOLOGY_ADOPTION_TABLE}`,
    );
    expect(TECHNOLOGY_ADOPTION_SQL).toContain("WHERE technology = {name:String}");
    expect(TECHNOLOGY_ADOPTION_SQL).toContain("GROUP BY technology");
    expect(clickhouse.query).toHaveBeenCalledWith(TECHNOLOGY_ADOPTION_SQL, {
      name: "WordPress",
    });
  });

  it("takes the newest weekly row (argMax over computed_at) when the rollup has rows", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        latest_domain_count: "123456",
        latest_computed_at: "2026-08-24 03:00:00",
      },
    ]);
    const adoption = await loadTechnologyAdoption("WordPress");

    expect(TECHNOLOGY_ADOPTION_SQL).toContain("argMax(domain_count, computed_at)");
    expect(adoption).toEqual({
      domainCount: 123456,
      computedAt: "2026-08-24 03:00:00",
    });
  });
});

describe("the Domains tab read (technology_top_domains)", () => {
  it("dedupes by root_domain with argMax over computed_at, keyed by the detector name, ordered by centrality desc, paged with named params", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        root_domain: "wordpress.org",
        latest_harmonic_rank: "1",
        latest_harmonic_centrality: 21534096.5,
      },
    ]);
    const rows = await loadTechnologyDomainsPage("WordPress", 2, 50);

    const sql = TECHNOLOGY_TOP_DOMAINS_SQL;
    expect(sql).toContain(`FROM ${TECHNOLOGY_TOP_DOMAINS_TABLE}`);
    expect(sql).toContain("WHERE technology = {name:String}");
    expect(sql).toContain("GROUP BY root_domain");
    expect(sql).toContain("argMax(harmonic_rank, computed_at)");
    expect(sql).toContain("argMax(harmonic_centrality, computed_at)");
    expect(sql).toContain("ORDER BY latest_harmonic_centrality DESC");
    expect(sql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(clickhouse.query).toHaveBeenCalledWith(sql, {
      name: "WordPress",
      limit: 50,
      offset: 50,
    });
    expect(rows).toEqual([
      {
        root_domain: "wordpress.org",
        harmonic_rank: "1",
        harmonic_centrality: 21534096.5,
      },
    ]);
  });

  it("counts distinct domains, matching the page's GROUP BY dedupe", async () => {
    clickhouse.query.mockResolvedValueOnce([{ total: "500" }]);
    const total = await countTechnologyDomains("WordPress");

    expect(total).toBe(500);
    expect(TECHNOLOGY_TOP_DOMAINS_COUNT_SQL).toContain(
      "uniqExact(root_domain)",
    );
    expect(TECHNOLOGY_TOP_DOMAINS_COUNT_SQL).toContain(
      "WHERE technology = {name:String}",
    );
    expect(clickhouse.query).toHaveBeenCalledWith(
      TECHNOLOGY_TOP_DOMAINS_COUNT_SQL,
      { name: "WordPress" },
    );
  });

  it("stamps the tab from max(computed_at), null while the rollup is mid first population", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadTechnologyDomainsComputedAt("WordPress")).toBeNull();

    clickhouse.query.mockResolvedValueOnce([
      { latest_computed_at: "2026-08-24 03:00:00" },
    ]);
    expect(await loadTechnologyDomainsComputedAt("WordPress")).toBe(
      "2026-08-24 03:00:00",
    );
    expect(TECHNOLOGY_TOP_DOMAINS_COMPUTED_AT_SQL).toContain("max(computed_at)");
    expect(TECHNOLOGY_TOP_DOMAINS_COMPUTED_AT_SQL).toContain(
      "GROUP BY technology",
    );
  });
});

describe("the Companies tab read (technology_companies)", () => {
  it("dedupes on the rollup key with GROUP BY, keyed by the detector name, no country filter when 'all'", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const rows = await loadTechnologyCompaniesPage("WordPress", "", 1, 50);

    expect(rows).toEqual([]);
    // No SE rows on the page: the name/industry lookups never run.
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(`FROM ${TECHNOLOGY_COMPANIES_TABLE}`);
    expect(sql).toContain("WHERE technology = {name:String}");
    expect(sql).toContain("GROUP BY country_code, company_id, root_domain");
    expect(sql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(sql).not.toContain("{country:String}");
    expect(params).toEqual({ name: "WordPress", limit: 50, offset: 0 });
  });

  it("applies the country filter as a named param, never interpolated", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await loadTechnologyCompaniesPage("WordPress", "SE", 2, 50);

    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("AND country_code = {country:String}");
    expect(sql).not.toContain("'SE'");
    expect(params).toEqual({
      name: "WordPress",
      country: "SE",
      limit: 50,
      offset: 50,
    });
  });

  it("enriches SE rows with se_companies FINAL names and NACE industries keyed by the page's own ids; non-SE rows stay bare", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        { country_code: "SE", company_id: "5560125220", root_domain: "example.se" },
        { country_code: "SE", company_id: "5560125220", root_domain: "other.se" },
        { country_code: "NO", company_id: "923609016", root_domain: "example.no" },
      ])
      .mockResolvedValueOnce([
        { company_id: "5560125220", legal_name: "Example AB" },
      ])
      .mockResolvedValueOnce([
        { company_id: "5560125220", code: "62.010", label: "Computer programming", is_primary: 1 },
        { company_id: "5560125220", code: "63.110", label: "Data processing", is_primary: 0 },
      ]);
    const rows = await loadTechnologyCompaniesPage("WordPress", "", 1, 50);

    // One page query + one names + one industries lookup, ids DEDUPED.
    expect(clickhouse.query).toHaveBeenCalledTimes(3);
    expect(clickhouse.query).toHaveBeenNthCalledWith(
      2,
      TECHNOLOGY_SE_COMPANY_NAMES_SQL,
      { ids: ["5560125220"] },
    );
    expect(clickhouse.query).toHaveBeenNthCalledWith(
      3,
      TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL,
      { ids: ["5560125220"] },
    );
    expect(TECHNOLOGY_SE_COMPANY_NAMES_SQL).toContain(
      "FROM corpscout.se_companies FINAL",
    );
    expect(TECHNOLOGY_SE_COMPANY_NAMES_SQL).toContain(
      "WHERE company_id IN {ids:Array(String)}",
    );
    expect(TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL).toContain(
      "FROM corpscout.se_company_industry_display_current",
    );
    expect(TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL).toContain(
      "WHERE company_id IN {ids:Array(String)}",
    );
    // Primary classification first.
    expect(TECHNOLOGY_SE_COMPANY_INDUSTRIES_SQL).toContain("is_primary DESC");

    const industries = [
      { code: "62.010", label: "Computer programming", is_primary: 1 },
      { code: "63.110", label: "Data processing", is_primary: 0 },
    ];
    expect(rows).toEqual([
      {
        country_code: "SE",
        company_id: "5560125220",
        root_domain: "example.se",
        legal_name: "Example AB",
        industries,
      },
      {
        country_code: "SE",
        company_id: "5560125220",
        root_domain: "other.se",
        legal_name: "Example AB",
        industries,
      },
      // Non-SE: no name/industry source yet -- the id stands in for the name
      // at render time, industries stay empty (the extension point).
      {
        country_code: "NO",
        company_id: "923609016",
        root_domain: "example.no",
        legal_name: "",
        industries: [],
      },
    ]);
  });

  it("counts distinct rollup keys under the same country filter as the page", async () => {
    clickhouse.query.mockResolvedValueOnce([{ total: "1234" }]);
    const total = await countTechnologyCompanies("WordPress", "SE");

    expect(total).toBe(1234);
    expect(TECHNOLOGY_COMPANIES_COUNT_SQL).toContain(
      "uniqExact(country_code, company_id, root_domain)",
    );
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("AND country_code = {country:String}");
    expect(params).toEqual({ name: "WordPress", country: "SE" });
  });

  it("offers the country filter's options from the rollup's own DISTINCT values for this technology", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { country_code: "NO" },
      { country_code: "SE" },
      { country_code: "" },
    ]);
    const countries = await loadTechnologyCompanyCountries("WordPress");

    expect(countries).toEqual(["NO", "SE"]);
    expect(TECHNOLOGY_COMPANY_COUNTRIES_SQL).toContain("SELECT DISTINCT");
    expect(TECHNOLOGY_COMPANY_COUNTRIES_SQL).toContain(
      `FROM ${TECHNOLOGY_COMPANIES_TABLE}`,
    );
    expect(TECHNOLOGY_COMPANY_COUNTRIES_SQL).toContain(
      "WHERE technology = {name:String}",
    );
    expect(clickhouse.query).toHaveBeenCalledWith(
      TECHNOLOGY_COMPANY_COUNTRIES_SQL,
      { name: "WordPress" },
    );
  });

  it("stamps the tab from max(computed_at), null while the rollup is mid first population", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadTechnologyCompaniesComputedAt("WordPress")).toBeNull();
    expect(TECHNOLOGY_COMPANIES_COMPUTED_AT_SQL).toContain(
      `FROM ${TECHNOLOGY_COMPANIES_TABLE}`,
    );
    expect(TECHNOLOGY_COMPANIES_COMPUTED_AT_SQL).toContain("GROUP BY technology");
  });

  it("the base page read exposes the key columns only -- payload enrichment stays per-page and per-country", () => {
    expect(TECHNOLOGY_COMPANIES_SELECT_SQL).toContain("country_code");
    expect(TECHNOLOGY_COMPANIES_SELECT_SQL).toContain("company_id");
    expect(TECHNOLOGY_COMPANIES_SELECT_SQL).toContain("root_domain");
  });
});

describe("the retired live SE companies query", () => {
  it("is gone: no export touches commoncrawl_page_technologies, and the old loader no longer exists", () => {
    expect("loadSeCompaniesUsingTechnology" in technologiesServer).toBe(false);
    expect("SE_COMPANIES_USING_TECHNOLOGY_SQL" in technologiesServer).toBe(false);
    for (const [name, value] of Object.entries(technologiesServer)) {
      if (typeof value !== "string") continue;
      expect(value, `${name} must not touch the 10.6B-row live table`).not.toContain(
        "commoncrawl_page_technologies",
      );
    }
  });
});
