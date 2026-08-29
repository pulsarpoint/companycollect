/**
 * SQL-shape tests for `/admin/technologies`'s query builders: FINAL on every
 * catalog read, named params only, LIMITs everywhere, the rollup tolerated
 * empty, and the SE companies query key-pruned through the IN-subquery on
 * company_domains. ClickHouse is faked at the module boundary.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import { SE_COMPANIES_USING_TECHNOLOGY_LIMIT } from "~/lib/technologies";
import {
  countTechnologies,
  listTechnologiesPage,
  loadSeCompaniesUsingTechnology,
  loadTechnologyAdoption,
  loadTechnologyCategoryOptions,
  loadTechnologyDetail,
  SE_COMPANIES_USING_TECHNOLOGY_SQL,
  TECHNOLOGY_ADOPTION_SQL,
  TECHNOLOGY_ADOPTION_TABLE,
  TECHNOLOGY_CATALOG_TABLE,
  TECHNOLOGY_CATEGORY_OPTIONS_SQL,
  TECHNOLOGY_DETAIL_SQL,
  TECHNOLOGY_LIST_SELECT_SQL,
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

describe("the live SE companies query", () => {
  it("prunes the 10.6B-row table through the IN-subquery on company_domains' SE root_domains, name as a named param", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await loadSeCompaniesUsingTechnology("WordPress");

    const sql = SE_COMPANIES_USING_TECHNOLOGY_SQL;
    expect(sql).toContain("FROM corpscout.commoncrawl_page_technologies");
    expect(sql).toContain("root_domain IN (");
    expect(sql).toContain("FROM corpscout.company_domains");
    expect(sql).toContain("WHERE country_code = 'SE'");
    expect(sql).toContain("AND technology = {name:String}");
    expect(sql).not.toContain("WordPress");
    expect(clickhouse.query).toHaveBeenCalledWith(sql, { name: "WordPress" });
  });

  it("joins back to company_domains FINAL for (company_id, root_domain) and caps the list", () => {
    const sql = SE_COMPANIES_USING_TECHNOLOGY_SQL;
    expect(sql).toContain("FROM corpscout.company_domains AS domains FINAL");
    expect(sql).toContain("domains.country_code = 'SE'");
    expect(sql).toContain("domains.company_id AS company_id");
    expect(sql).toContain("domains.root_domain AS root_domain");
    expect(sql).toContain("companies.legal_name AS legal_name");
    expect(sql).toContain(`LIMIT ${SE_COMPANIES_USING_TECHNOLOGY_LIMIT}`);
  });

  it("is a guarded read: a ClickHouse failure degrades to an error string, never a thrown 500", async () => {
    clickhouse.query.mockRejectedValueOnce(new Error("Timeout error."));
    const result = await loadSeCompaniesUsingTechnology("WordPress");
    expect(result).toEqual({ rows: [], error: "Timeout error." });
  });

  it("never aggregates the whole commoncrawl_page_technologies table", () => {
    // The only GROUP-less aggregate allowed here is DISTINCT under the
    // explicit root_domain probe set; a bare GROUP BY over the table would
    // be the forbidden full-table rollup.
    expect(SE_COMPANIES_USING_TECHNOLOGY_SQL).not.toContain("GROUP BY");
  });
});
