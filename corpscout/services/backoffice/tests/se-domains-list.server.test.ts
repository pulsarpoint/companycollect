import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  buildSeDomainsFilter,
  DOMAINS_COMPANY_NAMES_SQL,
  DOMAINS_COUNTS_SQL,
  DOMAINS_LIST_SELECT_SQL,
  DOMAIN_COMPANIES_SQL,
  DOMAIN_COMPANY_COUNTS_SQL,
  listSeDomainsPage,
  loadSeDomainCompanies,
  loadSeDomainCompanyCounts,
  loadSeDomainsCounts,
} from "~/lib/se-domains-list.server";
import { EMPTY_SE_DOMAINS_FILTERS as EMPTY } from "~/lib/se-domains-filters";

const ROW = {
  company_id: "5560125220",
  root_domain: "example.se",
  website_url: "https://www.example.se/",
  website_host: "www.example.se",
  association: "connected",
  is_primary: 1,
  confidence: 0.9,
  sources: ["wikidata", "brave"],
  supporting_sources: ["wikidata", "brave"],
  verification_status: "success",
  review_status: "unreviewed",
  active: 1,
  inactive_reason: "",
  company_count: 2,
};

describe("se-domains-list.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset().mockResolvedValue([]);
  });

  it("reads the entity through FINAL with a per-domain company count, sorted by confidence", () => {
    expect(DOMAINS_LIST_SELECT_SQL).toContain("FROM corpscout.se_company_domain AS d FINAL");
    expect(DOMAINS_LIST_SELECT_SQL).toContain("uniqExact(company_id) AS company_count");
    expect(DOMAINS_LIST_SELECT_SQL).toContain("toFloat64(d.confidence) AS confidence");
    expect(DOMAINS_COUNTS_SQL).toContain("FROM corpscout.se_company_domain AS d FINAL");
    expect(DOMAINS_COUNTS_SQL).toContain("toString(uniqExact(d.root_domain)) AS domains");
    expect(DOMAINS_COUNTS_SQL).toContain("toString(uniqExact(d.company_id)) AS companies");
    expect(DOMAINS_COUNTS_SQL).toContain(
      "toString(uniqExactIf(d.root_domain, s.company_count > 1)) AS shared",
    );
    expect(DOMAINS_COMPANY_NAMES_SQL).toContain("FROM corpscout.se_companies_serving");
    expect(DOMAINS_COMPANY_NAMES_SQL).toContain("WHERE company_id IN {companyIds:Array(String)}");
    expect(DOMAIN_COMPANIES_SQL).toContain("WHERE d.root_domain = {domain:String}");
    expect(DOMAIN_COMPANY_COUNTS_SQL).toContain("WHERE root_domain IN {domains:Array(String)}");
  });

  it("builds one predicate per filter, parameterized", () => {
    expect(buildSeDomainsFilter(EMPTY)).toEqual({ where: [], params: {} });
    expect(buildSeDomainsFilter({ ...EMPTY, domain: "exam" })).toEqual({
      where: ["d.root_domain LIKE {domain:String}"],
      params: { domain: "%exam%" },
    });
    expect(buildSeDomainsFilter({ ...EMPTY, company: "5560125220" })).toEqual({
      where: ["d.company_id = {company:String}"],
      params: { company: "5560125220" },
    });
    expect(buildSeDomainsFilter({ ...EMPTY, association: "uncertain" })).toEqual({
      where: ["d.association = {association:String}"],
      params: { association: "uncertain" },
    });
    expect(buildSeDomainsFilter({ ...EMPTY, status: "active" })).toEqual({
      where: ["d.active = 1"],
      params: {},
    });
    expect(buildSeDomainsFilter({ ...EMPTY, status: "inactive" })).toEqual({
      where: ["d.active = 0"],
      params: {},
    });
    expect(buildSeDomainsFilter({ ...EMPTY, minConfidence: "0.7", maxConfidence: "0.9" })).toEqual({
      where: ["d.confidence >= {minConfidence:Float64}", "d.confidence <= {maxConfidence:Float64}"],
      params: { minConfidence: 0.7, maxConfidence: 0.9 },
    });
    expect(buildSeDomainsFilter({ ...EMPTY, shared: "1" })).toEqual({
      where: ["s.company_count > 1"],
      params: {},
    });
  });

  it("pages under the filter and names the page's companies in one read", async () => {
    clickhouse.query
      .mockResolvedValueOnce([ROW, { ...ROW, company_id: "5560125221", confidence: 0.6 }])
      .mockResolvedValueOnce([{ company_id: "5560125220", legal_name: "Example AB" }]);
    const { rows } = await listSeDomainsPage({ ...EMPTY, shared: "1", page: 2, pageSize: 50 });

    const [listSql, listParams] = clickhouse.query.mock.calls[0];
    expect(listSql).toContain("WHERE s.company_count > 1");
    expect(listSql).toContain("ORDER BY d.confidence DESC, d.root_domain, d.company_id");
    expect(listSql).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
    expect(listParams).toEqual({ limit: 50, offset: 50 });
    expect(clickhouse.query.mock.calls[1]).toEqual([
      DOMAINS_COMPANY_NAMES_SQL,
      { companyIds: ["5560125220", "5560125221"] },
    ]);
    expect(rows.map((row) => [row.company_id, row.legal_name])).toEqual([
      ["5560125220", "Example AB"],
      ["5560125221", ""],
    ]);
  });

  it("skips the names read on an empty page", async () => {
    await listSeDomainsPage({ ...EMPTY, page: 1, pageSize: 50 });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
  });

  it("counts under the same filter as the page", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { rows: "11370", domains: "9355", companies: "8735", shared: "718" },
    ]);
    const counts = await loadSeDomainsCounts({ ...EMPTY, minConfidence: "0.9" });
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("WHERE d.confidence >= {minConfidence:Float64}");
    expect(params).toEqual({ minConfidence: 0.9 });
    expect(counts).toEqual({ rows: 11370, domains: 9355, companies: 8735, shared: 718 });
  });

  it("lists every company behind one domain, named", async () => {
    clickhouse.query
      .mockResolvedValueOnce([ROW])
      .mockResolvedValueOnce([{ company_id: "5560125220", legal_name: "Example AB" }]);
    const rows = await loadSeDomainCompanies("example.se");
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ domain: "example.se" });
    expect(rows).toEqual([{ ...ROW, legal_name: "Example AB" }]);
  });

  it("maps connected domains to how many SE companies claim each", async () => {
    clickhouse.query.mockResolvedValueOnce([{ root_domain: "a.se", company_count: "2" }]);
    expect(await loadSeDomainCompanyCounts(["a.se", "b.se"])).toEqual(new Map([["a.se", 2]]));
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ domains: ["a.se", "b.se"] });
    clickhouse.query.mockClear();
    expect(await loadSeDomainCompanyCounts([])).toEqual(new Map());
    expect(clickhouse.query).not.toHaveBeenCalled();
  });
});
