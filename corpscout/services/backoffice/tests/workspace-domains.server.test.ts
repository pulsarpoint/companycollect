import { beforeEach, expect, it, vi } from "vitest";
import { parseWorkspaceDomainFilters } from "~/lib/workspace-domains";
const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { listWorkspaceDomains, listDomainSites } = await import("~/lib/workspace-domains.server");
beforeEach(() => vi.resetAllMocks());
it("paginates the inventory and refreshes associations and matching for only the visible page", async () => {
  db.chQuery.mockResolvedValueOnce(Array.from({ length: 26 }, (_, i) => ({ root_domain: `${String(i).padStart(2, "0")}.example` })))
    .mockResolvedValueOnce([{ name: "domains_search", total: "123142485" }, { name: "websites", total: "0" }]).mockResolvedValueOnce([{ refreshed_at: "2026-09-24" }]).mockResolvedValueOnce([{ root_domain: "00.example", company_count: 2 }]).mockResolvedValueOnce([{ domain: "00.example", status: "failed" }]);
  const result = await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams()), "");
  expect(result).toMatchObject({ hasMore: true, next: "24.example", total: "123142485", matchingTotal: "123142485", refreshedAt: "2026-09-24" });
  expect(result.rows).toHaveLength(25);
  expect(db.chQuery).toHaveBeenCalledTimes(5);
  expect(result.rows[0]).toMatchObject({ company_count: 2, company_matching_status: "failed" });
  expect(db.chQuery.mock.calls[3][1].domains).toHaveLength(25);
  const sql = db.chQuery.mock.calls.slice(0, 3).map(([sql]) => sql).join("\n");
  expect(sql).not.toMatch(/JOIN|UNION|FINAL|count\(/);
  expect(sql).toContain("FROM corpscout.domains_search");
});
it("applies combined and negative filters before pagination using bound values", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams("prefix=example'&suffix=.SE&source=commoncrawl&source=se_company_domain&sourceMatch=all&dns=without&websites=observed&companies=with")), "before.se");
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("hasAll(sources, {sources:Array(String)})");
  expect(sql).toContain("has_dns_records = 0");
  expect(sql).toContain("has_website = 1 AND observed_website_count > 0");
  expect(sql).toContain("FROM corpscout.company_domains_resolved WHERE is_active = 1");
  expect(sql).not.toContain("has_company = 1");
  expect(sql).toContain("endsWith(root_domain, {suffix:String})");
  expect(sql).not.toContain("example'");
  expect(params).toMatchObject({ prefix: "example'", suffix: ".se", after: "before.se", sources: ["commoncrawl", "se_company_domain"] });
});
it("counts the complete filtered set independently of the pagination cursor", async () => {
  db.chQuery.mockResolvedValueOnce([])
    .mockResolvedValueOnce([{ name: "domains_search", total: "123142485" }])
    .mockResolvedValueOnce([{ refreshed_at: "2026-09-24" }])
    .mockResolvedValueOnce([{ total: "676571" }]);
  const result = await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams("suffix=se&companies=without")), "last-page.se");
  expect(result).toMatchObject({ matchingTotal: "676571", total: "123142485" });
  const [countSql, countParams] = db.chQuery.mock.calls[3];
  expect(countSql).toContain("count()");
  expect(countSql).toContain("endsWith(root_domain, {suffix:String}) AND root_domain NOT IN");
  expect(countSql).not.toMatch(/after|LIMIT|OFFSET/);
  expect(countParams).not.toHaveProperty("after");
  expect(countParams).toMatchObject({ suffix: ".se" });
});
it("shows zero matches without substituting the inventory total", async () => {
  db.chQuery.mockResolvedValueOnce([])
    .mockResolvedValueOnce([{ name: "domains_search", total: "123142485" }])
    .mockResolvedValueOnce([]).mockResolvedValueOnce([{ total: "0" }]);
  const result = await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams("suffix=invalid-tld")), "");
  expect(result.matchingTotal).toBe("0");
});
it("expands websites directly from the website inventory", async () => {
  const site = { website_origin: "https://www.example.se", evidence_status: "observed", sources: ["webtech"], last_observed_at: "2026-09-24" };
  db.chQuery.mockResolvedValueOnce([site]);
  expect(await listDomainSites("example.se", "")).toEqual({ sites: [site], next: site.website_origin, hasMore: false });
  expect(db.chQuery).toHaveBeenCalledTimes(1);
  expect(db.chQuery.mock.calls[0][0]).toContain("FROM corpscout.websites WHERE root_domain={domain:String}");
});

it("excludes every matching outcome, not only successful proposals, before counting or paging", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams("suffix=se&companies=without&companyMatching=without")), "some.se");
  for (const index of [0, 3]) {
    const sql = db.chQuery.mock.calls[index][0];
    expect(sql).toMatch(/root_domain NOT IN\s+\(SELECT domain FROM corpscout.website_company_lookup_results\)/);
    expect(sql).toContain("FROM corpscout.company_domains_resolved WHERE is_active = 1");
    expect(sql).not.toMatch(/found|status =|proposals|has_company =/);
  }
  expect(db.chQuery.mock.calls[3][0]).not.toContain("after");
});
