import { beforeEach, expect, it, vi } from "vitest";
import { parseWorkspaceDomainFilters } from "~/lib/workspace-domains";
const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { listWorkspaceDomains, listDomainSites } = await import("~/lib/workspace-domains.server");
beforeEach(() => vi.resetAllMocks());
it("reads only the serving snapshot and paginates without expensive counts or joins", async () => {
  db.chQuery.mockResolvedValueOnce(Array.from({ length: 26 }, (_, i) => ({ root_domain: `${String(i).padStart(2, "0")}.example` })))
    .mockResolvedValueOnce([{ name: "domains_search", total: "123142485" }, { name: "websites", total: "0" }]).mockResolvedValueOnce([{ refreshed_at: "2026-09-24" }]);
  const result = await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams()), "");
  expect(result).toMatchObject({ hasMore: true, next: "24.example", total: "123142485", refreshedAt: "2026-09-24" });
  expect(result.rows).toHaveLength(25);
  expect(db.chQuery).toHaveBeenCalledTimes(3);
  const sql = db.chQuery.mock.calls.map(([sql]) => sql).join("\n");
  expect(sql).not.toMatch(/JOIN|UNION|FINAL|count\(/);
  expect(sql).toContain("FROM corpscout.domains_search");
});
it("applies combined and negative filters before pagination using bound values", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams("prefix=example'&source=commoncrawl&source=se_company_domain&sourceMatch=all&dns=without&websites=observed&companies=with")), "before.se");
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("hasAll(sources, {sources:Array(String)})");
  expect(sql).toContain("has_dns_records = 0");
  expect(sql).toContain("has_website = 1 AND observed_website_count > 0");
  expect(sql).toContain("has_company = 1");
  expect(sql).not.toContain("example'");
  expect(params).toMatchObject({ prefix: "example'", after: "before.se", sources: ["commoncrawl", "se_company_domain"] });
});
it("expands websites directly from the website inventory", async () => {
  const site = { website_origin: "https://www.example.se", evidence_status: "observed", sources: ["webtech"], last_observed_at: "2026-09-24" };
  db.chQuery.mockResolvedValueOnce([site]);
  expect(await listDomainSites("example.se", "")).toEqual({ sites: [site], next: site.website_origin, hasMore: false });
  expect(db.chQuery).toHaveBeenCalledTimes(1);
  expect(db.chQuery.mock.calls[0][0]).toContain("FROM corpscout.websites WHERE root_domain={domain:String}");
});
