import { beforeEach, expect, it, vi } from "vitest";
const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
const { listWorkspaceDomains, listDomainSites } =
  await import("~/lib/workspace-domains.server");
beforeEach(() => vi.resetAllMocks());

it("unifies source inventories, deduplicates roots, and enriches only the current page", async () => {
  const roots = Array.from({ length: 26 }, (_, i) => ({
    root_domain: `${String(i).padStart(2, "0")}.example`,
  }));
  db.chQuery.mockImplementation(async (sql: string) => {
    if (sql.includes("SELECT graph_release"))
      return [{ graph_release: "release" }];
    if (sql.startsWith("SELECT DISTINCT root_domain")) return roots;
    return [];
  });
  const result = await listWorkspaceDomains(
    { prefix: "", companies: "any", webtech: "any" },
    "",
  );
  expect(result.rows).toHaveLength(25);
  expect(result.hasMore).toBe(true);
  expect(result.next).toBe("24.example");
  expect(result.rows[0]).toMatchObject({
    companies: 0,
    archived: 0,
    webtech: 0,
    dns: 0,
  });
  const enrichment = db.chQuery.mock.calls.filter(([, params]) => params?.domains);
  expect(enrichment).toHaveLength(4);
  for (const [, params] of enrichment)
    expect(params.domains).not.toContain("25.example");
  const graph = db.chQuery.mock.calls.find(([sql]) =>
    sql.includes("FROM corpscout.commoncrawl_domain_graph_nodes"),
  );
  expect(graph?.[1].release).toBe("release");
});

it("filters on stored detections and preserves country-qualified company identity", async () => {
  db.chQuery.mockImplementation(async (sql: string) => {
    if (sql.startsWith("SELECT DISTINCT root_domain"))
      return [{ root_domain: "example.se" }];
    if (sql.includes("uniqExact((country_code, company_id))"))
      return [
        {
          root_domain: "example.se",
          count: 2,
          records: [
            ["SE", "123", "registry"],
            ["CZ", "123", "registry"],
          ],
        },
      ];
    return [];
  });
  const result = await listWorkspaceDomains(
    { prefix: "example'", companies: "with", webtech: "with" },
    "before.se",
  );
  const [sql, params] = db.chQuery.mock.calls[0];
  expect(sql).toContain("webtech_domain_technologies_current");
  expect(sql).not.toContain("technology_count > 0");
  expect(sql).not.toContain("example'");
  expect(params).toMatchObject({ prefix: "example'", after: "before.se" });
  expect(result.rows[0].companies).toBe(2);
});

it("keeps site technology counts isolated and includes unscanned hostnames", async () => {
  db.chQuery
    .mockResolvedValueOnce([
      { hostname: "shop.example.se" },
      { hostname: "www.example.se" },
    ])
    .mockResolvedValueOnce([
      { hostname: "www.example.se", count: 1, technologies: ["WordPress"] },
    ])
    .mockResolvedValueOnce([
      {
        hostname: "shop.example.se",
        count: 2,
        technologies: ["React", "Shopify"],
      },
    ]);
  const result = await listDomainSites("example.se", "");
  expect(result.sites).toEqual([
    {
      hostname: "shop.example.se",
      archived: 0,
      webtech: 2,
      technologies: ["React", "Shopify"],
    },
    {
      hostname: "www.example.se",
      archived: 1,
      webtech: 0,
      technologies: ["WordPress"],
    },
  ]);
  expect(db.chQuery.mock.calls[1][0]).toContain(
    "domain(page_url) IN {names:Array(String)}",
  );
  expect(db.chQuery.mock.calls[2][0]).toContain(
    "final_hostname IN {names:Array(String)}",
  );
});

it("applies negative filters before pagination rather than filtering just the returned page", async () => {
  db.chQuery.mockResolvedValue([]);
  await listWorkspaceDomains(
    { prefix: "example", companies: "without", webtech: "without" },
    "",
  );
  for (const [sql] of db.chQuery.mock.calls.filter(([sql]) =>
    sql.startsWith("SELECT DISTINCT"),
  )) {
    expect(sql).toContain("NOT IN (SELECT root_domain");
    expect(sql).toContain(
      "NOT IN (SELECT DISTINCT root_domain FROM corpscout.webtech_domain_technologies_current)",
    );
  }
});
