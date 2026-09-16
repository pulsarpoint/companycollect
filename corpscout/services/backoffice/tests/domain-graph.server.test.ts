import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  domainGraphPath,
  graphDomainError,
  parseDomainGraphSearch,
} from "~/lib/domain-graph";
import type { DomainGraphSearch } from "~/lib/domain-graph";

const chQuery = vi.hoisted(() => vi.fn());
vi.mock("~/lib/clickhouse.server", () => ({ chQuery }));
const { getDomainGraphReleases, searchDomainGraph } = await import(
  "~/lib/domain-graph.server"
);

const search: DomainGraphSearch = {
  domain: "example.com",
  release: "cc-main-2026-jun-jul-aug",
  direction: "all",
  page: 1,
  pageSize: 50,
};

describe("domain graph search parameters", () => {
  it("accepts website URLs and preserves compound country domains", () => {
    expect(
      parseDomainGraphSearch(
        new URL(
          "https://app/admin/graph?domain=https://www.Example.co.uk/about&page=-3&pageSize=999&direction=invalid",
        ),
      ),
    ).toEqual({
      ...search,
      domain: "example.co.uk",
      release: "",
      pageSize: 200,
    });
    expect(
      parseDomainGraphSearch(
        new URL("https://app/admin/graph?domain=bücher.de"),
      ).domain,
    ).toBe("xn--bcher-kva.de");
  });

  it.each([
    "localhost",
    "-bad.com",
    "bad-.com",
    "a..com",
    "127.0.0.1",
    "foo';DROP TABLE x;--.com",
    `${"a".repeat(64)}.com`,
  ])("rejects invalid domain %s", (domain) => {
    expect(graphDomainError(domain)).not.toBeNull();
  });

  it("keeps navigation scoped to the chosen release", () => {
    const url = new URL(
      domainGraphPath({ ...search, domain: "next.se", page: 2 }),
      "https://app",
    );
    expect(url.pathname).toBe("/admin/graph");
    expect(url.searchParams.get("release")).toBe(search.release);
    expect(url.searchParams.get("domain")).toBe("next.se");
    expect(url.searchParams.get("page")).toBe("2");
  });
});

describe("ClickHouse domain graph search", () => {
  beforeEach(() => chQuery.mockReset());

  it("offers only published snapshots ordered by publication time", async () => {
    chQuery.mockResolvedValueOnce([]);
    await getDomainGraphReleases();
    expect(chQuery.mock.calls[0][0]).toContain(
      "commoncrawl_domain_graph_snapshots FINAL",
    );
    expect(chQuery.mock.calls[0][0]).toContain("ORDER BY published_at DESC");
  });

  it("distinguishes an absent domain and does not scan its adjacency", async () => {
    chQuery.mockResolvedValueOnce([]);
    expect(await searchDomainGraph(search)).toMatchObject({
      found: false,
      total: 0,
      rows: [],
    });
    expect(chQuery).toHaveBeenCalledTimes(1);
    expect(chQuery.mock.calls[0][0]).toContain(
      "commoncrawl_domain_graph_snapshots FINAL",
    );
  });

  it("distinguishes an existing isolated domain", async () => {
    chQuery.mockResolvedValueOnce([{ node_id: 7 }]);
    chQuery.mockResolvedValueOnce([
      { total: "0", mutual: "0", outgoing_only: "0", incoming_only: "0" },
    ]);
    expect(await searchDomainGraph(search)).toMatchObject({
      found: true,
      total: 0,
      rows: [],
    });
    expect(chQuery).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["all", "1", 103],
    ["mutual", "reciprocal = 1", 21],
    ["outgoing", "outgoing = 1 AND incoming = 0", 62],
    ["incoming", "incoming = 1 AND outgoing = 0", 20],
  ] as const)(
    "filters %s connections and clamps pages to the filtered total",
    async (direction, filter, total) => {
      const rows = [
        {
          connected_domain: "neighbor.se",
          outgoing: 1,
          incoming: 1,
          reciprocal: 1,
          n_hosts: 3,
        },
      ];
      chQuery.mockResolvedValueOnce([{ node_id: 7 }]);
      chQuery.mockResolvedValueOnce([
        {
          total: "103",
          mutual: "21",
          outgoing_only: "62",
          incoming_only: "20",
        },
      ]);
      chQuery.mockResolvedValueOnce(rows);

      const result = await searchDomainGraph({
        ...search,
        direction,
        page: 999,
      });
      expect(result.total).toBe(total);
      expect(result.page).toBe(Math.ceil(total / 50));
      expect(result.rows).toEqual(rows);
      const [sql, params] = chQuery.mock.calls[2];
      expect(sql).toContain(`WHERE ${filter}`);
      expect(sql).toContain("ORDER BY reciprocal DESC, connected_domain ASC");
      expect(sql).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt64}");
      expect(sql).not.toContain(search.domain);
      expect(sql).not.toContain(search.release);
      expect(params).toEqual({
        domain: search.domain,
        release: search.release,
        limit: 50,
        offset: (result.page - 1) * 50,
      });
      for (const [query] of chQuery.mock.calls)
        expect(query).toContain("max_execution_time = 20");
    },
  );

  it("rejects invalid input before accessing the database", async () => {
    await expect(
      searchDomainGraph({ ...search, domain: "bad domain" }),
    ).rejects.toThrow("valid domain");
    expect(chQuery).not.toHaveBeenCalled();
  });
});
