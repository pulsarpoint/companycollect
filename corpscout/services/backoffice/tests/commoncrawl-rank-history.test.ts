import { afterEach, expect, it, vi } from "vitest";
const { chQuery, graphReleases, loadedRankingReleases } = vi.hoisted(() => ({
  chQuery: vi.fn(),
  graphReleases: vi.fn(),
  loadedRankingReleases: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery }));
vi.mock("~/lib/commoncrawl-graph.server", () => ({
  graphReleases,
  loadedRankingReleases,
}));
const { loadAuthoritySnapshots } =
  await import("~/lib/web-intelligence.server");
afterEach(() => vi.unstubAllEnvs());
const old = "cc-main-2026-jun-jul-aug",
  current = "cc-main-2026-jul-aug-sep";
const row = (release: string, rank: number, loaded: string) => ({
  crawl_id: release,
  cc_harmonic_centrality: 1,
  cc_harmonic_rank: rank,
  cc_pagerank: 0.5,
  cc_pagerank_rank: rank,
  n_hosts: 2,
  resolved_at: loaded,
});
it("orders by coverage rather than spelling or import time, and calculates improvement", async () => {
  vi.stubEnv("COMMONCRAWL_GRAPH_PG_URL", "test");
  graphReleases.mockResolvedValue([
    { graph_release: current, coverage_end: "2026-09-01" },
    { graph_release: old, coverage_end: "2026-08-01" },
  ]);
  loadedRankingReleases.mockResolvedValue([
    { graph_release: old },
    { graph_release: current },
  ]);
  chQuery.mockImplementation((sql: string) =>
    Promise.resolve(
      sql.includes("graph_signals")
        ? [row(old.toUpperCase(), 999, "2026-09-25")]
        : [row(old, 20, "2026-09-25"), row(current, 10, "2026-09-20")],
    ),
  );
  const history = await loadAuthoritySnapshots("example.com");
  expect(history.map((r) => r.crawlId)).toEqual([current, old]);
  expect(history[0]).toMatchObject({
    isCurrentRelease: true,
    harmonicRank: 10,
    harmonicRankChange: 10,
    pageRankChange: 10,
  });
  expect(history[1].harmonicRank).toBe(20);
});
it("does not present an older or legacy position as the current rank when absent", async () => {
  vi.stubEnv("COMMONCRAWL_GRAPH_PG_URL", "test");
  graphReleases.mockResolvedValue([
    { graph_release: current, coverage_end: "2026-09-01" },
    { graph_release: old, coverage_end: "2026-08-01" },
  ]);
  loadedRankingReleases.mockResolvedValue([
    { graph_release: old },
    { graph_release: current },
  ]);
  chQuery.mockImplementation((sql: string) =>
    Promise.resolve(
      sql.includes("graph_signals")
        ? [row(current.toUpperCase(), 999, "2026-09-25")]
        : [row(old, 20, "2026-09-25")],
    ),
  );
  const history = await loadAuthoritySnapshots("example.com");
  expect(history[0]).toMatchObject({
    isCurrentRelease: true,
    harmonicRank: null,
    pageRankRank: null,
    harmonicRankChange: null,
  });
  expect(history[1].harmonicRank).toBe(20);
});

it("shows unimported releases as gaps instead of calculating a change across them", async () => {
  vi.stubEnv("COMMONCRAWL_GRAPH_PG_URL", "test");
  const middle = "cc-main-2026-may-jun-jul";
  graphReleases.mockResolvedValue([
    {
      graph_release: current,
      coverage_end: "2026-09-01",
      ranks_available: true,
    },
    { graph_release: old, coverage_end: "2026-08-01", ranks_available: true },
    {
      graph_release: middle,
      coverage_end: "2026-07-01",
      ranks_available: true,
    },
  ]);
  loadedRankingReleases.mockResolvedValue([
    { graph_release: middle, rows: "100" },
    { graph_release: current, rows: "200" },
  ]);
  chQuery.mockImplementation((sql: string) =>
    Promise.resolve(
      sql.includes("graph_signals")
        ? []
        : [row(middle, 20, "2026-09-25"), row(current, 10, "2026-09-20")],
    ),
  );
  const history = await loadAuthoritySnapshots("example.com");
  expect(history[1]).toMatchObject({
    availability: "not_imported",
    harmonicRank: null,
  });
  expect(history[0]).toMatchObject({
    harmonicRankChange: null,
    population: 200,
  });
});
