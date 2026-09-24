import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
const catalog = vi.hoisted(() => ({ loadTechnologyCatalogEntries: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
vi.mock("~/lib/webtech-maintenance.server", () => ({ assertWebtechAvailable: vi.fn() }));
vi.mock("~/lib/technology-catalog.server", () => catalog);
const { getDomainWebtech } = await import("~/lib/webtech.server");

beforeEach(() => {
  vi.resetAllMocks();
  catalog.loadTechnologyCatalogEntries.mockResolvedValue({});
});

it("selects a site's own scan instead of another hostname under the root", async () => {
  db.chQuery.mockResolvedValueOnce([]);
  await getDomainWebtech("example.se", "shop.example.se");
  expect(db.chQuery.mock.calls[0][0]).toContain(
    "AND final_hostname = {site:String}",
  );
  expect(db.chQuery.mock.calls[0][1]).toEqual({
    domain: "example.se",
    site: "shop.example.se",
  });
});

it("keeps domains without scans distinct from scanned domains", async () => {
  db.chQuery.mockResolvedValueOnce([]);
  expect(await getDomainWebtech("example.se")).toEqual({
    domain: "example.se",
    scan: null,
    pages: [],
    detections: [],
    catalog: {},
  });
  expect(db.chQuery).toHaveBeenCalledTimes(1);
});

it("reads the latest scan including zero detections, rather than falling back to an older positive scan", async () => {
  const scan = {
    crawl_id: "crawl",
    detector_version: "detector",
    scan_id: "empty-scan",
    report_sha256: "hash",
    technology_count: 0,
  };
  db.chQuery.mockResolvedValueOnce([scan]).mockResolvedValueOnce([]);
  const result = await getDomainWebtech("example.se");
  expect(result.scan).toEqual(scan);
  expect(result.detections).toEqual([]);
  expect(db.chQuery.mock.calls[0][0]).toContain("ORDER BY scanned_at DESC");
  expect(db.chQuery.mock.calls[0][0]).not.toContain("technology_count > 0");
  expect(db.chQuery.mock.calls[1][1]).toEqual({
    domain: "example.se",
    origin: undefined,
    page: undefined,
    crawl: "crawl",
    detector: "detector",
    scan: "empty-scan",
    hash: "hash",
  });
});

it("preserves UInt64 IDs and unmatched detections while enriching canonical names", async () => {
  const detections = [
    {
      detected_name: "ReactJS",
      technology: "React",
      technology_id: "18446744073709551615",
    },
    { detected_name: "Unlisted tool", technology: "", technology_id: null },
  ];
  db.chQuery
    .mockResolvedValueOnce([
      {
        crawl_id: "c",
        detector_version: "d",
        scan_id: "s",
        report_sha256: "h",
      },
    ])
    .mockResolvedValueOnce(detections);
  expect((await getDomainWebtech("example.se")).detections).toEqual(detections);
  expect(db.chQuery.mock.calls[1][0]).toContain("toString(technology_id)");
  expect(catalog.loadTechnologyCatalogEntries).toHaveBeenCalledWith(["React"]);
});

it("keeps page detections separate and filters redirect hosts only after picking latest attempts", async () => {
  const scans = [
    { website_origin: "https://example.se", page_url: "https://example.se/", crawl_id: "c", detector_version: "d", scan_id: "s", report_sha256: "a" },
    { website_origin: "https://example.se", page_url: "https://example.se/admin", crawl_id: "c", detector_version: "d", scan_id: "s", report_sha256: "b" },
  ];
  db.chQuery.mockResolvedValueOnce(scans).mockResolvedValueOnce([{ detected_name: "React", technology_id: null, version: "18" }]).mockResolvedValueOnce([{ detected_name: "React", technology_id: null, version: "19" }]);
  const result = await getDomainWebtech("example.se", "example.se");
  expect(result.pages.map(page => page.detections[0].version)).toEqual(["18", "19"]);
  const sql = db.chQuery.mock.calls[0][0];
  expect(sql.indexOf("LIMIT 1 BY")).toBeLessThan(sql.indexOf("AND final_hostname"));
  expect(db.chQuery.mock.calls[2][1].page).toBe("https://example.se/admin");
});

it("lists latest attempts per requested page, including failed and empty scans, without joining detections", async () => {
  const { listWebtechScans } = await import("~/lib/webtech.server");
  db.chQuery.mockResolvedValueOnce([{ root_domain: "example.se", outcome: "hard_timeout", technology_count: 0 }])
    .mockResolvedValueOnce([{ total: "1", domains: "1", pages: "1" }]);
  const result = await listWebtechScans({ prefix: "example", page: 2 });
  expect(result.total).toBe(1);
  expect(result.rows[0].outcome).toBe("hard_timeout");
  for (const [sql, params] of db.chQuery.mock.calls) {
    expect(sql).toContain("webtech_domain_scan_results FINAL");
    expect(sql).toContain("LIMIT 1 BY root_domain, website_origin, page_url");
    expect(sql).not.toContain("webtech_domain_technologies");
    expect(sql).not.toContain("commoncrawl");
    expect(sql).not.toContain("technology_count > 0");
    expect(params).toMatchObject({ prefix: "example", offset: 50, limit: 50 });
  }
});

it("keeps all historical scans and scopes domain history with an exact parameter", async () => {
  const { listWebtechScans } = await import("~/lib/webtech.server");
  db.chQuery.mockResolvedValueOnce([]).mockResolvedValueOnce([{ total: "103", domains: "1", pages: "2" }]);
  const result = await listWebtechScans({ domain: "example.se", view: "history", page: 3 });
  expect(result).toMatchObject({ total: 103, domains: 1, pages: 2 });
  for (const [sql, params] of db.chQuery.mock.calls) {
    expect(sql).not.toContain("LIMIT 1 BY");
    expect(sql).toContain("root_domain = {domain:String}");
    expect(params).toMatchObject({ domain: "example.se", offset: 100 });
  }
});

it("loads historical detections using the full page, batch, detector, scan and report identity", async () => {
  const { getWebtechScan } = await import("~/lib/webtech.server");
  const identity = { website_origin: "https://example.se", page_url: "https://example.se/admin", crawl_id: "old", detector_version: "v1", scan_id: "same-id", report_sha256: "a".repeat(64) };
  db.chQuery.mockResolvedValueOnce([identity]).mockResolvedValueOnce([{ detected_name: "React", technology_id: null, version: "18" }]);
  const result = await getWebtechScan("example.se", identity);
  expect(result?.detections[0].version).toBe("18");
  expect(db.chQuery.mock.calls[0][1]).toEqual({ domain: "example.se", ...identity });
  expect(db.chQuery.mock.calls[1][1]).toEqual({ domain: "example.se", origin: identity.website_origin, page: identity.page_url, crawl: "old", detector: "v1", scan: "same-id", hash: identity.report_sha256 });
  db.chQuery.mockResolvedValueOnce([]);
  expect(await getWebtechScan("other.se", identity)).toBeNull();
  expect(db.chQuery).toHaveBeenCalledTimes(3);
});
