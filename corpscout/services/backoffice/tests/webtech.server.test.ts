import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
const catalog = vi.hoisted(() => ({ loadTechnologyCatalogEntries: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
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
