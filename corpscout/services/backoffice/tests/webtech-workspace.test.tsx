import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { WebtechScansTable, WebtechViewTabs } from "~/components/admin/webtech-scans-table";
import { parseWebtechScanIdentity, parseWebtechSearch, webtechScanPath } from "~/lib/webtech";

const scan = {
  root_domain: "example.se", website_origin: "https://example.se", page_url: "https://example.se/search?q=a&b=c",
  crawl_id: "batch", detector_version: "v1", scan_id: "scan-1", report_sha256: "a".repeat(64),
  scanned_at: "2026-09-24 10:00:00", outcome: "hard_timeout", technology_count: 0,
  requested_url: "https://example.se/search?q=a&b=c", final_url: "",
};

describe("Webtech workspace navigation", () => {
  it("round-trips the full scan identity including URLs with query parameters", () => {
    const path = webtechScanPath(scan.root_domain, scan);
    const identity = parseWebtechScanIdentity(new URL(path, "http://localhost").searchParams);
    expect(identity).toEqual({ website_origin: scan.website_origin, page_url: scan.page_url, crawl_id: scan.crawl_id, detector_version: scan.detector_version, scan_id: scan.scan_id, report_sha256: scan.report_sha256 });
    expect(parseWebtechScanIdentity(new URLSearchParams({ scan_id: "scan-1" }))).toBeNull();
  });

  it("can open legacy reports with an empty scan ID without accepting an omitted identity field", () => {
    const params = new URL(webtechScanPath(scan.root_domain, { ...scan, scan_id: "" }), "http://localhost").searchParams;
    expect(parseWebtechScanIdentity(params)?.scan_id).toBe("");
    params.delete("scan_id");
    expect(parseWebtechScanIdentity(params)).toBeNull();
  });

  it("resets pagination when switching views while retaining the domain filter", () => {
    const html = renderToStaticMarkup(<MemoryRouter><WebtechViewTabs basePath="/admin/webtech" view="history" prefix="example" /></MemoryRouter>);
    expect(html).toContain("Latest results");
    expect(html).toContain("Scan history");
    expect(html).toContain("view=latest&amp;prefix=example");
    expect(html).toContain("view=history&amp;prefix=example");
    expect(html).not.toContain("common-crawl");
  });

  it("shows failed zero-result scans, totals, history links and bounded pagination", () => {
    const html = renderToStaticMarkup(<MemoryRouter><WebtechScansTable basePath="/admin/webtech" view="history" page={2} prefix="example" result={{ rows: [scan], total: 103, domains: 7, pages: 8 }} /></MemoryRouter>);
    for (const text of ["103", "scans", "hard timeout", "View scan", "Previous", "Next", "/admin/webtech/example.se/scan?"]) expect(html).toContain(text);
    expect(html).toContain("view=history&amp;page=3&amp;prefix=example");
    expect(html).not.toContain("common-crawl");
  });

  it("normalizes search and rejects invalid page values", () => {
    expect(parseWebtechSearch(new URLSearchParams("prefix=WORDPRESS&view=invalid&page=-1"))).toEqual({ prefix: "wordpress", view: "latest", page: 1 });
    expect(parseWebtechSearch(new URLSearchParams("view=history&page=2"))).toMatchObject({ view: "history", page: 2 });
  });
});
