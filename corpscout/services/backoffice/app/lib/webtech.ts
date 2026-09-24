import type { WebtechScan } from "~/lib/webtech.server";

export type WebtechView = "latest" | "history";
export const WEBTECH_PAGE_SIZE = 50;

export function parseWebtechSearch(params: URLSearchParams) {
  const page = Number(params.get("page") ?? 1);
  return {
    prefix: (params.get("prefix") ?? "").trim().toLowerCase().slice(0, 253),
    view: params.get("view") === "history" ? "history" as const : "latest" as const,
    page: Number.isSafeInteger(page) && page > 0 ? Math.min(page, 100_000) : 1,
  };
}

export function webtechDomainPath(domain: string) {
  return `/admin/webtech/${encodeURIComponent(domain)}`;
}

export function webtechListPath(basePath: string, view: WebtechView, page = 1, prefix = "") {
  const params = new URLSearchParams({ view });
  if (page > 1) params.set("page", String(page));
  if (prefix) params.set("prefix", prefix);
  return `${basePath}?${params}`;
}

export type WebtechScanIdentity = Pick<WebtechScan,
  "website_origin" | "page_url" | "crawl_id" | "detector_version" | "scan_id" | "report_sha256"
>;

export function webtechScanPath(domain: string, scan: WebtechScanIdentity) {
  const params = new URLSearchParams({
    website_origin: scan.website_origin,
    page_url: scan.page_url,
    crawl_id: scan.crawl_id,
    detector_version: scan.detector_version,
    scan_id: scan.scan_id,
    report_sha256: scan.report_sha256,
  });
  return `${webtechDomainPath(domain)}/scan?${params}`;
}

export function parseWebtechScanIdentity(params: URLSearchParams): WebtechScanIdentity | null {
  const identity = {
    website_origin: params.get("website_origin") ?? "",
    page_url: params.get("page_url") ?? "",
    crawl_id: params.get("crawl_id") ?? "",
    detector_version: params.get("detector_version") ?? "",
    scan_id: params.get("scan_id") ?? "",
    report_sha256: params.get("report_sha256") ?? "",
  };
  // Older reports have no scan ID. The remaining identity, including the
  // report hash, still selects that exact stored scan; a missing parameter is invalid.
  return Object.entries(identity).every(([key, value]) =>
    params.has(key) && value.length <= 8192 && (key === "scan_id" || value.length > 0),
  ) && /^[a-f0-9]{64}$/i.test(identity.report_sha256) ? identity : null;
}
