import { WEBTECH_PAGE_SIZE, type WebtechScanIdentity, type WebtechView } from "~/lib/webtech";
import { assertWebtechAvailable } from "~/lib/webtech-maintenance.server";
import { chQuery } from "~/lib/clickhouse.server";
import { loadTechnologyCatalogEntries } from "~/lib/technology-catalog.server";

export interface WebtechScan {
  website_origin: string;
  page_url: string;
  crawl_id: string;
  detector_version: string;
  scan_id: string;
  report_sha256: string;
  scanned_at: string;
  outcome: string;
  technology_count: number;
  requested_url: string;
  final_url: string;
}

export interface WebtechDetection {
  website_origin: string;
  page_url: string;
  detected_name: string;
  technology_id: string | null;
  technology: string;
  catalog_match: string;
  version: string;
  confidence: number;
  categories: string[];
  analysis_complete: number;
}

export async function getDomainWebtech(domain: string, site?: string) {
  assertWebtechAvailable();
  // Select the whole latest attempt per requested page before filtering observed hosts.
  // A changed/failed redirect must never resurrect a previous destination.
  const scans = await chQuery<WebtechScan>(
    `SELECT website_origin, page_url, crawl_id, detector_version, scan_id, report_sha256,
      toString(scanned_at) AS scanned_at, outcome, technology_count,
      requested_url, final_url
    FROM (
      SELECT * FROM corpscout.webtech_domain_scan_results FINAL
      WHERE root_domain = {domain:String}
      ORDER BY scanned_at DESC, crawl_id DESC, detector_version DESC, scan_id DESC, report_sha256 DESC
      LIMIT 1 BY root_domain, website_origin, page_url
    )
    WHERE 1 ${site ? "AND final_hostname = {site:String}" : ""}
    ORDER BY website_origin, page_url`,
    { domain, ...(site ? { site } : {}) },
  );
  return loadScanDetections(domain, scans);
}

async function loadScanDetections(domain: string, scans: WebtechScan[]) {
  const pages = await Promise.all(scans.map(async (scan) => {
    const detections = await chQuery<WebtechDetection>(
        `SELECT website_origin, page_url, detected_name, toString(technology_id) AS technology_id,
          technology, catalog_match, version, confidence, categories, analysis_complete
        FROM corpscout.webtech_domain_technologies FINAL
        WHERE root_domain = {domain:String}
          AND website_origin = {origin:String} AND page_url = {page:String}
          AND crawl_id = {crawl:String} AND detector_version = {detector:String}
          AND scan_id = {scan:String} AND report_sha256 = {hash:String}
        ORDER BY lowerUTF8(detected_name), detected_name`,
        {
          domain,
          origin: scan.website_origin,
          page: scan.page_url,
          crawl: scan.crawl_id,
          detector: scan.detector_version,
          scan: scan.scan_id,
          hash: scan.report_sha256,
        },
      );
    return { scan, detections };
  }));
  const detections = pages.flatMap(page => page.detections);
  const catalog = await loadTechnologyCatalogEntries(
    detections
      .filter((row) => row.technology_id !== null)
      .map((row) => row.technology),
  );
  return { domain, scan: scans.at(0) ?? null, detections, catalog, pages };
}

export interface WebtechScanRow extends WebtechScan {
  root_domain: string;
}

/** List scan metadata only; detection details are loaded for an individual domain or scan. */
export async function listWebtechScans({
  domain,
  prefix = "",
  view = "latest",
  page = 1,
}: {
  domain?: string;
  prefix?: string;
  view?: WebtechView;
  page?: number;
} = {}) {
  assertWebtechAvailable();
  const params = { domain: domain ?? "", prefix, limit: WEBTECH_PAGE_SIZE, offset: (page - 1) * WEBTECH_PAGE_SIZE };
  const selection = `SELECT root_domain, website_origin, page_url, crawl_id, detector_version,
      scan_id, report_sha256, scanned_at, outcome, technology_count, requested_url, final_url
    FROM corpscout.webtech_domain_scan_results FINAL
    WHERE ${domain ? "root_domain = {domain:String}" : "startsWith(root_domain, {prefix:String})"}
    ${view === "latest" ? `ORDER BY scanned_at DESC, crawl_id DESC, detector_version DESC, scan_id DESC, report_sha256 DESC
      LIMIT 1 BY root_domain, website_origin, page_url` : ""}`;
  const [rows, counts] = await Promise.all([
    chQuery<WebtechScanRow>(
      `SELECT * REPLACE(toString(scanned_at) AS scanned_at) FROM (${selection})
       ORDER BY ${view === "history" ? "scanned_at DESC," : ""} root_domain, website_origin, page_url,
         crawl_id DESC, detector_version DESC, scan_id DESC, report_sha256 DESC
       LIMIT {limit:UInt32} OFFSET {offset:UInt64}`,
      params,
    ),
    chQuery<{ total: string; domains: string; pages: string }>(
      `SELECT toString(count()) AS total, toString(uniqExact(root_domain)) AS domains,
        toString(uniqExact(tuple(root_domain, website_origin, page_url))) AS pages
       FROM (${selection})`,
      params,
    ),
  ]);
  const count = counts[0];
  return { rows, total: Number(count?.total ?? 0), domains: Number(count?.domains ?? 0), pages: Number(count?.pages ?? 0) };
}

export async function getWebtechScan(domain: string, identity: WebtechScanIdentity) {
  assertWebtechAvailable();
  const scans = await chQuery<WebtechScan>(
    `SELECT website_origin, page_url, crawl_id, detector_version, scan_id, report_sha256,
      toString(scanned_at) AS scanned_at, outcome, technology_count, requested_url, final_url
     FROM corpscout.webtech_domain_scan_results FINAL
     WHERE root_domain = {domain:String}
       AND website_origin = {website_origin:String} AND page_url = {page_url:String}
       AND crawl_id = {crawl_id:String} AND detector_version = {detector_version:String}
       AND scan_id = {scan_id:String} AND report_sha256 = {report_sha256:String}
     LIMIT 1`,
    { domain, ...identity },
  );
  return scans.length ? loadScanDetections(domain, scans) : null;
}
