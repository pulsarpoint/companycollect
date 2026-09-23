import { chQuery } from "~/lib/clickhouse.server";
import { loadTechnologyCatalogEntries } from "~/lib/technology-catalog.server";

export interface WebtechScan {
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
  // Select the scan first, including zero-detection outcomes, so an older
  // positive scan can never be presented as the latest result.
  const scans = await chQuery<WebtechScan>(
    `SELECT crawl_id, detector_version, scan_id, report_sha256,
      toString(scanned_at) AS scanned_at, outcome, technology_count,
      requested_url, final_url
    FROM corpscout.webtech_domain_scan_results FINAL
    WHERE root_domain = {domain:String}
      ${site ? "AND final_hostname = {site:String}" : ""}
    ORDER BY scanned_at DESC, recorded_at DESC, detector_version DESC, scan_id DESC
    LIMIT 1`,
    { domain, ...(site ? { site } : {}) },
  );
  const scan = scans.at(0);
  const detections = scan
    ? await chQuery<WebtechDetection>(
        `SELECT detected_name, toString(technology_id) AS technology_id,
          technology, catalog_match, version, confidence, categories, analysis_complete
        FROM corpscout.webtech_domain_technologies_current
        WHERE root_domain = {domain:String}
          AND crawl_id = {crawl:String} AND detector_version = {detector:String}
          AND scan_id = {scan:String} AND report_sha256 = {hash:String}
        ORDER BY lowerUTF8(detected_name), detected_name`,
        {
          domain,
          crawl: scan.crawl_id,
          detector: scan.detector_version,
          scan: scan.scan_id,
          hash: scan.report_sha256,
        },
      )
    : [];
  const catalog = await loadTechnologyCatalogEntries(
    detections
      .filter((row) => row.technology_id !== null)
      .map((row) => row.technology),
  );
  return { domain, scan: scan ?? null, detections, catalog };
}
