CREATE DATABASE IF NOT EXISTS corpscout;

-- Whole-row argMax permits domain/page predicates to pass through the grouping.
-- This repairs the shadow view before the explicit migration-436 cutover.
CREATE OR REPLACE VIEW corpscout.webtech_domain_technologies_current_v2 AS
SELECT d.root_domain,
    d.crawl_id,
    d.detector_version,
    d.scan_id,
    d.detected_name,
    d.technology_id,
    d.technology,
    d.catalog_match,
    d.detected_slug,
    d.version,
    d.confidence,
    d.category_ids,
    d.categories,
    d.category_slugs,
    d.requested_url,
    d.final_url,
    d.final_hostname,
    d.outcome,
    d.analysis_status,
    d.analysis_complete,
    d.scanned_at,
    d.result_bucket,
    d.result_object_key,
    d.report_sha256,
    d.run_id,
    d.recorded_at,
    d.website_origin,
    d.page_url
FROM corpscout.webtech_domain_technologies_v2 AS d FINAL
INNER JOIN (
    SELECT root_domain, website_origin, page_url,
        argMax(tuple(crawl_id, detector_version, scan_id, report_sha256),
               tuple(scanned_at, crawl_id, detector_version, scan_id, report_sha256)) AS latest
    FROM corpscout.webtech_domain_scan_results_v2 FINAL
    GROUP BY root_domain, website_origin, page_url
) AS s
ON d.root_domain = s.root_domain AND d.website_origin = s.website_origin AND d.page_url = s.page_url
AND d.crawl_id = tupleElement(s.latest, 1) AND d.detector_version = tupleElement(s.latest, 2)
AND d.scan_id = tupleElement(s.latest, 3) AND d.report_sha256 = tupleElement(s.latest, 4);
