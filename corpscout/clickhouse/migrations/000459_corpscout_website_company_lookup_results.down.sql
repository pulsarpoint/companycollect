DROP VIEW IF EXISTS corpscout.website_company_lookup_proposals;
DROP VIEW IF EXISTS corpscout.website_company_lookup_results_latest;
DROP TABLE IF EXISTS corpscout.website_company_lookup_searches;
DROP TABLE IF EXISTS corpscout.website_company_lookup_evidence;
DROP TABLE IF EXISTS corpscout.website_company_lookup_candidates;
DROP TABLE IF EXISTS corpscout.website_company_lookup_results;

CREATE OR REPLACE VIEW corpscout.website_full_crawl_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision FROM corpscout.website_full_crawl_requests FINAL;
ALTER TABLE corpscout.website_full_crawl_requests
    DROP CONSTRAINT IF EXISTS valid_matching_country,
    DROP COLUMN IF EXISTS match_company,
    DROP COLUMN IF EXISTS company_country,
    DROP COLUMN IF EXISTS skip_company_matching_if_mapped;
ALTER TABLE corpscout.website_full_crawl_results DROP COLUMN IF EXISTS company_matching_status;

CREATE OR REPLACE VIEW corpscout.website_site_info_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision FROM corpscout.website_site_info_requests FINAL;
ALTER TABLE corpscout.website_site_info_requests
    DROP CONSTRAINT IF EXISTS valid_matching_country,
    DROP COLUMN IF EXISTS match_company,
    DROP COLUMN IF EXISTS company_country,
    DROP COLUMN IF EXISTS skip_company_matching_if_mapped;
ALTER TABLE corpscout.website_site_info_results DROP COLUMN IF EXISTS company_matching_status;
