-- Remove readers before their storage, leaving the database and existing crawls intact.
DROP VIEW IF EXISTS corpscout.website_site_info_requests_current;
DROP VIEW IF EXISTS corpscout.website_jobs_crawl_requests_current;
DROP VIEW IF EXISTS corpscout.website_full_crawl_requests_current;

DROP TABLE IF EXISTS corpscout.website_site_info_requests;
DROP TABLE IF EXISTS corpscout.website_jobs_crawl_requests;
DROP TABLE IF EXISTS corpscout.website_full_crawl_requests;
