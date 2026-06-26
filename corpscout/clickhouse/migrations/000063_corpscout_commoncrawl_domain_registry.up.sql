CREATE DATABASE IF NOT EXISTS corpscout;

-- Domain master/registry: one row per (root_domain, crawl) seen in CommonCrawl, written by EVERY
-- enrichment pass (tech AND industry). It holds only domain-level facts both passes produce, so the
-- two never clobber each other (unlike commoncrawl_domains, which the industry pass owns and fills
-- with NACE). The child tables (commoncrawl_domains, _technologies, _company_identifiers,
-- _company_profile) all key to root_domain; this guarantees the parent exists even for a tech-only run.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_registry
(
    crawl_id LowCardinality(String),
    root_domain String,
    subdomain String,
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
