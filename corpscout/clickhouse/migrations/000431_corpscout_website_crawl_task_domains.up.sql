CREATE DATABASE IF NOT EXISTS corpscout;

-- Frozen domain selection per crawl task, the website-crawl equivalent of
-- company_brave_search_input. The input asset writes one row per selected domain and
-- the results asset processes exactly this set for its task_id. Selection metadata
-- (fingerprint, total, status) stays in the processing.tasks PostgreSQL record.
-- Crawl settings still come from the request tables. This only fixes membership.
CREATE TABLE IF NOT EXISTS corpscout.website_crawl_task_domains
(
    task_id String,
    crawl_type LowCardinality(String),
    domain String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_task CHECK task_id != '',
    CONSTRAINT valid_crawl_type CHECK crawl_type IN ('full', 'jobs', 'site_info'),
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain)
)
ENGINE = ReplacingMergeTree
ORDER BY (task_id, domain);
