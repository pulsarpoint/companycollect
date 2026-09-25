CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract for crawl drafts: one partition per task so cleanup is
-- DROP PARTITION, sorted by task and domain for reads, and a required submission_id
-- so a retried import replaces only its own rows. Drafts are purged after completion,
-- so the table is rebuilt only while empty. The mutation-pool setting stays because
-- that retry still deletes the submission's rows with a lightweight DELETE.
SELECT throwIf(count() > 0, 'website_crawl_task_domains must be empty before its layout changes')
FROM corpscout.website_crawl_task_domains;

DROP TABLE IF EXISTS corpscout.website_crawl_task_domains;

CREATE TABLE corpscout.website_crawl_task_domains
(
    task_id String,
    crawl_type LowCardinality(String),
    domain String,
    website_url String,
    source_name LowCardinality(String),
    submission_id String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_task CHECK notEmpty(task_id) AND notEmpty(submission_id),
    CONSTRAINT valid_crawl_type CHECK crawl_type IN ('full', 'jobs', 'site_info'),
    CONSTRAINT valid_domain CHECK notEmpty(domain) AND domain = lowerUTF8(domain),
    CONSTRAINT valid_website_url CHECK protocol(website_url) IN ('http', 'https') AND notEmpty(domain(website_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, domain)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
