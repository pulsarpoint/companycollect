SELECT throwIf(count() > 0, 'website_crawl_task_domains must be empty before its layout changes')
FROM corpscout.website_crawl_task_domains;

DROP TABLE IF EXISTS corpscout.website_crawl_task_domains;

CREATE TABLE corpscout.website_crawl_task_domains
(
    task_id String,
    crawl_type LowCardinality(String),
    domain String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6),
    website_url String DEFAULT '',
    source_name String DEFAULT '',
    submission_id String DEFAULT '',
    CONSTRAINT valid_task CHECK task_id != '',
    CONSTRAINT valid_crawl_type CHECK crawl_type IN ('full', 'jobs', 'site_info'),
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain)
)
ENGINE = ReplacingMergeTree
ORDER BY (task_id, domain)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
