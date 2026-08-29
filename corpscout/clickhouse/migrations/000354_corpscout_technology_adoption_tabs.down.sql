CREATE DATABASE IF NOT EXISTS corpscout;

-- Walks back to the SE-only rollup shape of 000353. All three tables are fully rebuildable
-- by one run of the technology_catalog job.
DROP TABLE IF EXISTS corpscout.technology_top_domains;
DROP TABLE IF EXISTS corpscout.technology_companies;

CREATE TABLE IF NOT EXISTS corpscout.technology_se_companies
(
    technology String,
    company_id String,
    root_domain String,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (technology, company_id, root_domain);
