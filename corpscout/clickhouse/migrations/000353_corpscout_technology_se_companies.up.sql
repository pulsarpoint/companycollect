CREATE DATABASE IF NOT EXISTS corpscout;

-- Weekly rollup: which Swedish companies use which technology. The live equivalent
-- (commoncrawl_page_technologies pruned by the SE company domains) still reads ~491M rows
-- and took 15-18s per technology detail page load (measured 2026-08-29) -- one pruned scan
-- per WEEK here instead, and the page reads this table keyed by technology in milliseconds.
-- Filled by the technology_catalog dagster job (stage + EXCHANGE, refuse-on-empty).
CREATE TABLE IF NOT EXISTS corpscout.technology_se_companies
(
    technology String,
    company_id String,
    root_domain String,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (technology, company_id, root_domain);
