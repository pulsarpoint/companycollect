CREATE DATABASE IF NOT EXISTS corpscout;

-- Global adoption rollup for the technology pages: how many distinct crawled root domains
-- carry each technology. Computed by the weekly technology_catalog dagster job (one GROUP BY
-- pass over commoncrawl_page_technologies -- `technology` sits LAST in that table's sort key,
-- so a live per-technology count scans all ~10.6B rows at 6-26s per query, measured
-- 2026-08-29 -- this rollup is the ONLY affordable way to put the number on a page). The
-- SE-companies-using-a-technology view stays LIVE -- filtering by root_domain (the sort key
-- head) over the ~17k SE company domains is key-pruned and fast.
CREATE TABLE IF NOT EXISTS corpscout.technology_adoption
(
    technology String,
    domain_count UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (technology);
