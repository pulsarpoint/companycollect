CREATE DATABASE IF NOT EXISTS corpscout;

-- Owner redesign of the technology detail page (2026-08-29): two adoption views -- Domains
-- (crawled domains ordered by harmonic centrality) and Companies (country-filtered, with
-- industries). Two weekly rollups back them, both filled by the technology_catalog job.

-- Replaces technology_se_companies (000353, created minutes earlier, zero readers): same
-- shape but COUNTRY-GENERIC -- company_domains already carries country_code, and new
-- countries land in this rollup (and the page's country filter) automatically.
DROP TABLE IF EXISTS corpscout.technology_se_companies;

CREATE TABLE IF NOT EXISTS corpscout.technology_companies
(
    technology String,
    country_code LowCardinality(String),
    company_id String,
    root_domain String,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (technology, country_code, company_id, root_domain);

-- Top crawled domains per technology by CommonCrawl harmonic centrality
-- (commoncrawl_domain_graph_signals). A technology's full domain set runs to tens of
-- millions (jQuery: 24M) -- ordering that live is infeasible, so the weekly job keeps the
-- top ~500 per technology.
CREATE TABLE IF NOT EXISTS corpscout.technology_top_domains
(
    technology String,
    root_domain String,
    harmonic_centrality Float64,
    harmonic_rank UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (technology, root_domain);
