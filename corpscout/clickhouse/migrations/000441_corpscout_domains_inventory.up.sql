CREATE DATABASE IF NOT EXISTS corpscout;

-- Unified inventory sourced only from commoncrawl_domains and se_company_domain.
CREATE TABLE IF NOT EXISTS corpscout.domains
(
    root_domain String,
    sources Array(LowCardinality(String)),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    source_run_id String
)
ENGINE = MergeTree
ORDER BY root_domain;

DROP TABLE IF EXISTS corpscout.domain_inventory SYNC;
