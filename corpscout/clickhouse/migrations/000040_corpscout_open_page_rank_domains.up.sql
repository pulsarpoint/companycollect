CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.open_page_rank_domains
(
    source_system LowCardinality(String),
    source_list_name LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_rank UInt32,
    domain String,
    root_domain String,
    domain_extension LowCardinality(String),
    open_page_rank Nullable(Float64),
    source_url String,
    retrieved_date Date,
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, source_system, source_list_name, domain);
