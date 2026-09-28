CREATE DATABASE IF NOT EXISTS corpscout;

-- IDs use the same normalized UTF-8/SHA-256 convention as website_id and page_id.
ALTER TABLE corpscout.domains ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));
ALTER TABLE corpscout.domains ADD PROJECTION IF NOT EXISTS domains_by_id
    (SELECT domain_id, root_domain ORDER BY domain_id);
ALTER TABLE corpscout.websites ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));
ALTER TABLE corpscout.pages ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));
ALTER TABLE corpscout.domains_search ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));

-- A source index, not another company-domain entity. Non-company sources have
-- empty country/company IDs. Company identity always includes country_code.
CREATE TABLE IF NOT EXISTS corpscout.domains_sources
(
    domain_id String,
    source_table LowCardinality(String),
    source_record_id String,
    source_name LowCardinality(String),
    country_code LowCardinality(String),
    company_id String,
    association LowCardinality(String),
    confidence Float64,
    is_active UInt8,
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    source_run_id String,
    INDEX company_country country_code TYPE set(0) GRANULARITY 1,
    CONSTRAINT valid_domain_id CHECK match(domain_id, '^[a-f0-9]{64}$'),
    CONSTRAINT valid_source CHECK source_table != '' AND source_name != '',
    CONSTRAINT valid_company_identity CHECK (country_code = '' AND company_id = '')
        OR (match(country_code, '^[A-Z]{2}$') AND company_id != ''),
    CONSTRAINT valid_confidence CHECK isFinite(confidence) AND confidence BETWEEN 0 AND 1,
    CONSTRAINT valid_active CHECK is_active IN (0, 1)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY source_table
ORDER BY (domain_id, source_table, country_code, company_id, source_record_id);

-- Create missing parents before exposing company references or contributions.
INSERT INTO corpscout.domains (root_domain, sources, first_seen_at, last_seen_at, source_run_id)
SELECT root_domain, ['se_company_domain'], min(first_seen_at), max(last_seen_at), 'migration-000462'
FROM corpscout.se_company_domain FINAL
WHERE root_domain NOT IN (
    SELECT root_domain FROM corpscout.domains
    WHERE root_domain IN (SELECT root_domain FROM corpscout.se_company_domain FINAL)
)
GROUP BY root_domain;

INSERT INTO corpscout.domains_sources
SELECT lower(hex(SHA256(root_domain))), 'se_company_domain', company_id, 'se_company_domain',
    country_code, company_id,
    multiIf(is_active, 'connected', review_status = 'rejected', 'not_connected', 'uncertain'),
    if(review_status IN ('confirmed_primary', 'confirmed_related'), 1., toFloat64(suggested_confidence)),
    is_active, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.se_company_domain_resolved;

-- Bound each resumable membership set to one eighth of the SHA-256 space.
-- A single set containing 100M+ IDs can exceed memory before any rows are read.

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= '0' AND domain_id < '2'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= '0' AND domain_id < '2'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= '2' AND domain_id < '4'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= '2' AND domain_id < '4'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= '4' AND domain_id < '6'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= '4' AND domain_id < '6'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= '6' AND domain_id < '8'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= '6' AND domain_id < '8'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= '8' AND domain_id < 'a'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= '8' AND domain_id < 'a'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= 'a' AND domain_id < 'c'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= 'a' AND domain_id < 'c'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= 'c' AND domain_id < 'e'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= 'c' AND domain_id < 'e'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domains', '', 'commoncrawl', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl') AND domain_id >= 'e' AND domain_id < 'g'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domains' AND domain_id >= 'e' AND domain_id < 'g'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= '0' AND domain_id < '2'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= '0' AND domain_id < '2'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= '2' AND domain_id < '4'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= '2' AND domain_id < '4'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= '4' AND domain_id < '6'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= '4' AND domain_id < '6'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= '6' AND domain_id < '8'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= '6' AND domain_id < '8'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= '8' AND domain_id < 'a'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= '8' AND domain_id < 'a'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= 'a' AND domain_id < 'c'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= 'a' AND domain_id < 'c'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= 'c' AND domain_id < 'e'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= 'c' AND domain_id < 'e'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

INSERT INTO corpscout.domains_sources
SELECT domain_id, 'commoncrawl_domain_graph_nodes', '', 'commoncrawl_graph', '', '', '',
    0., 0, first_seen_at, last_seen_at, now64(6, 'UTC'), 'migration-000462'
FROM corpscout.domains PREWHERE has(sources, 'commoncrawl_graph') AND domain_id >= 'e' AND domain_id < 'g'
WHERE domain_id NOT IN (
    SELECT domain_id FROM corpscout.domains_sources
    WHERE source_table = 'commoncrawl_domain_graph_nodes' AND domain_id >= 'e' AND domain_id < 'g'
)
SETTINGS max_threads = 4, max_insert_threads = 2, max_memory_usage = 8589934592,
    max_bytes_before_external_sort = 536870912, max_execution_time = 7200;

-- Retain the normalized domain string as the fold's natural identity/evidence key.
-- domain_id is the reference used for central domain joins and source lookups.
ALTER TABLE corpscout.se_company_domain ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));
ALTER TABLE corpscout.se_company_domain_history ADD COLUMN IF NOT EXISTS domain_id String
    MATERIALIZED lower(hex(SHA256(root_domain)));

ALTER TABLE corpscout.domains MATERIALIZE PROJECTION domains_by_id
SETTINGS mutations_sync = 1, max_threads = 4;
