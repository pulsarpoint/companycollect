CREATE DATABASE IF NOT EXISTS corpscout;

-- Preparation only. Copy and validate the index with old publishers paused before
-- the coordinated table-name cutover. Keep the old index until parity is proven.
CREATE TABLE IF NOT EXISTS corpscout.domains_sources_next
(
    domain_id String,
    source_table LowCardinality(String),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    source_run_id String,
    CONSTRAINT valid_domain_id CHECK match(domain_id, '^[a-f0-9]{64}$'),
    CONSTRAINT valid_source CHECK source_table != ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY source_table
ORDER BY (domain_id, source_table);

-- Association status is owned by the country summary with live review overlays.
-- Adding another country requires an explicit UNION ALL here and in search.
SYSTEM STOP VIEW corpscout.domains_company_filter;
ALTER TABLE corpscout.domains_company_filter MODIFY QUERY
WITH associations AS (
    SELECT domain_id, country_code, company_id
    FROM corpscout.se_company_domain_resolved
    WHERE is_active = 1
)
SELECT d.root_domain AS root_domain, d.domain_id AS domain_id,
    toUInt32(uniqExact((s.country_code, s.company_id))) AS company_count,
    now64(3, 'UTC') AS refreshed_at
FROM (
    SELECT domain_id, root_domain FROM corpscout.domains
    WHERE domain_id IN (SELECT domain_id FROM associations)
) AS d
INNER JOIN associations AS s ON d.domain_id = s.domain_id
GROUP BY d.root_domain, d.domain_id
SETTINGS max_threads = 4, max_memory_usage = 4294967296,
    max_bytes_before_external_group_by = 268435456, max_execution_time = 300;
SYSTEM START VIEW corpscout.domains_company_filter;
SYSTEM REFRESH VIEW corpscout.domains_company_filter;
SYSTEM WAIT VIEW corpscout.domains_company_filter;
