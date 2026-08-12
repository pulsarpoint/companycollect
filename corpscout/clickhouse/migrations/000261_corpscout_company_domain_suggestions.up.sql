CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverse identity index for bounded company-to-domain candidate generation. The raw web tables
-- are ordered by root_domain. This table is deliberately ordered by normalized evidence value so
-- a country job can look up exact names, identifiers, domain labels, and people without repeatedly
-- scanning the Common Crawl corpus.
CREATE TABLE IF NOT EXISTS corpscout.web_domain_identity_features
(
    feature_type LowCardinality(String),
    normalized_value String,
    root_domain String,
    raw_value String,
    source_field LowCardinality(String),
    source_url String,
    crawl_id LowCardinality(String),
    source_resolved_at DateTime64(3, 'UTC'),
    indexed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(indexed_at)
PARTITION BY feature_type
ORDER BY (feature_type, normalized_value, root_domain, source_field, crawl_id);

-- Current review candidates. These rows are explicitly suggestions and are never consumed as the
-- canonical company-domain relationship without a separate review/acceptance boundary.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_suggestions
(
    country_iso2 LowCardinality(String),
    company_id String,
    root_domain String,
    rank UInt16,
    company_name String,
    candidate_sources Array(LowCardinality(String)),
    identifier_score Float32,
    website_name_score Float32,
    domain_name_score Float32,
    people_score Float32,
    industry_score Float32,
    country_score Float32,
    web_presence_score Float32,
    conflict_penalty Float32,
    total_score Float32,
    scoring_version LowCardinality(String),
    discovery_run_id String,
    suggested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, company_id, rank, root_domain);

-- One retained explanation per score component. Keeping evidence separate makes the review page
-- auditable without duplicating large source URLs and values in every suggestion row.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_suggestion_evidence
(
    country_iso2 LowCardinality(String),
    company_id String,
    root_domain String,
    signal_type LowCardinality(String),
    source_field LowCardinality(String),
    company_value String,
    domain_value String,
    score_contribution Float32,
    source_url String,
    crawl_id LowCardinality(String),
    discovery_run_id String,
    suggested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY
(
    country_iso2,
    company_id,
    root_domain,
    signal_type,
    source_field
);

-- Append-only provenance for successfully published country snapshots. Failed runs never claim a
-- snapshot because this row is inserted only after both current tables have been exchanged.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_discovery_runs
(
    country_iso2 LowCardinality(String),
    discovery_run_id String,
    scoring_version LowCardinality(String),
    company_count UInt64,
    candidate_pair_count UInt64,
    disqualified_candidate_count UInt64,
    suggestion_count UInt64,
    evidence_count UInt64,
    configuration_json String,
    started_at DateTime64(3, 'UTC'),
    completed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, completed_at, discovery_run_id);
