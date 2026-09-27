CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.website_company_lookup_results (
    country LowCardinality(String), domain String, request_id String, attempt UInt32,
    batch_id String, input_id String, run_id String, website_url String,
    status LowCardinality(String), existing_company_ids Array(String), found Bool, company_id String, confidence Nullable(Float64),
    no_match_probability Nullable(Float64), site_type LowCardinality(String),
    reasons Array(String), basis LowCardinality(String), stop_reason String,
    basic_status LowCardinality(String), model String, served_models Array(String),
    model_calls UInt32, input_tokens UInt64, output_tokens UInt64,
    known_cost_usd Float64, unknown_cost_calls UInt32,
    candidate_count UInt32, search_count UInt32, result_path String,
    started_at DateTime64(6, 'UTC'), finished_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_match CHECK found = (status = 'matched') AND (NOT found OR (company_id != '' AND confidence IS NOT NULL)),
    CONSTRAINT valid_status CHECK status IN ('matched', 'not_found', 'already_mapped', 'failed', 'cancelled')
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (country, domain, request_id, attempt);

CREATE TABLE IF NOT EXISTS corpscout.website_company_lookup_candidates (
    country LowCardinality(String), domain String, request_id String, attempt UInt32,
    company_id String, selected Bool, legal_name String, status String,
    primary_street_address String, primary_postal_code String, primary_city String, activity_description String,
    name_similarity Nullable(Float64), confidence Nullable(Float64), basis String, reasons Array(String),
    industry_status LowCardinality(String), industry_reasons Array(String),
    industry_codes Array(String), industry_versions Array(String), industry_labels Array(String),
    industry_reference_statuses Array(String), industry_reference_labels Array(String),
    industry_sources Array(String), industry_primary Array(UInt8),
    finished_at DateTime64(6, 'UTC'), ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (country, domain, request_id, attempt, company_id);

CREATE TABLE IF NOT EXISTS corpscout.website_company_lookup_evidence (
    country LowCardinality(String), domain String, request_id String, attempt UInt32,
    origin LowCardinality(String), row_index UInt32, kind LowCardinality(String),
    value String, normalized_company_id String, source_url String, quote String,
    finished_at DateTime64(6, 'UTC'), ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (country, domain, request_id, attempt, origin, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_company_lookup_searches (
    country LowCardinality(String), domain String, request_id String, attempt UInt32,
    row_index UInt32, query_id String, kind LowCardinality(String), table_name String,
    sql String, parameters Map(String, String), status LowCardinality(String),
    duration_ms Float64, http_status Nullable(UInt16), row_count UInt32,
    returned_company_ids Array(String), error String,
    finished_at DateTime64(6, 'UTC'), ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (country, domain, request_id, attempt, row_index);

CREATE VIEW IF NOT EXISTS corpscout.website_company_lookup_results_latest AS
SELECT * FROM corpscout.website_company_lookup_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC LIMIT 1 BY country, domain;

-- Filter AFTER choosing the newest attempt. An unresolved new lookup must not
-- silently revive an older proposal. Acceptance into a country domain table is separate.
CREATE VIEW IF NOT EXISTS corpscout.website_company_lookup_proposals AS
SELECT * FROM (
    SELECT * FROM corpscout.website_company_lookup_results FINAL
    ORDER BY finished_at DESC, request_id DESC, attempt DESC LIMIT 1 BY country, domain
) WHERE found AND status = 'matched';

-- Matching is an optional extension of the existing recurring crawl request.
ALTER TABLE corpscout.website_full_crawl_requests
    ADD COLUMN IF NOT EXISTS match_company Bool DEFAULT false,
    ADD COLUMN IF NOT EXISTS company_country LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS skip_company_matching_if_mapped Bool DEFAULT true,
    ADD CONSTRAINT IF NOT EXISTS valid_matching_country CHECK NOT match_company OR company_country = 'SE';
CREATE OR REPLACE VIEW corpscout.website_full_crawl_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision, match_company, company_country, skip_company_matching_if_mapped
FROM corpscout.website_full_crawl_requests FINAL;
ALTER TABLE corpscout.website_full_crawl_results
    ADD COLUMN IF NOT EXISTS company_matching_status LowCardinality(String) DEFAULT '';

-- Matching is an optional extension of the existing recurring crawl request.
ALTER TABLE corpscout.website_site_info_requests
    ADD COLUMN IF NOT EXISTS match_company Bool DEFAULT false,
    ADD COLUMN IF NOT EXISTS company_country LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS skip_company_matching_if_mapped Bool DEFAULT true,
    ADD CONSTRAINT IF NOT EXISTS valid_matching_country CHECK NOT match_company OR company_country = 'SE';
CREATE OR REPLACE VIEW corpscout.website_site_info_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision, match_company, company_country, skip_company_matching_if_mapped
FROM corpscout.website_site_info_requests FINAL;
ALTER TABLE corpscout.website_site_info_results
    ADD COLUMN IF NOT EXISTS company_matching_status LowCardinality(String) DEFAULT '';
