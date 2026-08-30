CREATE DATABASE IF NOT EXISTS corpscout;

-- Greenhouse is the explicit schema template. The three AS clauses below only
-- copy DDL. Every provider receives physically separate tables and pipelines.
CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_boards
(
    provider_board_id String,
    board_token String,
    display_name String,
    board_url String,
    enabled UInt8,
    configured_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY provider_board_id;

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_board_company_links
(
    provider_board_id String,
    company_id String,
    match_method LowCardinality(String),
    evidence_url String,
    reviewed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (provider_board_id, company_id);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_board_snapshots
(
    snapshot_uid FixedString(64),
    provider_board_id String,
    source_run_id String,
    source_url String,
    source_object_key String,
    retrieved_at DateTime64(3, 'UTC'),
    http_status UInt16,
    job_count UInt64
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(retrieved_at)
ORDER BY (provider_board_id, retrieved_at, snapshot_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_job_ad_versions
(
    version_uid FixedString(64),
    provider_board_id String,
    source_job_ad_id String,
    company_id String,
    content_hash FixedString(64),
    title_original String CODEC(ZSTD(3)),
    description_html_original String CODEC(ZSTD(6)),
    description_text_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    employer_name String,
    department_name LowCardinality(String),
    team_name LowCardinality(String),
    employment_type LowCardinality(String),
    workplace_type LowCardinality(String),
    is_remote UInt8,
    publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    job_url String,
    apply_url String,
    source_url String,
    source_object_key String,
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(retrieved_at)
ORDER BY (provider_board_id, source_job_ad_id, version_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_job_ad_events
(
    event_uid FixedString(64),
    provider_board_id String,
    source_job_ad_id String,
    company_id String,
    event_at DateTime64(3, 'UTC'),
    effective_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String),
    is_active UInt8,
    is_estimated UInt8,
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(effective_at)
ORDER BY (provider_board_id, source_job_ad_id, effective_at, event_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_job_ad_current
(
    provider_board_id String,
    source_job_ad_id String,
    company_id String,
    content_hash FixedString(64),
    title_original String CODEC(ZSTD(3)),
    description_html_original String CODEC(ZSTD(6)),
    description_text_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    employer_name String,
    department_name LowCardinality(String),
    team_name LowCardinality(String),
    employment_type LowCardinality(String),
    workplace_type LowCardinality(String),
    is_remote UInt8,
    publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    job_url String,
    apply_url String,
    source_url String,
    source_object_key String,
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (provider_board_id, source_job_ad_id);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_job_ad_location_versions
(
    location_uid FixedString(64),
    version_uid FixedString(64),
    provider_board_id String,
    source_job_ad_id String,
    company_id String,
    location_index UInt16,
    city String,
    region String,
    country_code LowCardinality(String),
    street_address String,
    postal_code String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(retrieved_at)
ORDER BY (provider_board_id, source_job_ad_id, version_uid, location_index, location_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_greenhouse_job_ad_compensation_versions
(
    compensation_uid FixedString(64),
    version_uid FixedString(64),
    provider_board_id String,
    source_job_ad_id String,
    company_id String,
    currency LowCardinality(String),
    interval LowCardinality(String),
    minimum_amount Nullable(Float64),
    maximum_amount Nullable(Float64),
    compensation_text String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYYYYMM(retrieved_at)
ORDER BY (provider_board_id, source_job_ad_id, version_uid, compensation_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_lever_boards AS corpscout.se_greenhouse_boards;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_board_company_links AS corpscout.se_greenhouse_board_company_links;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_board_snapshots AS corpscout.se_greenhouse_board_snapshots;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_job_ad_versions AS corpscout.se_greenhouse_job_ad_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_job_ad_events AS corpscout.se_greenhouse_job_ad_events;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_job_ad_current AS corpscout.se_greenhouse_job_ad_current;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_job_ad_location_versions AS corpscout.se_greenhouse_job_ad_location_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_lever_job_ad_compensation_versions AS corpscout.se_greenhouse_job_ad_compensation_versions;

CREATE TABLE IF NOT EXISTS corpscout.se_ashby_boards AS corpscout.se_greenhouse_boards;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_board_company_links AS corpscout.se_greenhouse_board_company_links;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_board_snapshots AS corpscout.se_greenhouse_board_snapshots;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_job_ad_versions AS corpscout.se_greenhouse_job_ad_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_job_ad_events AS corpscout.se_greenhouse_job_ad_events;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_job_ad_current AS corpscout.se_greenhouse_job_ad_current;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_job_ad_location_versions AS corpscout.se_greenhouse_job_ad_location_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_ashby_job_ad_compensation_versions AS corpscout.se_greenhouse_job_ad_compensation_versions;

CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_boards AS corpscout.se_greenhouse_boards;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_board_company_links AS corpscout.se_greenhouse_board_company_links;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_board_snapshots AS corpscout.se_greenhouse_board_snapshots;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_job_ad_versions AS corpscout.se_greenhouse_job_ad_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_job_ad_events AS corpscout.se_greenhouse_job_ad_events;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_job_ad_current AS corpscout.se_greenhouse_job_ad_current;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_job_ad_location_versions AS corpscout.se_greenhouse_job_ad_location_versions;
CREATE TABLE IF NOT EXISTS corpscout.se_smartrecruiters_job_ad_compensation_versions AS corpscout.se_greenhouse_job_ad_compensation_versions;
