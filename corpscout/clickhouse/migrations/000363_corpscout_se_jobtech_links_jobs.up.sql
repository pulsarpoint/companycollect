CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per successfully validated dated archive. Raw rows include
-- Platsbanken for auditability while downstream JobTech serving tables contain
-- only advertisements whose canonical publisher is external.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_snapshots
(
    snapshot_uid FixedString(64),
    snapshot_date Date,
    catalog_url String,
    source_url String,
    archive_object_key String,
    archive_sha256 FixedString(64),
    archive_etag String,
    archive_size_bytes UInt64,
    raw_member_path String,
    raw_member_size_bytes UInt64,
    raw_row_count UInt64,
    platsbanken_row_count UInt64,
    external_row_count UInt64,
    external_provider_count UInt16,
    source_last_modified_at Nullable(DateTime64(3, 'UTC')),
    source_run_id String,
    retrieved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(retrieved_at)
PARTITION BY toYear(snapshot_date)
ORDER BY (snapshot_date, snapshot_uid);

-- The durable advertisement identity is the SHA-256 hex digest of canonical
-- provider, a NUL separator, and provider-owned source identifier. JobTech's
-- own id and URL may change while that publisher identity remains stable.
-- version_uid hashes that identity with the canonical normalized serving
-- payload and deliberately excludes observation and ingestion timestamps.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_versions
(
    version_uid FixedString(64),
    source_job_ad_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    jobtech_links_id String,
    source_hashsum String,
    version_at DateTime64(3, 'UTC'),
    source_first_seen_at Nullable(DateTime64(3, 'UTC')),
    publication_at Nullable(DateTime64(3, 'UTC')),
    display_publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    is_valid UInt8,
    canonical_url String,
    headline_original String CODEC(ZSTD(3)),
    brief_description_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    employer_name String,
    employer_url String,
    employer_logo_url String,
    employment_types Array(String),
    workplace_type LowCardinality(String),
    number_of_vacancies Nullable(UInt32),
    occupation_concept_id String,
    occupation_label_original LowCardinality(String),
    ssyk_level4_code LowCardinality(String),
    experience_requirements_original String CODEC(ZSTD(3)),
    skills_original String CODEC(ZSTD(3)),
    qualifications_original String CODEC(ZSTD(3)),
    responsibilities_original String CODEC(ZSTD(3)),
    education_requirements_original String CODEC(ZSTD(3)),
    job_benefits_original String CODEC(ZSTD(3)),
    work_hours_original String CODEC(ZSTD(3)),
    snapshot_uid FixedString(64),
    source_url String,
    source_object_key String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(version_at)
ORDER BY (provider, source_job_ad_uid, version_at, version_uid);

-- A compact daily presence fact. Content is held once in the version table and
-- this table records which version was present in each successful snapshot.
-- observation_uid hashes snapshot_uid with source_job_ad_uid.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_observations
(
    observation_uid FixedString(64),
    snapshot_uid FixedString(64),
    snapshot_date Date,
    observed_at DateTime64(3, 'UTC'),
    source_job_ad_uid FixedString(64),
    version_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    jobtech_links_id String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (provider, source_job_ad_uid, snapshot_date, observation_uid);

-- One normalized workplace location per content version. A JobTech ad can
-- contain several municipalities or countries.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_location_versions
(
    location_uid FixedString(64),
    version_uid FixedString(64),
    source_job_ad_uid FixedString(64),
    provider LowCardinality(String),
    location_index UInt16,
    municipality_concept_id String,
    municipality_name_original LowCardinality(String),
    region_concept_id String,
    region_name_original LowCardinality(String),
    country_concept_id String,
    country_name_original LowCardinality(String),
    version_at DateTime64(3, 'UTC'),
    source_run_id String,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(version_at)
ORDER BY (
    provider,
    source_job_ad_uid,
    version_uid,
    location_index,
    location_uid
);

-- Only JobTech's accepted binary enrichments are exported. Scored model
-- candidates remain in raw object storage and DuckDB because they are not
-- employer-declared job requirements.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_enrichment_versions
(
    enrichment_uid FixedString(64),
    version_uid FixedString(64),
    source_job_ad_uid FixedString(64),
    provider LowCardinality(String),
    enrichment_type LowCardinality(String),
    concept_label_original LowCardinality(String),
    matched_term_original LowCardinality(String),
    term_misspelled UInt8,
    version_at DateTime64(3, 'UTC'),
    source_run_id String,
    ingested_at DateTime64(3, 'UTC'),
    CONSTRAINT se_jobtech_links_enrichment_type
        CHECK enrichment_type IN ('occupation', 'competency', 'trait', 'geo')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(version_at)
ORDER BY (
    provider,
    source_job_ad_uid,
    version_uid,
    enrichment_type,
    enrichment_uid
);

-- Consecutive daily observations form an interval. A closed interval ends at
-- the first successful snapshot where the advertisement is absent, so that end
-- is explicitly estimated rather than presented as a source removal event.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_active_intervals
(
    source_job_ad_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    interval_number UInt16,
    active_from DateTime64(3, 'UTC'),
    active_to Nullable(DateTime64(3, 'UTC')),
    active_to_basis LowCardinality(String),
    is_end_estimated UInt8,
    first_snapshot_date Date,
    last_snapshot_date Date,
    first_observed_at DateTime64(3, 'UTC'),
    last_observed_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYear(active_from)
ORDER BY (provider, source_job_ad_uid, active_from, interval_number);

-- Atomically replaced serving snapshot containing one row per current
-- external publisher advertisement. Locations remain normalized in the
-- location-version table and join through version_uid.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_current
(
    source_job_ad_uid FixedString(64),
    version_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    jobtech_links_id String,
    source_hashsum String,
    version_at DateTime64(3, 'UTC'),
    active_from DateTime64(3, 'UTC'),
    source_first_seen_at Nullable(DateTime64(3, 'UTC')),
    publication_at Nullable(DateTime64(3, 'UTC')),
    display_publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    is_valid UInt8,
    canonical_url String,
    headline_original String CODEC(ZSTD(3)),
    brief_description_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    employer_name String,
    employer_url String,
    employer_logo_url String,
    employment_types Array(String),
    workplace_type LowCardinality(String),
    number_of_vacancies Nullable(UInt32),
    occupation_concept_id String,
    occupation_label_original LowCardinality(String),
    ssyk_level4_code LowCardinality(String),
    snapshot_uid FixedString(64),
    snapshot_date Date,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (provider, source_job_ad_uid);

-- Company resolution is intentionally separate from immutable source data.
-- A match belongs to the observed content version so an employer-name change
-- cannot silently inherit an older company assignment.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ad_company_matches
(
    match_uid FixedString(64),
    source_job_ad_uid FixedString(64),
    version_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    company_id String,
    match_status LowCardinality(String),
    match_method LowCardinality(String),
    confidence Float32,
    employer_name_original String,
    normalized_employer_name String,
    company_legal_name String,
    evidence_summary String CODEC(ZSTD(3)),
    matcher_version LowCardinality(String),
    snapshot_uid FixedString(64),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    reviewed_by String,
    review_note String CODEC(ZSTD(3)),
    updated_at DateTime64(3, 'UTC'),
    CONSTRAINT se_jobtech_links_match_status
        CHECK match_status IN ('candidate', 'accepted', 'rejected'),
    CONSTRAINT se_jobtech_links_match_confidence
        CHECK confidence >= 0 AND confidence <= 1
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_job_ad_uid, version_uid, company_id, match_uid);
