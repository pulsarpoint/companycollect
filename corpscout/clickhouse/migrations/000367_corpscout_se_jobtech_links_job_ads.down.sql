CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_jobtech_links_job_ads;

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
