CREATE DATABASE IF NOT EXISTS corpscout;

-- The previous `job_ad_current` shape exposed only advertisements observed in
-- the latest snapshot. Keep one serving table instead: every advertisement is
-- present once and its lifecycle status is resolved from snapshot presence.
CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ads
(
    source_job_ad_uid FixedString(64),
    version_uid FixedString(64),
    provider LowCardinality(String),
    source_identifier String,
    jobtech_links_id String,
    source_hashsum String,
    version_at DateTime64(3, 'UTC'),
    interval_number UInt16,
    status LowCardinality(String),
    active_from DateTime64(3, 'UTC'),
    active_to Nullable(DateTime64(3, 'UTC')),
    active_to_basis LowCardinality(String),
    is_end_estimated UInt8,
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
    resolved_against_snapshot_date Date,
    resolved_at DateTime64(3, 'UTC'),
    CONSTRAINT se_jobtech_links_job_status
        CHECK status IN ('active', 'expired')
)
ENGINE = MergeTree
ORDER BY (status, provider, source_job_ad_uid);

-- application_deadline remains source evidence and is not used as a removal
-- event because publishers can remove, extend, or leave an ad visible around
-- that date. The current-only table was never populated by a Dagster asset.
DROP TABLE IF EXISTS corpscout.se_jobtech_links_job_ad_current;
