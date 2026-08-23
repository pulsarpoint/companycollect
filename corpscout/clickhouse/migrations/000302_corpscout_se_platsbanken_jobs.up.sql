CREATE DATABASE IF NOT EXISTS corpscout;

-- Full, immutable content states. Historical archive rows and prospective
-- JobStream versions share one key so a company timeline can span both eras.
CREATE TABLE IF NOT EXISTS corpscout.se_platsbanken_job_ad_versions
(
    version_uid FixedString(64),
    source_job_ad_id String,
    source_record_id String,
    source_original_id String,
    source_external_id String,
    version_at DateTime64(3, 'UTC'),
    version_kind LowCardinality(String),
    is_removed UInt8,
    publication_at Nullable(DateTime64(3, 'UTC')),
    last_publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    removed_at Nullable(DateTime64(3, 'UTC')),
    employer_org_number String,
    match_eligibility LowCardinality(String),
    employer_name String,
    employer_workplace String,
    employer_url String,
    headline_original String CODEC(ZSTD(3)),
    description_text_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    webpage_url String,
    number_of_vacancies Nullable(UInt64),
    employment_type_concept_id String,
    employment_type_label_original LowCardinality(String),
    salary_type_concept_id String,
    salary_type_label_original LowCardinality(String),
    salary_description_original String,
    duration_concept_id String,
    duration_label_original LowCardinality(String),
    working_hours_concept_id String,
    working_hours_label_original LowCardinality(String),
    scope_min Nullable(Float32),
    scope_max Nullable(Float32),
    experience_required Nullable(UInt8),
    access_to_own_car Nullable(UInt8),
    driving_license_required Nullable(UInt8),
    occupation_concept_id String,
    occupation_label_original LowCardinality(String),
    occupation_group_concept_id String,
    occupation_group_label_original LowCardinality(String),
    occupation_field_concept_id String,
    occupation_field_label_original LowCardinality(String),
    municipality_code String,
    municipality_concept_id String,
    municipality_name_original LowCardinality(String),
    region_code String,
    region_concept_id String,
    region_name_original LowCardinality(String),
    country_code String,
    country_concept_id String,
    country_name_original LowCardinality(String),
    street_address String,
    postcode String,
    city String,
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    source_type LowCardinality(String),
    source_url String,
    source_object_key String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(version_at)
ORDER BY (source_job_ad_id, version_at, version_uid);

-- Sparse removals remain independently queryable and never erase the latest
-- complete content version. effective_at is the lifecycle clock while
-- event_at records when the source emitted or represented the change.
CREATE TABLE IF NOT EXISTS corpscout.se_platsbanken_job_ad_events
(
    event_uid FixedString(64),
    source_job_ad_id String,
    source_record_id String,
    event_at DateTime64(3, 'UTC'),
    effective_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String),
    is_active UInt8,
    active_to_basis LowCardinality(String),
    is_estimated UInt8,
    employer_org_number String,
    source_url String,
    source_object_key String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(effective_at)
ORDER BY (source_job_ad_id, effective_at, event_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_platsbanken_job_ad_requirement_versions
(
    requirement_uid FixedString(64),
    version_uid FixedString(64),
    source_job_ad_id String,
    requirement_level LowCardinality(String),
    requirement_type LowCardinality(String),
    concept_id String,
    label_original LowCardinality(String),
    legacy_ams_taxonomy_id String,
    weight Nullable(Float32),
    source_url String,
    source_object_key String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (
    source_job_ad_id,
    version_uid,
    requirement_level,
    requirement_type,
    concept_id,
    requirement_uid
);

CREATE TABLE IF NOT EXISTS corpscout.se_platsbanken_job_ad_active_intervals
(
    source_job_ad_id String,
    interval_number UInt16,
    employer_org_number String,
    active_from DateTime64(3, 'UTC'),
    active_to Nullable(DateTime64(3, 'UTC')),
    active_to_basis LowCardinality(String),
    is_end_estimated UInt8,
    first_event_at DateTime64(3, 'UTC'),
    last_event_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYear(active_from)
ORDER BY (employer_org_number, active_from, source_job_ad_id, interval_number);

CREATE TABLE IF NOT EXISTS corpscout.company_job_history
(
    country_code LowCardinality(String),
    company_id String,
    source_system LowCardinality(String),
    source_job_ad_id String,
    interval_number UInt16,
    active_from DateTime64(3, 'UTC'),
    active_to Nullable(DateTime64(3, 'UTC')),
    active_to_basis LowCardinality(String),
    is_end_estimated UInt8,
    publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    removed_at Nullable(DateTime64(3, 'UTC')),
    employer_name String,
    headline_original String CODEC(ZSTD(3)),
    description_text_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    number_of_vacancies Nullable(UInt64),
    occupation_concept_id String,
    occupation_label_original LowCardinality(String),
    occupation_group_concept_id String,
    occupation_group_label_original LowCardinality(String),
    occupation_field_concept_id String,
    occupation_field_label_original LowCardinality(String),
    employment_type_concept_id String,
    employment_type_label_original LowCardinality(String),
    duration_concept_id String,
    duration_label_original LowCardinality(String),
    working_hours_concept_id String,
    working_hours_label_original LowCardinality(String),
    municipality_code String,
    municipality_name_original LowCardinality(String),
    region_code String,
    region_name_original LowCardinality(String),
    workplace_country_code String,
    workplace_country_name_original LowCardinality(String),
    webpage_url String,
    source_type LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYear(active_from)
ORDER BY (
    country_code,
    company_id,
    active_from,
    source_system,
    source_job_ad_id,
    interval_number
);

CREATE TABLE IF NOT EXISTS corpscout.company_job_current
(
    country_code LowCardinality(String),
    company_id String,
    source_system LowCardinality(String),
    source_job_ad_id String,
    interval_number UInt16,
    active_from DateTime64(3, 'UTC'),
    active_to Nullable(DateTime64(3, 'UTC')),
    active_to_basis LowCardinality(String),
    is_end_estimated UInt8,
    publication_at Nullable(DateTime64(3, 'UTC')),
    application_deadline Nullable(DateTime64(3, 'UTC')),
    removed_at Nullable(DateTime64(3, 'UTC')),
    employer_name String,
    headline_original String CODEC(ZSTD(3)),
    description_text_original String CODEC(ZSTD(6)),
    detected_language LowCardinality(String),
    number_of_vacancies Nullable(UInt64),
    occupation_concept_id String,
    occupation_label_original LowCardinality(String),
    occupation_group_concept_id String,
    occupation_group_label_original LowCardinality(String),
    occupation_field_concept_id String,
    occupation_field_label_original LowCardinality(String),
    employment_type_concept_id String,
    employment_type_label_original LowCardinality(String),
    duration_concept_id String,
    duration_label_original LowCardinality(String),
    working_hours_concept_id String,
    working_hours_label_original LowCardinality(String),
    municipality_code String,
    municipality_name_original LowCardinality(String),
    region_code String,
    region_name_original LowCardinality(String),
    workplace_country_code String,
    workplace_country_name_original LowCardinality(String),
    webpage_url String,
    source_type LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, source_system, source_job_ad_id);

CREATE TABLE IF NOT EXISTS corpscout.company_hiring_monthly
(
    country_code LowCardinality(String),
    company_id String,
    month_start Date,
    ads_published UInt64,
    advertised_positions UInt64,
    ads_with_known_vacancies UInt64,
    ads_closed UInt64,
    active_ads_end_of_month UInt64,
    active_positions_end_of_month UInt64,
    active_ads_with_known_vacancies UInt64,
    median_open_days Nullable(Float64),
    distinct_occupation_groups UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYear(month_start)
ORDER BY (country_code, company_id, month_start);
