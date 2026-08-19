CREATE DATABASE IF NOT EXISTS corpscout;

-- Reviewable Sweden-specific person profiles. The source tables remain the
-- immutable evidence. These columns identify exactly which source records and
-- source-content versions produced the current draft.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_draft
(
    person_id UUID,
    company_id String,
    name String,
    name_normalized String,
    description Nullable(String),

    bolagsverket_source_record_uids Array(String),
    bolagsverket_profile_hash Nullable(FixedString(64)),
    esef_source_record_uids Array(String),
    esef_profile_hash Nullable(FixedString(64)),
    wikidata_source_record_uids Array(String),
    wikidata_profile_hash Nullable(FixedString(64)),
    source_count UInt8 MATERIALIZED
        toUInt8(notEmpty(bolagsverket_source_record_uids))
        + toUInt8(notEmpty(esef_source_record_uids))
        + toUInt8(notEmpty(wikidata_source_record_uids)),

    match_method LowCardinality(String),
    match_confidence Float32,
    profile_update_method LowCardinality(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,

    review_status LowCardinality(String),
    reviewed_by String,
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    source_run_id String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),

    INDEX idx_se_company_person_draft_name name_normalized
        TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (company_id, person_id);

-- A person can have many roles at the same company. Roles are kept separate
-- from the profile so adding evidence never overwrites another role. The
-- country key makes this table reusable when person resolution expands beyond
-- Sweden.
CREATE TABLE IF NOT EXISTS corpscout.company_person_role
(
    role_id UUID,
    person_id UUID,
    country_code LowCardinality(String),
    company_id String,
    role_name String,
    role_name_normalized String,
    role_category LowCardinality(String),
    status LowCardinality(String),
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    fiscal_year Nullable(UInt16),

    bolagsverket_source_record_uids Array(String),
    bolagsverket_role_hash Nullable(FixedString(64)),
    esef_source_record_uids Array(String),
    esef_role_hash Nullable(FixedString(64)),
    wikidata_source_record_uids Array(String),
    wikidata_role_hash Nullable(FixedString(64)),
    source_count UInt8 MATERIALIZED
        toUInt8(notEmpty(bolagsverket_source_record_uids))
        + toUInt8(notEmpty(esef_source_record_uids))
        + toUInt8(notEmpty(wikidata_source_record_uids)),

    match_confidence Float32,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (country_code, company_id, person_id, role_id);
