CREATE DATABASE IF NOT EXISTS corpscout;

-- The data dropped by this migration's up.sql cannot be restored -- mirrors
-- 000313's down.sql precedent. Rolling back only recreates both tables'
-- schemas empty (copied from 000145's company_people_all CREATE and
-- 000288:6-43's se_company_person_draft CREATE, renamed here to its
-- post-000290-rename name) so the tables exist again for a subsequent
-- re-apply or a manual backfill.
CREATE TABLE IF NOT EXISTS corpscout.company_people_all
(
    country_iso2 LowCardinality(String),
    company_id String,
    company_name String,
    first_name String,
    last_name String,
    full_name_normalized String, -- lowerUTF8(trim(first || ' ' || last))
    role_original String,
    role_kind LowCardinality(String),
    signatory_kind LowCardinality(String),
    fiscal_year Int32,
    identifier_kind LowCardinality(String), -- '' for SE (no public person id)
    identifier_value String,
    source LowCardinality(String), -- 'se_xbrl_signatures'
    source_statement_key String,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_people_name full_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (full_name_normalized, country_iso2, company_id, fiscal_year);

CREATE TABLE IF NOT EXISTS corpscout.se_company_person_draft_legacy
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
