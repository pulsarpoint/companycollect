DROP TABLE IF EXISTS corpscout.se_company_person_role;
DROP TABLE IF EXISTS corpscout.se_company_person_role_draft;

-- Restore the removed schema when rolling this migration back. Its former
-- derived rows cannot be reconstructed by a schema rollback.
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
