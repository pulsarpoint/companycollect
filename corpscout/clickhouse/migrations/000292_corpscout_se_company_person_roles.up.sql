CREATE DATABASE IF NOT EXISTS corpscout;

-- The old country-generic relation was populated before se_company_person was
-- rebuilt and its person IDs no longer identify rows in that table. Replace it
-- with Swedish role evidence and current assignments that use the same draft
-- and person IDs as the active pipeline.
DROP TABLE IF EXISTS corpscout.company_person_role;

-- Immutable role classifications for source person observations. A changed
-- source observation already receives a new person_draft_id. If a static role
-- mapping changes, role_code changes role_draft_id and appends a new mapping
-- observation without overwriting the earlier classification.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_role_draft
(
    role_draft_id UUID,
    person_draft_id UUID,
    company_id String,
    source LowCardinality(String),
    source_record_uid String,
    person_role_hash FixedString(64),
    source_role_code String,
    source_role_name String,
    role_code String,
    fiscal_year Nullable(UInt16),
    source_observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_source_role_code CHECK trim(source_role_code) != '',
    CONSTRAINT has_canonical_role_code CHECK trim(role_code) != ''
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, source, role_draft_id);

-- Application-facing current role assignments. One normalized person can
-- have several role rows. Evidence arrays retain the exact role and person
-- draft observations used to construct each assignment.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_role
(
    role_id UUID,
    person_id UUID,
    company_id String,
    role_code String,
    role_draft_ids Array(UUID),
    person_draft_ids Array(UUID),
    sources Array(String),
    source_count UInt8 MATERIALIZED toUInt8(length(sources)),
    fiscal_years Array(UInt16),
    first_observed_at DateTime64(3, 'UTC'),
    last_observed_at DateTime64(3, 'UTC'),
    role_draft_set_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(arrayStringConcat(
            arrayMap(role_draft_id -> toString(role_draft_id), arraySort(role_draft_ids)),
            '\n'
        )))),
    is_current UInt8,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),

    CONSTRAINT has_role_evidence CHECK notEmpty(role_draft_ids),
    CONSTRAINT has_person_evidence CHECK notEmpty(person_draft_ids),
    CONSTRAINT has_role_code CHECK trim(role_code) != ''
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (company_id, person_id, role_id);
