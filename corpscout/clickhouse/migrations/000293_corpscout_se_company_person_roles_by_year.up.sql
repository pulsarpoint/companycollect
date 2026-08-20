CREATE DATABASE IF NOT EXISTS corpscout;

-- This application-facing table is derived entirely from immutable person and
-- role drafts. Recreate it before the full build so a role is represented by
-- one row for one fiscal year instead of one row spanning an array of years.
DROP TABLE IF EXISTS corpscout.se_company_person_role;

CREATE TABLE corpscout.se_company_person_role
(
    role_id UUID,
    person_id UUID,
    company_id String,
    role_code String,
    role_draft_ids Array(UUID),
    person_draft_ids Array(UUID),
    sources Array(String),
    source_count UInt8 MATERIALIZED toUInt8(length(sources)),
    fiscal_year Nullable(UInt16),
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
