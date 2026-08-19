CREATE DATABASE IF NOT EXISTS corpscout;

-- Application-facing Sweden company people. Every row is an LLM-normalized
-- profile built from one or more immutable source observations in
-- se_company_person_draft. Roles remain in the separate company-person role
-- relation so one profile can carry several canonical roles.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person
(
    person_id UUID,
    company_id String,
    name String,
    description Nullable(String),

    draft_ids Array(UUID),
    draft_set_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(arrayStringConcat(
            arrayMap(draft_id -> toString(draft_id), arraySort(draft_ids)),
            '\n'
        )))),
    profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'se-company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            toString(length(lowerUTF8(trim(ifNull(description, ''))))), ':',
            lowerUTF8(trim(ifNull(description, '')))
        )))),

    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),

    CONSTRAINT has_source_observation CHECK notEmpty(draft_ids),
    CONSTRAINT has_person_name CHECK trim(name) != ''
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (company_id, person_id);
