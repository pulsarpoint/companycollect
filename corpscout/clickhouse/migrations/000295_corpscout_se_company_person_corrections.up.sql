CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only ledger of human decisions about Sweden company people. The
-- pipeline applies these as input on every run. Rows are never updated. A
-- later row names an earlier one in supersedes_correction_id to retire it.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    subject_person_id UUID,
    target_person_id Nullable(UUID),
    draft_ids Array(UUID),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, subject_person_id, created_at, correction_id);

-- One row per model call per person. The newest created_at for a
-- (person_id, input_hash) pair is the current suggestion unless a correction
-- approves an older one.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_enrichment_observation
(
    suggestion_id UUID,
    company_id String,
    person_id UUID,
    input_hash FixedString(64),
    draft_ids Array(UUID),
    suggestion String,
    raw_response String,
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT valid_suggestion CHECK isValidJSON(suggestion)
)
ENGINE = MergeTree
ORDER BY (company_id, person_id, input_hash, created_at);

-- Provenance of applied corrections and the published suggestion. A merged
-- person keeps its evidence rows and points at the surviving person.
ALTER TABLE corpscout.se_company_person
    ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] AFTER draft_ids,
    ADD COLUMN IF NOT EXISTS correction_set_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(arrayStringConcat(
            arraySort(arrayMap(id -> toString(id), correction_ids)), '\n'
        )))) AFTER correction_ids,
    ADD COLUMN IF NOT EXISTS suggestion_id Nullable(UUID) AFTER correction_set_hash,
    ADD COLUMN IF NOT EXISTS merged_into_person_id Nullable(UUID) AFTER suggestion_id;

ALTER TABLE corpscout.se_company_person_role
    ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] AFTER person_draft_ids;
