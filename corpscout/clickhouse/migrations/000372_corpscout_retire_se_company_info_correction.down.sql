CREATE DATABASE IF NOT EXISTS corpscout;

-- Recreates the retired ledger empty, in its 000297 shape with 000299's widened
-- has_company check -- this only restores the schema, not the 4 rows it held.
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, created_at, correction_id);

GRANT INSERT ON corpscout.se_company_info_correction
TO corpscout_person_correction_writer;
