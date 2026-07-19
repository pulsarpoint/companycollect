CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_officers
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    signatory_kind LowCardinality(String), -- 'board_signature' | 'certification' | 'auditor'
    person_seq UInt16,
    first_name String,
    last_name String,
    role_original String,
    role_kind LowCardinality(String), -- normalized, see officers.py mapping, 'unknown' fallback
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq);
