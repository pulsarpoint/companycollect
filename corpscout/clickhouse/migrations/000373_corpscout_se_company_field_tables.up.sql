CREATE DATABASE IF NOT EXISTS corpscout;

-- 2026-09-02 field registry design. The hand-written publisher of se_company_info gives
-- way to a registry-driven model in three long tables:
--   se_company_field_registry  -- the registry exported from dagster_v3 (one row per
--                                 field plus a field = '*' row carrying the wide
--                                 projection statement), ReplacingMergeTree(version)
--                                 read with argMax(..., version) like se_code_labels
--   se_company_field_candidate -- every source's value for every field, append-only,
--                                 one row per (company, field, source, source record)
--   se_company_field           -- one resolved row per (company, field)
-- Reviewer decisions stay in se_company_info_field_value (000371). Additive only.

CREATE TABLE IF NOT EXISTS corpscout.se_company_field_registry
(
    datatype LowCardinality(String),
    country LowCardinality(String),
    field String,
    value_type LowCardinality(String),
    display_group LowCardinality(String),
    structured Bool,
    python_only Bool,
    sources Array(String),
    policy_name LowCardinality(String),
    policy_version String,
    resolve_sql String,
    registry_version String,
    version DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (datatype, country, field);

-- value is the display form and is never empty: an absent value is no row. value_json
-- carries compare_key (the normalised form agreement is counted on) and the field's
-- structured members. A re-extraction of the same source record replaces its row
-- (ReplacingMergeTree on extracted_at), a new source record adds one, nothing is deleted.
CREATE TABLE IF NOT EXISTS corpscout.se_company_field_candidate
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    source_record_uid String,
    value String,
    value_json String DEFAULT '',
    observed_at DateTime64(3, 'UTC'),
    extracted_at DateTime64(3, 'UTC'),
    extractor_version LowCardinality(String),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        field, '\n', source, '\n', source_record_uid, '\n', value, '\n', value_json)))),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_value CHECK trim(value) != ''
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (company_id, field, source, source_record_uid);

-- decision_id is set when a reviewer decision supplied the value. candidate_count and
-- agreeing_sources describe the eligible candidates the policy saw for that company.
CREATE TABLE IF NOT EXISTS corpscout.se_company_field
(
    company_id String,
    field LowCardinality(String),
    value String,
    value_json String DEFAULT '',
    source LowCardinality(String),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    decision_id Nullable(UUID),
    policy_name LowCardinality(String),
    policy_version String,
    candidate_count UInt16,
    agreeing_sources Array(String),
    registry_version String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_value CHECK trim(value) != ''
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, field);

-- The backoffice resolves one company right after a decision: it runs the field's
-- statement (writes se_company_field) and the projection statement (writes
-- se_company_info). INSERT only, like every other grant to this role.
GRANT INSERT ON corpscout.se_company_field_candidate
TO corpscout_person_correction_writer;

GRANT INSERT ON corpscout.se_company_field
TO corpscout_person_correction_writer;

GRANT INSERT ON corpscout.se_company_info
TO corpscout_person_correction_writer;
