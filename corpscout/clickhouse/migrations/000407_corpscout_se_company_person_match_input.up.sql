CREATE DATABASE IF NOT EXISTS corpscout;

-- Current model input is maintained by every person-normalization writer. Model
-- configuration belongs to the last attempt, never to the source-data snapshot.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match_input
(
    company_id String,
    data_hash FixedString(64),
    bindings_hash FixedString(64),
    input_snapshot String,
    eligible Bool,
    hash_version UInt16,
    normalized_at DateTime64(3, 'UTC'),
    computed_at DateTime64(6, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^[0-9]{10}([0-9]{2})?$'),
    CONSTRAINT valid_snapshot CHECK JSONType(input_snapshot) = 'Object'
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (company_id);

-- Empty hashes mean unknown for pre-migration attempts. Do not fabricate their
-- provenance from today's source rows or automatically resend every company.
ALTER TABLE corpscout.se_company_person_match_state
    ADD COLUMN IF NOT EXISTS data_hash String DEFAULT '',
    ADD COLUMN IF NOT EXISTS bindings_hash String DEFAULT '',
    ADD COLUMN IF NOT EXISTS prompt_hash String DEFAULT '',
    ADD COLUMN IF NOT EXISTS model_hash String DEFAULT '',
    ADD COLUMN IF NOT EXISTS input_snapshot String DEFAULT '',
    ADD COLUMN IF NOT EXISTS config_snapshot String DEFAULT '';
