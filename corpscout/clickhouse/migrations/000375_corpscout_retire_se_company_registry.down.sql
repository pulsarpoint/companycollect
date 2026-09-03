CREATE DATABASE IF NOT EXISTS corpscout;

-- Recreates the retired pair empty, in its 000257 shape. This restores the schema only,
-- not the observations -- the S3 snapshots and the per-source tables are the archive.
CREATE TABLE IF NOT EXISTS corpscout.se_company_registry_observations
(
    company_id String,
    source LowCardinality(String),
    company_id_raw Nullable(String),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    derived_status LowCardinality(Nullable(String)),
    status_reason LowCardinality(Nullable(String)),
    incorporation_date Nullable(Date),
    dissolution_date Nullable(Date),
    activity_description Nullable(String),
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    marketing_block_code LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_company UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (company_id, source, observed_at, observation_fingerprint);

CREATE TABLE IF NOT EXISTS corpscout.se_company_registry_current
(
    company_id String,
    source LowCardinality(String),
    company_id_raw Nullable(String),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    derived_status LowCardinality(Nullable(String)),
    status_reason LowCardinality(Nullable(String)),
    incorporation_date Nullable(Date),
    dissolution_date Nullable(Date),
    activity_description Nullable(String),
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    marketing_block_code LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_company UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    has_observation UInt8 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (company_id, source);
