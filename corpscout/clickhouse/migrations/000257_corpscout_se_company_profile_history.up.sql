CREATE DATABASE IF NOT EXISTS corpscout;

-- se_company_registry_observations and se_company_registry_current removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

CREATE TABLE IF NOT EXISTS corpscout.se_company_proceeding_observations
(
    company_id String,
    source LowCardinality(String),
    proceeding_code LowCardinality(Nullable(String)),
    effective_date Nullable(Date),
    raw_proceeding Nullable(String),
    proceeding_identity FixedString(64),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_proceeding UInt8,
    proceeding_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (
    company_id,
    source,
    proceeding_identity,
    observed_at,
    observation_fingerprint
);

CREATE TABLE IF NOT EXISTS corpscout.se_company_proceedings_current
(
    company_id String,
    source LowCardinality(String),
    proceeding_code LowCardinality(Nullable(String)),
    effective_date Nullable(Date),
    raw_proceeding Nullable(String),
    proceeding_identity FixedString(64),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_proceeding UInt8,
    proceeding_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    has_observation UInt8 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (company_id, source, proceeding_identity);

CREATE TABLE IF NOT EXISTS corpscout.se_company_industry_observations
(
    company_id String,
    source LowCardinality(String),
    ng1_code LowCardinality(Nullable(String)),
    ng2_code LowCardinality(Nullable(String)),
    ng3_code LowCardinality(Nullable(String)),
    ng4_code LowCardinality(Nullable(String)),
    ng5_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_industry UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (company_id, source, observed_at, observation_fingerprint);

CREATE TABLE IF NOT EXISTS corpscout.se_company_industry_current
(
    company_id String,
    source LowCardinality(String),
    ng1_code LowCardinality(Nullable(String)),
    ng2_code LowCardinality(Nullable(String)),
    ng3_code LowCardinality(Nullable(String)),
    ng4_code LowCardinality(Nullable(String)),
    ng5_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_industry UInt8,
    state_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    has_observation UInt8 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (company_id, source);
