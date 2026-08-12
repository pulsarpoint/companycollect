CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE corpscout.se_company_addresses_current_snapshot_000256
(
    company_id String,
    address_type LowCardinality(String),
    source LowCardinality(String),
    raw_address Nullable(String),
    street_address Nullable(String),
    care_of Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    country_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    source_record_uid String,
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_address UInt8,
    address_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC'),
    has_observation UInt8 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (company_id, address_type, source);

INSERT INTO corpscout.se_company_addresses_current_snapshot_000256
SELECT
    company_id,
    address_type,
    source,
    raw_address,
    street_address,
    care_of,
    postal_code,
    post_town,
    country_code,
    source_run_id,
    source_record_id,
    source_payload_hash,
    source_record_uid,
    updated_from_raw_at,
    has_address,
    address_fingerprint,
    observation_fingerprint,
    observed_at,
    has_observation
FROM corpscout.se_company_addresses_current;

DROP VIEW corpscout.se_company_addresses_current;

RENAME TABLE corpscout.se_company_addresses_current_snapshot_000256
    TO corpscout.se_company_addresses_current;
