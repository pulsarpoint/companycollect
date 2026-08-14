CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_addresses_canonical_current
(
    company_id String,
    canonical_address_key FixedString(64),
    canonical_display_address String,
    representative_address_type LowCardinality(String),
    representative_address_source LowCardinality(String),
    representative_source_record_uid String,
    street_address String,
    care_of String,
    postal_code String,
    post_town String,
    country_code LowCardinality(String),
    address_kind LowCardinality(String),
    normalized_street String,
    normalized_postal_code String,
    normalized_post_town String,
    address_types Array(String),
    address_sources Array(String),
    member_count UInt16,
    normalization_run_id String,
    normalized_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, canonical_address_key);

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_members_current
(
    company_id String,
    canonical_address_key FixedString(64),
    address_key FixedString(64),
    address_type LowCardinality(String),
    address_source LowCardinality(String),
    raw_address String,
    display_address String,
    street_address String,
    care_of String,
    postal_code String,
    post_town String,
    country_code LowCardinality(String),
    registry_source_record_uid String,
    registry_source_run_id String,
    source_observed_at DateTime64(3, 'UTC'),
    normalization_run_id String,
    normalized_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    company_id,
    canonical_address_key,
    address_source,
    address_type,
    address_key
);
