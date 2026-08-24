-- Reverts 000314. Structure only -- the rows are not restored. Re-materializing
-- company_serving_current (after reverting the code change that removed the
-- se_company_address_display_current_build model, its schema.yml entry, the ADDRESSES
-- CurrentTable and the section-presence repoint) refills both tables.
--
-- se_company_address_display_current is recreated verbatim from 000267, the migration
-- that created it.
--
-- se_company_address_display_current_build was never owned by this ledger: dbt creates it
-- from se_company_address_display_current_build.sql with materialized='table', inferring
-- its column types from the model's SELECT. It is recreated here with the 000267 display
-- shape -- the column contract the publish step reads -- purely so that this down file
-- leaves nothing dangling. That shape only approximates dbt's inferred types (dbt widens
-- the literal and computed columns), and it is a placeholder only: the
-- authoritative restore is re-materializing the reverted model, which drops and recreates
-- this table with dbt's own types before writing a single row.

CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_display_current
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    source LowCardinality(String),
    raw_address String,
    display_address String,
    normalized_address String,
    street_address String,
    care_of String,
    postal_code String,
    post_town String,
    resolved_country_code LowCardinality(String),
    is_foreign UInt8,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    source_record_uid String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_type, source, address_key);

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_display_current_build
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    source LowCardinality(String),
    raw_address String,
    display_address String,
    normalized_address String,
    street_address String,
    care_of String,
    postal_code String,
    post_town String,
    resolved_country_code LowCardinality(String),
    is_foreign UInt8,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    source_record_uid String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_type, source, address_key);
