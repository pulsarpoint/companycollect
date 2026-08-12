CREATE DATABASE IF NOT EXISTS corpscout;

RENAME TABLE corpscout.se_company_addresses
    TO corpscout.se_company_addresses_legacy_000255;

CREATE TABLE corpscout.se_company_addresses
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
    source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    updated_from_raw_at DateTime64(3, 'UTC'),
    has_address UInt8,
    address_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (company_id, address_type, source, observed_at, observation_fingerprint);

INSERT INTO corpscout.se_company_addresses
(
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
    updated_from_raw_at,
    has_address,
    address_fingerprint,
    observation_fingerprint,
    observed_at
)
WITH
    toUInt8(
        ifNull(trim(raw_address), '') != ''
        OR ifNull(trim(street_address), '') != ''
        OR ifNull(trim(care_of), '') != ''
        OR ifNull(trim(postal_code), '') != ''
        OR ifNull(trim(post_town), '') != ''
    ) AS address_present,
    lower(hex(SHA256(arrayStringConcat([
        ifNull(raw_address, ''),
        ifNull(street_address, ''),
        ifNull(care_of, ''),
        ifNull(postal_code, ''),
        ifNull(post_town, ''),
        ifNull(country_code, ''),
        toString(address_present)
    ], char(31))))) AS fingerprint
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
    updated_from_raw_at,
    address_present,
    fingerprint,
    fingerprint,
    updated_from_raw_at
FROM corpscout.se_company_addresses_legacy_000255;

DROP TABLE corpscout.se_company_addresses_legacy_000255;

CREATE OR REPLACE VIEW corpscout.se_company_addresses_current AS
SELECT
    company_id,
    address_type,
    source,
    latest.1 AS raw_address,
    latest.2 AS street_address,
    latest.3 AS care_of,
    latest.4 AS postal_code,
    latest.5 AS post_town,
    latest.6 AS country_code,
    latest.7 AS source_run_id,
    latest.8 AS source_record_id,
    latest.9 AS source_payload_hash,
    latest.10 AS source_record_uid,
    latest.11 AS updated_from_raw_at,
    latest.12 AS has_address,
    latest.13 AS address_fingerprint,
    latest.14 AS observation_fingerprint,
    latest.15 AS observed_at,
    toUInt8(1) AS has_observation
FROM
(
    SELECT
        company_id,
        address_type,
        source,
        argMax(
            tuple(
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
                observed_at
            ),
            tuple(observed_at, source_run_id)
        ) AS latest
    FROM corpscout.se_company_addresses
    GROUP BY company_id, address_type, source
);
