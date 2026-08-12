CREATE TABLE corpscout.se_company_addresses_rollback_000255
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
    updated_from_raw_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_from_raw_at)
ORDER BY (company_id, address_type, source, source_record_id);

INSERT INTO corpscout.se_company_addresses_rollback_000255
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
    updated_from_raw_at
FROM corpscout.se_company_addresses_current
WHERE has_address = 1;

DROP VIEW corpscout.se_company_addresses_current;
DROP TABLE corpscout.se_company_addresses;
RENAME TABLE corpscout.se_company_addresses_rollback_000255
    TO corpscout.se_company_addresses;
