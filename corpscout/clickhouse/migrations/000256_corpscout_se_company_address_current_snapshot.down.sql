RENAME TABLE corpscout.se_company_addresses_current
    TO corpscout.se_company_addresses_current_snapshot_rollback_000256;

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

DROP TABLE corpscout.se_company_addresses_current_snapshot_rollback_000256;
