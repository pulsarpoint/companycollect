CREATE DATABASE IF NOT EXISTS corpscout;

-- Immutable selections, prepared before processing, like company_brave_search_input.
-- input_id is unique within task_id and identifies a source record/IP submission.
-- Different sources may submit the same canonical IP. Workers reuse enrichment by IP.
-- Producers must validate uniqueness and freeze the batch before queue admission.
CREATE TABLE IF NOT EXISTS corpscout.ip_enrichment_input
(
    task_id UUID,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_input CHECK notEmpty(trimBoth(input_id))
        AND position(input_id, char(0)) = 0 AND notEmpty(trimBoth(source_name)),
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id);

-- One immutable row per processing attempt, with independent component outcomes.
-- Retries of a write reuse result_id and the exact same row. A new lookup uses a new ID.
-- Keep history indefinitely. There is no TTL or source-specific dependency.
-- checked_at is the actual lookup time, including when reusing a cached result.
-- Nullable fields represent unavailable information. No raw RDAP JSON is duplicated.
CREATE TABLE IF NOT EXISTS corpscout.ip_enrichment_results
(
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    result_id UUID,
    task_id UUID,
    execution_id UUID,
    input_id String,
    source_run_id String,
    processor_version LowCardinality(String),
    attempt UInt32 DEFAULT 1,
    completed_at DateTime64(6, 'UTC'),
    ip_scope LowCardinality(String),

    city_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3, 'retryable_error' = 4,
        'terminal_error' = 5),
    city_checked_at Nullable(DateTime64(6, 'UTC')),
    city_error_code Nullable(String),
    city_retry_after Nullable(DateTime64(6, 'UTC')),
    continent_code Nullable(String),
    continent_name Nullable(String),
    country_iso_code Nullable(String),
    country_name Nullable(String),
    country_geoname_id Nullable(UInt32),
    registered_country_iso_code Nullable(String),
    registered_country_name Nullable(String),
    subdivision_iso_codes Array(String),
    subdivision_names Array(String),
    city_geoname_id Nullable(UInt32),
    city_name Nullable(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    accuracy_radius_km Nullable(UInt16),
    timezone Nullable(String),
    city_network Nullable(String),
    city_db_build_epoch Nullable(DateTime('UTC')),

    asn_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3, 'retryable_error' = 4,
        'terminal_error' = 5),
    asn_checked_at Nullable(DateTime64(6, 'UTC')),
    asn_error_code Nullable(String),
    asn_retry_after Nullable(DateTime64(6, 'UTC')),
    asn Nullable(UInt32),
    asn_organization Nullable(String),
    asn_network Nullable(String),
    asn_db_build_epoch Nullable(DateTime('UTC')),

    rdap_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3, 'retryable_error' = 4,
        'terminal_error' = 5),
    rdap_checked_at Nullable(DateTime64(6, 'UTC')),
    rdap_error_code Nullable(String),
    rdap_retry_after Nullable(DateTime64(6, 'UTC')),
    rdap_network_key Nullable(String),
    rdap_matched_cidr Nullable(String),
    rdap_rir Nullable(String),
    rdap_handle Nullable(String),
    rdap_start_address Nullable(String),
    rdap_end_address Nullable(String),
    rdap_name Nullable(String),
    rdap_registration_type Nullable(String),
    rdap_country_code Nullable(String),
    rdap_statuses Array(String),
    rdap_registrant_handles Array(String),
    rdap_registrant_names Array(String),
    rdap_parent_network_key Nullable(String),
    rdap_parent_handle Nullable(String),
    rdap_self_url Nullable(String),
    rdap_registration_date Nullable(DateTime64(6, 'UTC')),
    rdap_last_changed_at Nullable(DateTime64(6, 'UTC')),

    CONSTRAINT valid_result CHECK result_id != toUUID('00000000-0000-0000-0000-000000000000')
        AND attempt > 0,
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = ReplacingMergeTree
ORDER BY (bucket, ip, result_id);

-- Ordinary view, not a stored current table.
-- Latest attempt metadata/errors remain visible, even when serving older usable data.
-- Each component selects its newest conclusive lookup (found/not_found/not_global).
-- A newer negative lookup clears old values. Errors/not_attempted preserve prior data.
-- Without a conclusive lookup, expose the latest attempt's null payload and status.
-- Tuple aggregation preserves NULLs and prevents fields from different lookups mixing.
-- Data timestamps/result IDs make fallback and age explicit to consumers.
CREATE VIEW IF NOT EXISTS corpscout.ip_enrichment_current AS
SELECT
    bucket,
    ip,
    ip_version,
    latest.1 AS result_id,
    latest.2 AS task_id,
    latest.3 AS execution_id,
    latest.4 AS input_id,
    latest.5 AS source_run_id,
    latest.6 AS processor_version,
    latest.7 AS attempt,
    latest.8 AS completed_at,
    latest.9 AS ip_scope,
    latest.10 AS city_lookup_status,
    latest.11 AS city_checked_at,
    latest.12 AS city_error_code,
    latest.13 AS city_retry_after,
    latest.14 AS asn_lookup_status,
    latest.15 AS asn_checked_at,
    latest.16 AS asn_error_code,
    latest.17 AS asn_retry_after,
    latest.18 AS rdap_lookup_status,
    latest.19 AS rdap_checked_at,
    latest.20 AS rdap_error_code,
    latest.21 AS rdap_retry_after,
    city_data.1 AS city_data_result_id,
    city_data.2 AS city_data_status,
    city_data.3 AS city_data_at,
    city_data.4 AS continent_code,
    city_data.5 AS continent_name,
    city_data.6 AS country_iso_code,
    city_data.7 AS country_name,
    city_data.8 AS country_geoname_id,
    city_data.9 AS registered_country_iso_code,
    city_data.10 AS registered_country_name,
    city_data.11 AS subdivision_iso_codes,
    city_data.12 AS subdivision_names,
    city_data.13 AS city_geoname_id,
    city_data.14 AS city_name,
    city_data.15 AS latitude,
    city_data.16 AS longitude,
    city_data.17 AS accuracy_radius_km,
    city_data.18 AS timezone,
    city_data.19 AS city_network,
    city_data.20 AS city_db_build_epoch,
    asn_data.1 AS asn_data_result_id,
    asn_data.2 AS asn_data_status,
    asn_data.3 AS asn_data_at,
    asn_data.4 AS asn,
    asn_data.5 AS asn_organization,
    asn_data.6 AS asn_network,
    asn_data.7 AS asn_db_build_epoch,
    rdap_data.1 AS rdap_data_result_id,
    rdap_data.2 AS rdap_data_status,
    rdap_data.3 AS rdap_data_at,
    rdap_data.4 AS rdap_network_key,
    rdap_data.5 AS rdap_matched_cidr,
    rdap_data.6 AS rdap_rir,
    rdap_data.7 AS rdap_handle,
    rdap_data.8 AS rdap_start_address,
    rdap_data.9 AS rdap_end_address,
    rdap_data.10 AS rdap_name,
    rdap_data.11 AS rdap_registration_type,
    rdap_data.12 AS rdap_country_code,
    rdap_data.13 AS rdap_statuses,
    rdap_data.14 AS rdap_registrant_handles,
    rdap_data.15 AS rdap_registrant_names,
    rdap_data.16 AS rdap_parent_network_key,
    rdap_data.17 AS rdap_parent_handle,
    rdap_data.18 AS rdap_self_url,
    rdap_data.19 AS rdap_registration_date,
    rdap_data.20 AS rdap_last_changed_at
FROM
(
    SELECT
        bucket,
        ip,
        ip_version,
        argMax(tuple(result_id, task_id, execution_id, input_id, source_run_id, processor_version, attempt,
            completed_at, ip_scope, city_lookup_status, city_checked_at, city_error_code, city_retry_after,
            asn_lookup_status, asn_checked_at, asn_error_code, asn_retry_after, rdap_lookup_status, rdap_checked_at,
            rdap_error_code, rdap_retry_after), tuple(completed_at, result_id)) AS latest,
        if(countIf(city_lookup_status IN ('found', 'not_found', 'not_global')) > 0,
            argMaxIf(
                tuple(result_id, city_lookup_status, city_checked_at, continent_code, continent_name, country_iso_code,
                    country_name, country_geoname_id, registered_country_iso_code, registered_country_name,
                    subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude,
                    accuracy_radius_km, timezone, city_network, city_db_build_epoch),
                tuple(ifNull(city_checked_at, completed_at), completed_at, result_id),
                city_lookup_status IN ('found', 'not_found', 'not_global')
            ),
            argMax(
                tuple(result_id, city_lookup_status, city_checked_at, continent_code, continent_name, country_iso_code,
                    country_name, country_geoname_id, registered_country_iso_code, registered_country_name,
                    subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude,
                    accuracy_radius_km, timezone, city_network, city_db_build_epoch),
                tuple(completed_at, result_id)
            )
        ) AS city_data,
        if(countIf(asn_lookup_status IN ('found', 'not_found', 'not_global')) > 0,
            argMaxIf(
                tuple(result_id, asn_lookup_status, asn_checked_at, asn, asn_organization, asn_network, asn_db_build_epoch),
                tuple(ifNull(asn_checked_at, completed_at), completed_at, result_id),
                asn_lookup_status IN ('found', 'not_found', 'not_global')
            ),
            argMax(
                tuple(result_id, asn_lookup_status, asn_checked_at, asn, asn_organization, asn_network, asn_db_build_epoch),
                tuple(completed_at, result_id)
            )
        ) AS asn_data,
        if(countIf(rdap_lookup_status IN ('found', 'not_found', 'not_global')) > 0,
            argMaxIf(
                tuple(result_id, rdap_lookup_status, rdap_checked_at, rdap_network_key, rdap_matched_cidr, rdap_rir,
                    rdap_handle, rdap_start_address, rdap_end_address, rdap_name, rdap_registration_type,
                    rdap_country_code, rdap_statuses, rdap_registrant_handles, rdap_registrant_names,
                    rdap_parent_network_key, rdap_parent_handle, rdap_self_url, rdap_registration_date,
                    rdap_last_changed_at),
                tuple(ifNull(rdap_checked_at, completed_at), completed_at, result_id),
                rdap_lookup_status IN ('found', 'not_found', 'not_global')
            ),
            argMax(
                tuple(result_id, rdap_lookup_status, rdap_checked_at, rdap_network_key, rdap_matched_cidr, rdap_rir,
                    rdap_handle, rdap_start_address, rdap_end_address, rdap_name, rdap_registration_type,
                    rdap_country_code, rdap_statuses, rdap_registrant_handles, rdap_registrant_names,
                    rdap_parent_network_key, rdap_parent_handle, rdap_self_url, rdap_registration_date,
                    rdap_last_changed_at),
                tuple(completed_at, result_id)
            )
        ) AS rdap_data
    FROM corpscout.ip_enrichment_results FINAL
    GROUP BY bucket, ip, ip_version
);
