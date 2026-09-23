CREATE DATABASE IF NOT EXISTS corpscout;

-- Preserve the latest legacy row per IP as dated history. No network lookups.
-- Repeatable result IDs collapse retries. Old checked_at values cannot replace newer data.
-- The temporary import view remains until migration 435 verifies and retires the source.
CREATE VIEW IF NOT EXISTS corpscout.ip_enrichment_legacy_geoip_import AS
SELECT
    if(isIPv4String(ip), toString(toIPv4(ip)), toString(toIPv6(ip))) AS canonical_ip,
    reinterpretAsUUID(MD5(toJSONString(tuple('legacy-geoip-import-v1', ip, enriched_at, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch)))) AS result_id,
    toUUID('cd603d91-8a85-52f4-9b74-61f95d2f763a') AS task_id,
    toUUID('cd603d91-8a85-52f4-9b74-61f95d2f763a') AS execution_id,
    concat('legacy-geoip:', ip) AS input_id,
    'migration:434:commoncrawl_ip_geoip' AS source_run_id,
    'legacy-geoip-import-v1' AS processor_version,
    toUInt32(1) AS attempt,
    toDateTime64(enriched_at, 6, 'UTC') AS completed_at,
    toDateTime64(enriched_at, 6, 'UTC') AS city_checked_at,
    toDateTime64(enriched_at, 6, 'UTC') AS asn_checked_at,
    'not_attempted' AS rdap_lookup_status,
    ip_scope,
    city_lookup_status,
    asn_lookup_status,
    continent_code,
    continent_name,
    country_iso_code,
    country_name,
    country_geoname_id,
    registered_country_iso_code,
    registered_country_name,
    subdivision_iso_codes,
    subdivision_names,
    city_geoname_id,
    city_name,
    latitude,
    longitude,
    accuracy_radius_km,
    timezone,
    asn,
    asn_organization,
    city_network,
    asn_network,
    city_db_build_epoch,
    asn_db_build_epoch
FROM corpscout.commoncrawl_ip_geoip FINAL;

INSERT INTO corpscout.ip_enrichment_results
(ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version, attempt, completed_at, city_checked_at, asn_checked_at, rdap_lookup_status, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch)
SELECT canonical_ip AS ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version, attempt, completed_at, city_checked_at, asn_checked_at, rdap_lookup_status, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch
FROM corpscout.ip_enrichment_legacy_geoip_import;
