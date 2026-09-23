CREATE DATABASE IF NOT EXISTS corpscout;

-- Deploy readers and retire the legacy writer before applying this migration.
-- Compare SHA-256 fingerprints of every copied field, including NULLs and arrays.
-- Fixed-size fingerprints bound the EXCEPT working set for millions of records.
-- Refuse deletion if any current source row is missing or differs in the destination.
SELECT throwIf(count() > 0, 'Legacy GeoIP migration is incomplete - refusing to drop the source')
FROM (
    SELECT result_id, SHA256(toJSONString(tuple(canonical_ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version, attempt, completed_at, city_checked_at, asn_checked_at, rdap_lookup_status, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch))) AS payload
    FROM corpscout.ip_enrichment_legacy_geoip_import
    EXCEPT DISTINCT
    SELECT result_id, SHA256(toJSONString(tuple(ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version, attempt, completed_at, city_checked_at, asn_checked_at, rdap_lookup_status, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch))) AS payload
    FROM corpscout.ip_enrichment_results FINAL
    WHERE processor_version = 'legacy-geoip-import-v1'
);
DROP VIEW IF EXISTS corpscout.ip_enrichment_legacy_geoip_import;
DROP VIEW IF EXISTS corpscout.commoncrawl_ip_geoip_current;
DROP TABLE IF EXISTS corpscout.commoncrawl_ip_geoip;
