-- Restore the legacy snapshot from the retained imported history.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_geoip
(
    bucket                              UInt16,
    ip                                  String,
    ip_version                          UInt8,
    ip_scope                            LowCardinality(String),
    city_lookup_status                  LowCardinality(String),
    asn_lookup_status                   LowCardinality(String),
    continent_code                      Nullable(String),
    continent_name                      Nullable(String),
    country_iso_code                    Nullable(String),
    country_name                        Nullable(String),
    country_geoname_id                  Nullable(UInt32),
    registered_country_iso_code         Nullable(String),
    registered_country_name             Nullable(String),
    subdivision_iso_codes               Array(String),
    subdivision_names                   Array(String),
    city_geoname_id                     Nullable(UInt32),
    city_name                           Nullable(String),
    latitude                            Nullable(Float64),
    longitude                           Nullable(Float64),
    accuracy_radius_km                  Nullable(UInt16),
    timezone                            Nullable(String),
    asn                                 Nullable(UInt32),
    asn_organization                    Nullable(String),
    city_network                        Nullable(String),
    asn_network                         Nullable(String),
    city_db_build_epoch                 DateTime('UTC'),
    asn_db_build_epoch                  DateTime('UTC'),
    enriched_at                         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(enriched_at)
ORDER BY (bucket, ip);

CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_ip_geoip_current AS
SELECT *
FROM corpscout.commoncrawl_ip_geoip FINAL;

INSERT INTO corpscout.commoncrawl_ip_geoip
(bucket, ip, ip_version, ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch, enriched_at)
SELECT toUInt16(cityHash64(substring(input_id, length('legacy-geoip:') + 1)) % 256), substring(input_id, length('legacy-geoip:') + 1), if(isIPv4String(substring(input_id, length('legacy-geoip:') + 1)), toUInt8(4), toUInt8(6)), ip_scope, city_lookup_status, asn_lookup_status, continent_code, continent_name, country_iso_code, country_name, country_geoname_id, registered_country_iso_code, registered_country_name, subdivision_iso_codes, subdivision_names, city_geoname_id, city_name, latitude, longitude, accuracy_radius_km, timezone, asn, asn_organization, city_network, asn_network, city_db_build_epoch, asn_db_build_epoch, completed_at
FROM corpscout.ip_enrichment_results FINAL WHERE processor_version = 'legacy-geoip-import-v1';
