CREATE DATABASE IF NOT EXISTS corpscout;

-- The Sweden OSM address-point gazetteer, published from the host-local build DuckDB
-- (data/sweden_address_osm_source.duckdb, table sweden_address_osm.address_points) so the
-- OSM reference can be queried directly in ClickHouse instead of only inside the geocoder.
--
-- One row per OSM address object (node or way carrying addr:housenumber in Sweden), mirroring
-- the DuckDB build's final projection. Coordinates are WGS84. Snapshot provenance columns
-- (source_md5, source_snapshot_at, source_object_key, ...) tell a consumer which Geofabrik OSM
-- snapshot a row came from -- the same source_md5 the geocode store carries as reference_md5.
--
-- normalized_match_key is the join bridge to corpscout.se_address_geocodes.normalized_match_key.
-- It is NOT the OSM build's own normalized_* columns (address_match_key): it is recomputed with
-- the RESOLVER's normalization (address_resolution/search_documents.py _compact_text_sql:
-- strip_accents(lower(nfc_normalize(...))) with non-alphanumerics stripped) in the form
-- "<postcode>|<street><house>", so a matched geocode outcome finds its OSM point by key. The
-- build's own address_match_key is kept verbatim beside it for provenance.
--
-- The whole table is replaced atomically each run by an EXCHANGE TABLES publish, so readers
-- never observe a partial or empty table.

CREATE TABLE IF NOT EXISTS corpscout.se_osm_address_points
(
    source_record_id String,
    osm_type LowCardinality(String),
    osm_id Int64,
    country_code LowCardinality(String),
    street String,
    house_number String,
    unit String,
    postcode String,
    city String,
    place String,
    full_address String,
    normalized_street String,
    normalized_house_number String,
    normalized_postcode String,
    normalized_city String,
    address_match_key String,
    normalized_match_key String,
    longitude Float64,
    latitude Float64,
    coordinate_method LowCardinality(String),
    source_record_url String,
    source_tags_json String,
    source_url String,
    source_object_key String,
    source_md5 String,
    source_snapshot_at DateTime64(3, 'UTC'),
    source_retrieved_at DateTime64(3, 'UTC'),
    published_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (normalized_postcode, normalized_street, normalized_house_number);
