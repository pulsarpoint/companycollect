CREATE DATABASE IF NOT EXISTS corpscout;

-- The Sweden OSM named-road gazetteer, published from the host-local build DuckDB
-- (data/sweden_address_osm_source.duckdb, table sweden_address_osm.street_segments) alongside
-- the address points in 000322. One row per named OSM way carrying a highway tag, mirroring the
-- DuckDB build's final projection with a representative midpoint coordinate (WGS84).
--
-- normalized_match_key is postcode-less here (a street segment has no house number or postcode):
-- it is the RESOLVER's _compact_text_sql applied to the street name, so a street-level geocode
-- outcome can be related back to its road. Snapshot provenance mirrors the address-point table.
--
-- Replaced atomically each run by an EXCHANGE TABLES publish, so readers never see a partial table.

CREATE TABLE IF NOT EXISTS corpscout.se_osm_street_segments
(
    source_record_id String,
    osm_id Int64,
    street String,
    normalized_street String,
    normalized_match_key String,
    highway LowCardinality(String),
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
ORDER BY (normalized_street);
