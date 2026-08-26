CREATE DATABASE IF NOT EXISTS corpscout;

-- Robust city centroid reference table for the SE coarse-centroid geocode
-- fallback (serving-overlay architecture). One row per city key (accent-
-- preserving upper/trim of the SE post_town, see
-- sweden_company/centroid_keys.py:city_key_sql -- Swedish letters aa/ae/oe
-- MUST survive: this key intentionally does not reuse _compact_text_sql's
-- accent-stripping), derived from OSM address points via the
-- median-of-lat/median-of-lon robust centroid with a >=3-point quality gate
-- (sweden_company/centroid_derivation.py). point_count and spread_meters
-- (max haversine distance from the centroid) travel with each row so a poor
-- centroid can be filtered or downgraded by the serving read.
--
-- This is the coarser rung of the centroid ladder: the serving overlay falls
-- back to a city centroid only when no acceptable postcode centroid exists
-- for the address. Like se_postcode_centroids, this table is NOT the
-- geocode store -- it is a separate reference table joined in at READ time,
-- republished wholesale by the sweden_geocode_centroids_clickhouse asset on
-- each run. source_snapshot_at carries the OSM snapshot timestamp.
CREATE TABLE IF NOT EXISTS corpscout.se_city_centroids
(
    key String,
    latitude Float64,
    longitude Float64,
    point_count UInt32,
    spread_meters Float64,
    source_snapshot_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY key;
