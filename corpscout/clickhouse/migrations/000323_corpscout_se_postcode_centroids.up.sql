CREATE DATABASE IF NOT EXISTS corpscout;

-- Robust postcode centroid reference table for the SE coarse-centroid geocode
-- fallback (serving-overlay architecture). One row per postcode key
-- (digits-only compaction of the SE postal code, see
-- sweden_company/centroid_keys.py:postcode_key_sql), derived from OSM address
-- points via the median-of-lat/median-of-lon robust centroid with a >=3-point
-- quality gate (sweden_company/centroid_derivation.py). point_count and
-- spread_meters (max haversine distance from the centroid) travel with each
-- row so a poor centroid can be filtered or downgraded by the serving read.
--
-- This table is NOT the geocode store (se_address_geocodes): it is a
-- separate reference table joined in at READ time to fill unmatched/
-- ambiguous identities. It is republished wholesale (create-or-replace) by
-- the sweden_geocode_centroids_clickhouse asset on each run. source_snapshot_at
-- carries the OSM snapshot timestamp the centroid was computed against.
CREATE TABLE IF NOT EXISTS corpscout.se_postcode_centroids
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
