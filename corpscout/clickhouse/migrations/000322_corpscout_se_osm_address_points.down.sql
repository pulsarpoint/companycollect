CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000322. The gazetteer is a pure republish of the host-local OSM build DuckDB and
-- holds no state no other table keeps -- rematerializing the publish asset refills it.
DROP TABLE IF EXISTS corpscout.se_osm_address_points;
