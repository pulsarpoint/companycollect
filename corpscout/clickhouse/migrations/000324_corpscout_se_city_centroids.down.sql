-- Reverts 000324. This reference table is republished wholesale by the
-- sweden_geocode_centroids_clickhouse asset, so dropping it loses nothing
-- that a re-materialization does not recreate.

CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_city_centroids;
