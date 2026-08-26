-- Reverts 000325. Drops only the served overlay view this migration created. The fast
-- serving table se_address_geocodes_current it read from is owned by 000320 and is left
-- untouched, as is the store and the centroid reference tables.

CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.se_address_geocodes_served;
