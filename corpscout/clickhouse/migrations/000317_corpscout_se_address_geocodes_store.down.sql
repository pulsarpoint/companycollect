-- Reverts 000317. The store is append-only and holds outcomes no other table keeps once
-- the transition is complete -- re-materializing the geocoding job after reverting the code
-- refills it for the CURRENT policy and reference only. Adopted legacy_adopted_v1 rows are
-- NOT reproducible by any asset and would have to be re-imported by hand.

CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_address_geocodes;
