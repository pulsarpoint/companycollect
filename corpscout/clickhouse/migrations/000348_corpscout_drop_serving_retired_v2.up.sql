CREATE DATABASE IF NOT EXISTS corpscout;

-- Removes the pre-eodhd serving view parked under _retired by 000347's staged swap (its
-- refresh loop was stopped by that migration's SYSTEM STOP VIEW). Transitional swap machinery
-- with zero readers by construction.
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;
