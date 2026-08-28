CREATE DATABASE IF NOT EXISTS corpscout;

-- Removes the pre-market-flags serving view parked under _retired by 000344's staged swap
-- (its refresh loop was stopped by that migration's SYSTEM STOP VIEW). Transitional swap
-- machinery with zero readers by construction -- nothing ever read the _retired name.
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;
