CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000326. Drops only the refreshable materialized view this migration created --
-- DROP VIEW takes the view's inner MergeTree table with it. This is a down file undoing
-- exactly what its own up file created, so it is not a gated drop: the view holds no state
-- of its own beyond a copy of the reads it aggregates, and its inputs (se_company_address,
-- se_company_info, se_address_geocodes_served) are owned by other migrations and untouched.

DROP VIEW IF EXISTS corpscout.se_companies_current;
