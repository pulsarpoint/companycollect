CREATE DATABASE IF NOT EXISTS corpscout;

-- The public view still reads the authoritative observation table at this migration boundary.
-- Drop the insert trigger before its target so concurrent observation inserts cannot target a
-- missing table during rollback.
DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_mv;
DROP TABLE IF EXISTS corpscout.domain_hostnames_state;
