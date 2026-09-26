CREATE DATABASE IF NOT EXISTS corpscout;

-- Deploy removal of the old importer and Backoffice reads, stop any legacy runs,
-- and cancel their PostgreSQL manifests before applying. Saved outcomes remain.
DROP TABLE IF EXISTS corpscout.company_brave_search_input SYNC;
