CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the technology catalog. Contents are fully rebuildable from the vendored
-- extension bundle plus the public webappanalyzer catalog by one run of the dagster
-- technology_catalog job -- the icon bucket is untouched (S3 objects are never deleted by
-- migrations).
DROP TABLE IF EXISTS corpscout.technology_catalog;
