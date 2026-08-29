CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the adoption rollup. Fully rebuildable by one run of the technology_catalog job.
DROP TABLE IF EXISTS corpscout.technology_adoption;
