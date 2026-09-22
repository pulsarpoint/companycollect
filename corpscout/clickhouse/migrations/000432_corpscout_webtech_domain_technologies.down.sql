DROP VIEW IF EXISTS corpscout.webtech_domain_technologies_current;
DROP TABLE IF EXISTS corpscout.webtech_domain_technologies;
ALTER TABLE corpscout.technology_catalog DROP COLUMN IF EXISTS technology_id;
