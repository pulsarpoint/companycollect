CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_person_role
    DROP COLUMN IF EXISTS correction_ids;

ALTER TABLE corpscout.se_company_person
    DROP COLUMN IF EXISTS merged_into_person_id,
    DROP COLUMN IF EXISTS suggestion_id,
    DROP COLUMN IF EXISTS correction_set_hash,
    DROP COLUMN IF EXISTS correction_ids;

DROP TABLE IF EXISTS corpscout.se_company_person_enrichment_observation;
DROP TABLE IF EXISTS corpscout.se_company_person_correction;
