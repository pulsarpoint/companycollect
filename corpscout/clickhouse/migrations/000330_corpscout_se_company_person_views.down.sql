CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000330. Drops only the three read views and the collision-candidate table this
-- migration created. Every upstream source table (se_financial_report_signatories,
-- esef_document_people, wikidata_company_people, wikidata_persons,
-- wikidata_company_identifiers, company_identifier) is owned by earlier migrations and is
-- left untouched.
DROP VIEW IF EXISTS corpscout.se_company_person_bolagsverket;
DROP VIEW IF EXISTS corpscout.se_company_person_esef;
DROP VIEW IF EXISTS corpscout.se_company_person_wikidata;
DROP TABLE IF EXISTS corpscout.se_company_person_collision_candidate;
