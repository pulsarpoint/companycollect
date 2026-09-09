CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000395. Drops the eight se_esef_* views first, then drops the three
-- country-agnostic tables and renames their _legacy originals back, and finally drops
-- link_status.
DROP VIEW IF EXISTS corpscout.se_esef_document_group_relationships;
DROP VIEW IF EXISTS corpscout.se_esef_document_business_items;
DROP VIEW IF EXISTS corpscout.se_esef_document_people;
DROP VIEW IF EXISTS corpscout.se_esef_document_company_information;
DROP VIEW IF EXISTS corpscout.se_esef_document_contact_candidates;
DROP VIEW IF EXISTS corpscout.se_esef_disclosures;
DROP VIEW IF EXISTS corpscout.se_esef_facts;
DROP VIEW IF EXISTS corpscout.se_esef_filings;

DROP TABLE IF EXISTS corpscout.esef_document_group_relationships;
RENAME TABLE corpscout.esef_document_group_relationships_legacy TO corpscout.esef_document_group_relationships;

DROP TABLE IF EXISTS corpscout.esef_document_business_items;
RENAME TABLE corpscout.esef_document_business_items_legacy TO corpscout.esef_document_business_items;

DROP TABLE IF EXISTS corpscout.esef_document_people;
RENAME TABLE corpscout.esef_document_people_legacy TO corpscout.esef_document_people;

-- The se_company_person_esef read view was dropped by hand in SE person slice 0
-- (2026-09-09) and its DDL left this file per the dev-phase ledger policy. This migration's
-- ESEF products -- esef_document_people_legacy and the country-scoped se_esef_document_people
-- view -- are untouched.

ALTER TABLE corpscout.esef_entity_registry_map
    DROP COLUMN IF EXISTS link_status;
