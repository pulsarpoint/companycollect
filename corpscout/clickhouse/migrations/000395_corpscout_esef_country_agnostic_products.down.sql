CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverts 000395. Drops the eight se_esef_* views, restores se_company_person_esef to its
-- pre-000395 (000331) definition reading esef_document_people directly, drops the three
-- country-agnostic tables and renames their _legacy originals back, then drops link_status.
DROP VIEW IF EXISTS corpscout.se_esef_document_group_relationships;
DROP VIEW IF EXISTS corpscout.se_esef_document_business_items;
DROP VIEW IF EXISTS corpscout.se_esef_document_people;
DROP VIEW IF EXISTS corpscout.se_esef_document_company_information;
DROP VIEW IF EXISTS corpscout.se_esef_document_contact_candidates;
DROP VIEW IF EXISTS corpscout.se_esef_disclosures;
DROP VIEW IF EXISTS corpscout.se_esef_facts;
DROP VIEW IF EXISTS corpscout.se_esef_filings;

CREATE OR REPLACE VIEW corpscout.se_company_person_esef AS
SELECT
    company_id,
    source_record_uid,
    person_profile_hash,
    person_role_hash,
    name AS full_name,
    role,
    role_category,
    organization,
    status,
    effective_from,
    effective_to,
    confidence,
    fiscal_year,
    extracted_at AS source_observed_at,
    candidate_uid
FROM corpscout.esef_document_people FINAL
WHERE country_code = 'SE';

DROP TABLE IF EXISTS corpscout.esef_document_group_relationships;
RENAME TABLE corpscout.esef_document_group_relationships_legacy TO corpscout.esef_document_group_relationships;

DROP TABLE IF EXISTS corpscout.esef_document_business_items;
RENAME TABLE corpscout.esef_document_business_items_legacy TO corpscout.esef_document_business_items;

DROP TABLE IF EXISTS corpscout.esef_document_people;
RENAME TABLE corpscout.esef_document_people_legacy TO corpscout.esef_document_people;

ALTER TABLE corpscout.esef_entity_registry_map
    DROP COLUMN IF EXISTS link_status;
