ALTER TABLE corpscout.wikidata_persons
    DROP COLUMN IF EXISTS source_record_uid;

ALTER TABLE corpscout.se_company_audits
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_company_officers
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_financial_metrics
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_financial_facts
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_financial_reports
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_industries
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_company_addresses
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.se_companies
    DROP COLUMN IF EXISTS bolagsverket_source_record_uid,
    DROP COLUMN IF EXISTS scb_source_record_uid;
ALTER TABLE corpscout.esef_document_company_information
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.esef_document_contact_candidates
    DROP COLUMN IF EXISTS source_record_uid;
ALTER TABLE corpscout.esef_source_documents
    DROP COLUMN IF EXISTS source_record_uid;

DROP TABLE IF EXISTS corpscout.esef_document_group_relationships;
DROP TABLE IF EXISTS corpscout.esef_document_business_items;
DROP TABLE IF EXISTS corpscout.esef_document_people;
DROP TABLE IF EXISTS corpscout.wikidata_company_source_snapshots;
DROP TABLE IF EXISTS corpscout.company_description_observations;
DROP TABLE IF EXISTS corpscout.company_source_record_links;
DROP TABLE IF EXISTS corpscout.company_source_record_origins;
DROP TABLE IF EXISTS corpscout.company_source_records;
