ALTER TABLE corpscout.se_company_person_draft
    DROP COLUMN IF EXISTS wikidata_person_id;

ALTER TABLE corpscout.se_financial_report_signatories
    DROP COLUMN IF EXISTS person_role_hash,
    DROP COLUMN IF EXISTS person_profile_hash,
    DROP COLUMN IF EXISTS signatory_uid;

ALTER TABLE corpscout.esef_document_people
    DROP COLUMN IF EXISTS person_role_hash,
    DROP COLUMN IF EXISTS person_profile_hash;

ALTER TABLE corpscout.wikidata_company_people
    DROP COLUMN IF EXISTS person_role_hash;

ALTER TABLE corpscout.wikidata_persons
    DROP COLUMN IF EXISTS person_profile_hash;
