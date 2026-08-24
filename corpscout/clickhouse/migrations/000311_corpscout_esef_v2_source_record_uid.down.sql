ALTER TABLE corpscout.esef_source_documents_v2
    DROP COLUMN IF EXISTS source_record_uid;

ALTER TABLE corpscout.esef_document_contact_candidates_v2
    DROP COLUMN IF EXISTS source_record_uid;
