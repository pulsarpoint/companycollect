-- The dropped legacy tables contained early-development data and cannot be
-- restored. Rolling back only returns the canonical tables to their prior
-- temporary names so migration 000313 can be reapplied safely.
DROP TABLE IF EXISTS corpscout.esef_fact_disclosures_v2;
DROP TABLE IF EXISTS corpscout.esef_document_concept_labels_v2;
DROP TABLE IF EXISTS corpscout.esef_document_contact_candidates_v2;
DROP TABLE IF EXISTS corpscout.esef_facts_v2;
DROP TABLE IF EXISTS corpscout.esef_source_documents_v2;

RENAME TABLE
    corpscout.esef_source_documents TO corpscout.esef_source_documents_v2,
    corpscout.esef_facts TO corpscout.esef_facts_v2,
    corpscout.esef_document_contact_candidates TO corpscout.esef_document_contact_candidates_v2,
    corpscout.esef_document_concept_labels TO corpscout.esef_document_concept_labels_v2,
    corpscout.esef_fact_disclosures TO corpscout.esef_fact_disclosures_v2;
