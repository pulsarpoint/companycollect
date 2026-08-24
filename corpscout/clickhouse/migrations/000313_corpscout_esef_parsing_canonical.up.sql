-- The partitioned parsing tables are the only supported ESEF contracts.
-- This intentionally removes the early-development, non-partitioned tables.
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.esef_fact_disclosures;
DROP TABLE IF EXISTS corpscout.esef_document_concept_labels;
DROP TABLE IF EXISTS corpscout.esef_document_contact_candidates;
DROP TABLE IF EXISTS corpscout.esef_facts;
DROP TABLE IF EXISTS corpscout.esef_source_documents;

RENAME TABLE
    corpscout.esef_source_documents_v2 TO corpscout.esef_source_documents,
    corpscout.esef_facts_v2 TO corpscout.esef_facts,
    corpscout.esef_document_contact_candidates_v2 TO corpscout.esef_document_contact_candidates,
    corpscout.esef_document_concept_labels_v2 TO corpscout.esef_document_concept_labels,
    corpscout.esef_fact_disclosures_v2 TO corpscout.esef_fact_disclosures;
