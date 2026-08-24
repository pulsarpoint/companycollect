-- Filing identity belongs to corpscout.esef_filings. Parsed ESEF data has four
-- serving contracts: facts, contact candidates, concept labels, and disclosures.
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.esef_source_documents;
