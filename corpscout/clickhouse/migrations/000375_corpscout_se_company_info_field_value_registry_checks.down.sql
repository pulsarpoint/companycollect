CREATE DATABASE IF NOT EXISTS corpscout;

-- Restores 000371's pilot lists. Rows written for the wider lists would fail these
-- CHECKs only on a later INSERT, never retroactively.

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_field,
    ADD CONSTRAINT known_field CHECK field IN ('description', 'description_sv');

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_source,
    ADD CONSTRAINT known_source CHECK source IN ('scb', 'esef', 'wikidata', 'llm', 'reviewer');
