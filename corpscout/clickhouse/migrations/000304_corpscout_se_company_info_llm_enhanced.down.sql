CREATE DATABASE IF NOT EXISTS corpscout;

-- The exact inverse, in mirrored order: description_source comes back in the slot it
-- held (AFTER description_language, before description_sources), and llm_enhanced goes.
-- The restored column reads as '' for every row -- the label it used to carry ('scb',
-- 'wikidata', 'esef', 'llm', 'reviewed') is not recoverable from the flag, and a
-- resolve_all pass on the reverted code would rewrite it anyway.

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS description_source LowCardinality(String) AFTER description_language;

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS llm_enhanced;
