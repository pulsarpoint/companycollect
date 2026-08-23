CREATE DATABASE IF NOT EXISTS corpscout;

-- The pilot publishes English descriptions, but the Swedish text is what the register
-- itself says (SCB's verksamhetsbeskrivning) and what a Swedish-facing surface needs.
-- The owner's 2026-08-23 decision is that the final holds both languages natively:
-- description stays the published English text, description_sv the Swedish one.
--
-- Where each value comes from: SCB's own activity_description deterministically (the
-- Swedish original, never the translation 000300 added), and the model's Swedish summary
-- when several sources had to be merged -- written in the same call as the English one
-- from the same facts (prompt version se-company-info-description-v3), so no translation
-- pass and no bilingual view have to be kept in step with this column.
--
-- Nullable rather than DEFAULT '': a Wikidata/ESEF-only description has no Swedish
-- original at all, which is a different fact from an empty string.
--
-- Rows written before this migration read as NULL until they are resolved again.
-- se_company_info is keyed by company_id and every resolution appends a new version, so
-- one resolve_all pass (SECompanyInfoConfig.resolve_all) fills the column for every
-- published company -- nothing about their evidence moved, so no other scan would.

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS description_sv Nullable(String) AFTER description;
