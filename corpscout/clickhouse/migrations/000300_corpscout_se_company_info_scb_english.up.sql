CREATE DATABASE IF NOT EXISTS corpscout;

-- The SCB activity description (Bolagsverket verksamhetsbeskrivning) is Swedish, and
-- the Go translator already renders it into English in corpscout.text_translations
-- (source_table 'corpscout.se_companies', source_column 'activity_description', sv -> en),
-- which corpscout.se_companies_translated exposes as activity_description_en. The pilot
-- publishes English descriptions, so the SCB artifact carries the translated text beside
-- the Swedish original instead of re-deriving it downstream.
--
-- evidence_hash moves to v2 because the artifact's evidence now includes the translation:
-- a company whose translation arrives later (the translator runs outside this pipeline)
-- must be appended as a new version by the artifact asset's anti-join.
--
-- Rows written before this migration keep their v1 hash on disk: MODIFY COLUMN of a
-- MATERIALIZED expression only changes the metadata, it does not rewrite existing parts.
-- The next SCB artifact run stages v2 hashes for every company, the anti-join appends
-- them, and the older version collapses at merge under the same
-- (company_id, source_record_uid) key.

ALTER TABLE corpscout.se_company_info_scb
    ADD COLUMN IF NOT EXISTS activity_description_en String DEFAULT '' AFTER activity_description,
    MODIFY COLUMN evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-scb-v2\n',
        ifNull(legal_name, ''), '\n', ifNull(legal_name_raw, ''), '\n',
        ifNull(legal_form_code, ''), '\n', status, '\n',
        ifNull(toString(incorporation_date), ''), '\n', ifNull(toString(dissolution_date), ''), '\n',
        ifNull(activity_description, ''), '\n', activity_description_en, '\n', primary_sni_code, '\n', primary_nace_code
    ))));
