CREATE DATABASE IF NOT EXISTS corpscout;

-- se_companies.legal_form_code mixes two registers' code systems -- Bolagsverket
-- organisationsform text codes (AB-ORGFO, 2.86M rows) and SCB juridisk-form numbers
-- (10, 71, ..., 668K rows) -- so the code on its own is unreadable on every surface.
-- corpscout.se_code_labels (code_type 'legal_form') is the curated dictionary that names
-- all 56 codes in use, in English and -- since 000305 -- in the official Swedish.
--
-- The owner's 2026-08-23 decision: the label is part of the info merge, COPIED from the
-- register exactly like the translated description and never written by the model, in BOTH
-- languages. Both are used in the UI, the Swedish one as the official term.
--
-- evidence_hash moves to v3 because the artifact's evidence now includes both labels: a
-- corrected label has to be appended as a new version by the artifact asset's anti-join,
-- exactly the way a late translation is under v2.
--
-- Rows written before this migration keep their v1/v2 hash on disk: MODIFY COLUMN of a
-- MATERIALIZED expression only changes the metadata, it does not rewrite existing parts.
-- The next SCB artifact run stages v3 hashes for every company, the anti-join appends them
-- (~3.5M versions), and the older version collapses at merge under the same
-- (company_id, source_record_uid) key.
--
-- The final gets the same two columns in the same order, straight after legal_form_code:
-- info.py copies them from the newest SCB row like every other non-description field. No
-- MATERIALIZED expression on the final reads them (evidence_set_hash covers the artifacts'
-- hashes only), so they need no expression change there. Rows published before this
-- migration read as '' until they are resolved again -- se_company_info is keyed by
-- company_id and every resolution appends a new version, so one resolve_all pass
-- (SECompanyInfoConfig.resolve_all) with the model off fills the columns for every
-- published company.
--
-- The second ADD COLUMN positions itself AFTER a column the FIRST one adds, in the same
-- statement -- ClickHouse applies an ALTER's commands in order, which the
-- clickhouse-local harness executes rather than assumes.

ALTER TABLE corpscout.se_company_info_scb
    ADD COLUMN IF NOT EXISTS legal_form_label_en String DEFAULT '' AFTER legal_form_code,
    ADD COLUMN IF NOT EXISTS legal_form_label_sv String DEFAULT '' AFTER legal_form_label_en,
    MODIFY COLUMN evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-scb-v3\n',
        ifNull(legal_name, ''), '\n', ifNull(legal_name_raw, ''), '\n',
        ifNull(legal_form_code, ''), '\n', legal_form_label_en, '\n', legal_form_label_sv, '\n', status, '\n',
        ifNull(toString(incorporation_date), ''), '\n', ifNull(toString(dissolution_date), ''), '\n',
        ifNull(activity_description, ''), '\n', activity_description_en, '\n', primary_sni_code, '\n', primary_nace_code
    ))));

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS legal_form_label_en String DEFAULT '' AFTER legal_form_code,
    ADD COLUMN IF NOT EXISTS legal_form_label_sv String DEFAULT '' AFTER legal_form_label_en;
