CREATE DATABASE IF NOT EXISTS corpscout;

-- Restore 000300's v2 evidence_hash expression first, so nothing references the two label
-- columns by the time they are dropped -- the same order 000300's own down file uses.

ALTER TABLE corpscout.se_company_info_scb
    MODIFY COLUMN evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-scb-v2\n',
        ifNull(legal_name, ''), '\n', ifNull(legal_name_raw, ''), '\n',
        ifNull(legal_form_code, ''), '\n', status, '\n',
        ifNull(toString(incorporation_date), ''), '\n', ifNull(toString(dissolution_date), ''), '\n',
        ifNull(activity_description, ''), '\n', activity_description_en, '\n', primary_sni_code, '\n', primary_nace_code
    ))));

ALTER TABLE corpscout.se_company_info_scb
    DROP COLUMN IF EXISTS legal_form_label_sv,
    DROP COLUMN IF EXISTS legal_form_label_en;

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS legal_form_label_sv,
    DROP COLUMN IF EXISTS legal_form_label_en;
