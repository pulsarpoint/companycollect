CREATE DATABASE IF NOT EXISTS corpscout;

-- Restore 000297's v1 evidence_hash expression first, so nothing references
-- activity_description_en by the time the column is dropped.

ALTER TABLE corpscout.se_company_info_scb
    MODIFY COLUMN evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-scb-v1\n',
        ifNull(legal_name, ''), '\n', ifNull(legal_name_raw, ''), '\n',
        ifNull(legal_form_code, ''), '\n', status, '\n',
        ifNull(toString(incorporation_date), ''), '\n', ifNull(toString(dissolution_date), ''), '\n',
        ifNull(activity_description, ''), '\n', primary_sni_code, '\n', primary_nace_code
    ))));

ALTER TABLE corpscout.se_company_info_scb
    DROP COLUMN IF EXISTS activity_description_en;
