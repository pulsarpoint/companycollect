-- Source identities answer "which source row/entity?". These semantic hashes
-- answer "did any person or role field relevant to the draft change?". They
-- deliberately exclude run ids, retrieval timestamps, raw payload hashes and
-- source-record version ids.
CREATE DATABASE IF NOT EXISTS corpscout;

-- The wikidata_person_id column this migration also added to se_company_person_draft was
-- dropped by hand with that table in SE person slice 0 (2026-09-09) and its DDL left this
-- file per the dev-phase ledger policy. The four source-table hash columns below stay.

ALTER TABLE corpscout.wikidata_persons
    ADD COLUMN IF NOT EXISTS person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            toString(length(lowerUTF8(trim(ifNull(description, ''))))), ':',
            lowerUTF8(trim(ifNull(description, '')))
        )))) AFTER source_record_uid;

ALTER TABLE corpscout.wikidata_company_people
    ADD COLUMN IF NOT EXISTS person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role_property)))), ':',
            lowerUTF8(trim(role_property)), '\n',
            toString(length(lowerUTF8(trim(role_label)))), ':',
            lowerUTF8(trim(role_label)), '\n',
            ifNull(toString(start_date), ''), '\n',
            ifNull(toString(end_date), ''), '\n',
            toString(is_current)
        )))) AFTER source_payload_hash;

ALTER TABLE corpscout.esef_document_people
    ADD COLUMN IF NOT EXISTS person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(name)))), ':', lowerUTF8(trim(name)), '\n',
            '0:'
        )))) AFTER name,
    ADD COLUMN IF NOT EXISTS person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role)))), ':', lowerUTF8(trim(role)), '\n',
            toString(length(lowerUTF8(trim(role_category)))), ':',
            lowerUTF8(trim(role_category)), '\n',
            toString(length(lowerUTF8(trim(organization)))), ':',
            lowerUTF8(trim(organization)), '\n',
            toString(length(lowerUTF8(trim(status)))), ':', lowerUTF8(trim(status)), '\n',
            ifNull(toString(effective_from), ''), '\n',
            ifNull(toString(effective_to), ''), '\n',
            toString(fiscal_year)
        )))) AFTER status;

ALTER TABLE corpscout.se_financial_report_signatories
    ADD COLUMN IF NOT EXISTS signatory_uid FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'sweden-financial-report-signatory-v1\n',
            company_id, '\n', statement_key, '\n', signatory_kind, '\n',
            toString(person_seq)
        )))) AFTER person_seq,
    ADD COLUMN IF NOT EXISTS person_profile_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-profile-v1\n',
            toString(length(lowerUTF8(trim(first_name)))), ':',
            lowerUTF8(trim(first_name)), '\n',
            toString(length(lowerUTF8(trim(last_name)))), ':',
            lowerUTF8(trim(last_name))
        )))) AFTER last_name,
    ADD COLUMN IF NOT EXISTS person_role_hash FixedString(64) MATERIALIZED
        lower(hex(SHA256(concat(
            'company-person-role-v1\n',
            toString(length(lowerUTF8(trim(role_original)))), ':',
            lowerUTF8(trim(role_original)), '\n',
            toString(length(lowerUTF8(trim(role_kind)))), ':',
            lowerUTF8(trim(role_kind)), '\n',
            toString(length(lowerUTF8(trim(signatory_kind)))), ':',
            lowerUTF8(trim(signatory_kind)), '\n',
            toString(fiscal_year)
        )))) AFTER role_kind;
