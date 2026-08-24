-- se_company_addresses_current lost source_record_uid's content-derived DEFAULT: the
-- weekly rebuild creates its stage with CREATE TABLE AS <current> and fills it from the
-- DuckDB export whose column list omits source_record_uid (tables.py
-- SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS), so the value comes from the column DEFAULT --
-- which the history table (000255) has but the current snapshot, first swapped in via
-- 000256's staged CREATE (plain String), does not. Result in production: every current
-- row carries source_record_uid = '' while all 4.79M history rows carry real uids, and
-- the se_company_address artifacts (which filter source_record_uid != '') select nothing.
--
-- Restoring the DEFAULT here is self-healing: every future weekly stage inherits it via
-- CREATE TABLE AS current, and the export's omitted column computes at insert exactly as
-- it does for the history table. Verified on production before this migration: the
-- expression below reproduces the history table's stored uid for 198,580/198,580 sampled
-- current rows joined on (source, source_record_id, source_payload_hash), 0 mismatches.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_addresses_current
    MODIFY COLUMN source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    ))));

-- One-time backfill of the rows already swapped in with ''. The expression is the
-- column's own DEFAULT, and rows written after the ALTER never match the WHERE.
ALTER TABLE corpscout.se_company_addresses_current
    UPDATE source_record_uid = lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        if(source = 'bolagsverket', 'sweden_bolagsverket', 'sweden_scb'),
        '\nregistry_company\n', source_record_id, '\n', lowerUTF8(source_payload_hash)
    ))))
    WHERE source_record_uid = '';
