CREATE DATABASE IF NOT EXISTS corpscout;


ALTER TABLE corpscout.se_company_info_scb
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$');

ALTER TABLE corpscout.se_company_info_esef
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$');

ALTER TABLE corpscout.se_company_info_wikidata
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$');

ALTER TABLE corpscout.se_company_info
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$');

ALTER TABLE corpscout.se_company_info_correction
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$');
