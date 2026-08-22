CREATE DATABASE IF NOT EXISTS corpscout;

-- Sole traders (enskild firma) carry a 12-digit personnummer-based id in se_companies.
-- The info pilot copies them too, so has_company accepts 10 or 12 digits. All five tables
-- are still empty when this runs (phase 5 initial load has not happened), so no row can fail the new check.

ALTER TABLE corpscout.se_company_info_scb
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');

ALTER TABLE corpscout.se_company_info_esef
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');

ALTER TABLE corpscout.se_company_info_wikidata
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');

ALTER TABLE corpscout.se_company_info
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');

ALTER TABLE corpscout.se_company_info_correction
    DROP CONSTRAINT has_company,
    ADD CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');
