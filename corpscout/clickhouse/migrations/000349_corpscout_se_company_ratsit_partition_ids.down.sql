CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_company_id,
    ADD CONSTRAINT se_company_ratsit_company_id
        CHECK match(company_id, '^[0-9]{10}$');
