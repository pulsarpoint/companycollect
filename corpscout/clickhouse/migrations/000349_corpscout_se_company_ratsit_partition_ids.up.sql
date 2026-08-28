CREATE DATABASE IF NOT EXISTS corpscout;

-- Active se_companies includes canonical twelve-digit identifiers for sole
-- traders. Ratsit receives only their final ten digits in the URL, while scan
-- history and S3 retain the canonical company ID used by Corpscout.
ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_company_id,
    ADD CONSTRAINT se_company_ratsit_company_id
        CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');
