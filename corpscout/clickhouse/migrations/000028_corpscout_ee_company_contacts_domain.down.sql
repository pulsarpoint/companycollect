ALTER TABLE corpscout.ee_company_contacts
    DROP COLUMN IF EXISTS domain,
    DROP COLUMN IF EXISTS domain_source;
