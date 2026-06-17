DROP TABLE IF EXISTS corpscout.fi_company_situations;
DROP TABLE IF EXISTS corpscout.fi_tax_registrations;
DROP TABLE IF EXISTS corpscout.fi_registered_entries;
DROP TABLE IF EXISTS corpscout.fi_legal_forms;
DROP TABLE IF EXISTS corpscout.fi_addresses;
DROP TABLE IF EXISTS corpscout.fi_names;

ALTER TABLE corpscout.fi_companies
    DROP COLUMN IF EXISTS business_id_registration_date,
    DROP COLUMN IF EXISTS eu_id,
    DROP COLUMN IF EXISTS vat_id,
    DROP COLUMN IF EXISTS trade_register_status,
    DROP COLUMN IF EXISTS raw_status_code,
    DROP COLUMN IF EXISTS last_modified,
    DROP COLUMN IF EXISTS is_vat_registered,
    DROP COLUMN IF EXISTS is_employer_registered,
    DROP COLUMN IF EXISTS is_prepayment_registered;
