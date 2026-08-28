DROP VIEW IF EXISTS corpscout.se_ratsit_establishments_with_nace;
DROP VIEW IF EXISTS corpscout.se_ratsit_company_industries_with_nace;

ALTER TABLE corpscout.se_ratsit_financial_periods
    DROP CONSTRAINT se_ratsit_financial_period_kind,
    DROP COLUMN IF EXISTS period_kind;

ALTER TABLE corpscout.se_ratsit_responsible_people
    DROP CONSTRAINT se_ratsit_responsible_identity,
    DROP COLUMN IF EXISTS identity_available,
    DROP COLUMN IF EXISTS age,
    DROP COLUMN IF EXISTS name,
    DROP COLUMN IF EXISTS display_name_raw;

ALTER TABLE corpscout.se_ratsit_establishments
    DROP CONSTRAINT se_ratsit_establishment_employee_range,
    DROP CONSTRAINT se_ratsit_establishment_nace_mapping,
    DROP COLUMN IF EXISTS employee_count_open_ended,
    DROP COLUMN IF EXISTS employee_count_max,
    DROP COLUMN IF EXISTS employee_count_min,
    DROP COLUMN IF EXISTS nace_mapping_status,
    DROP COLUMN IF EXISTS nace_mapping_method,
    DROP COLUMN IF EXISTS nace_normalized_code,
    DROP COLUMN IF EXISTS nace_code,
    DROP COLUMN IF EXISTS nace_revision,
    DROP COLUMN IF EXISTS industry_description_original,
    DROP COLUMN IF EXISTS source_industry_code_set,
    DROP COLUMN IF EXISTS source_industry_code;

ALTER TABLE corpscout.se_ratsit_company_industry_codes
    DROP CONSTRAINT se_ratsit_industry_nace_mapping,
    DROP COLUMN IF EXISTS nace_mapping_status,
    DROP COLUMN IF EXISTS nace_mapping_method,
    DROP COLUMN IF EXISTS nace_normalized_code,
    DROP COLUMN IF EXISTS nace_code,
    DROP COLUMN IF EXISTS nace_revision,
    DROP COLUMN IF EXISTS industry_description_original,
    DROP COLUMN IF EXISTS source_industry_code_set,
    DROP COLUMN IF EXISTS source_industry_code;
