ALTER TABLE corpscout.fr_companies
    DROP COLUMN IF EXISTS address,
    DROP COLUMN IF EXISTS address_supplement,
    DROP COLUMN IF EXISTS postal_code,
    DROP COLUMN IF EXISTS city,
    DROP COLUMN IF EXISTS city_code,
    DROP COLUMN IF EXISTS country_label;
