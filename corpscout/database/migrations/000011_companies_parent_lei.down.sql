DROP INDEX IF EXISTS idx_companies_ultimate_parent_lei;
DROP INDEX IF EXISTS idx_companies_parent_lei;

ALTER TABLE companies
    DROP COLUMN IF EXISTS ultimate_parent_lei,
    DROP COLUMN IF EXISTS parent_lei;
