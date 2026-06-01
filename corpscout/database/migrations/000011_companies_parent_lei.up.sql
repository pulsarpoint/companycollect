ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS parent_lei      VARCHAR(20),
    ADD COLUMN IF NOT EXISTS ultimate_parent_lei VARCHAR(20);

CREATE INDEX IF NOT EXISTS idx_companies_parent_lei
    ON companies(parent_lei) WHERE parent_lei IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_companies_ultimate_parent_lei
    ON companies(ultimate_parent_lei) WHERE ultimate_parent_lei IS NOT NULL;
