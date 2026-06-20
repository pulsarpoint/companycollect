-- Drop large/unused provenance columns from the Norway + Finland ClickHouse
-- export tables, matching the Latvia change in 000021: the full source JSON
-- (raw_*) dominates table size, and the per-row source_payload_hash is
-- incompressible (one unique SHA256 per row) — neither is queried. They are
-- retained in the DuckDB/dbt staging layer. Forward-only + IF EXISTS, so this
-- is a no-op on a fresh deploy (after the CREATE migrations) and a real drop on
-- databases where the columns already exist.
CREATE DATABASE IF NOT EXISTS corpscout;

-- Norway BRREG raw tables (raw_* is the full source JSON).
ALTER TABLE corpscout.companies
    DROP COLUMN IF EXISTS raw_entity,
    DROP COLUMN IF EXISTS source_payload_hash;

ALTER TABLE corpscout.financial_statements
    DROP COLUMN IF EXISTS raw_financial_record,
    DROP COLUMN IF EXISTS source_payload_hash;

-- Norway resolved tables.
ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.no_websites DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.no_industries DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.no_financial_statements DROP COLUMN IF EXISTS source_payload_hash;

-- Finland resolved tables.
ALTER TABLE corpscout.fi_companies DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.fi_names DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.fi_websites DROP COLUMN IF EXISTS source_payload_hash;
ALTER TABLE corpscout.fi_industries DROP COLUMN IF EXISTS source_payload_hash;
