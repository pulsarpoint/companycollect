-- Drop large/unused provenance columns from the Latvia ClickHouse tables: the
-- full source JSON (raw_*) dominates table size, and the per-row source_payload_hash
-- is incompressible (one unique SHA256 per row) — neither is queried. They are
-- retained in the DuckDB staging layer. Forward-only + IF EXISTS, so this is a
-- no-op on a fresh deploy (after the CREATE migrations) and a real drop on
-- databases where the columns already exist.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.lv_companies
    DROP COLUMN IF EXISTS raw_entity,
    DROP COLUMN IF EXISTS source_payload_hash;

ALTER TABLE corpscout.lv_financial_statements
    DROP COLUMN IF EXISTS raw_financial_record,
    DROP COLUMN IF EXISTS source_payload_hash;

ALTER TABLE corpscout.lv_financial_metrics
    DROP COLUMN IF EXISTS source_payload_hash;
