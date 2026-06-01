-- Compatibility no-op.
--
-- `company_domains` is owned by 000001_initial_schema in the current schema.
-- This migration used to recreate an older incompatible table shape, which
-- breaks fresh schema replay and sqlc generation.
SELECT 1;
