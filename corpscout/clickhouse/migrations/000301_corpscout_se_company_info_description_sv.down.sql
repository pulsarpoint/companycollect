CREATE DATABASE IF NOT EXISTS corpscout;

-- No MATERIALIZED expression reads description_sv (evidence_set_hash covers the
-- artifacts' hashes only), so the column can be dropped on its own.

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS description_sv;
