CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the SE company-info correction ledger, replaced by se_company_info_field_value
-- (000371). Written at the apply step, after backoffice and dagster_v3 were already running
-- the code that no longer reads this table, per the 2026-08-25 ruling that a waiting DROP
-- never sits in the ledger. Gate re-verified 2026-09-02 immediately before apply: 0 of
-- 3,552,806 published rows carried correction_ids and the ledger held 4 rows. UNDROP
-- window is ~480 s after apply.
DROP TABLE IF EXISTS corpscout.se_company_info_correction;
