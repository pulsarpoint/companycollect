CREATE DATABASE IF NOT EXISTS corpscout;

-- THE ECONOMIC-ACTIVITY FIELD (2026-09-08 slice-6 design). SCB's Företagsstatus is an
-- economic-activity flag: 1 registered for VAT, F-tax or as an employer, 0 never was,
-- 9 was and no longer is. Since the 2026-09-08 precedence change it no longer decides
-- status for the companies Bolagsverket files, so it becomes its own entity field with the
-- values active, never and ceased. Three ALTERs, metadata-only: NULL on a suggestion row
-- means the source has no opinion, '' on a main or history row means unknown, like status.
-- Existing rows read '' until the fold rewrites them.
ALTER TABLE corpscout.se_company_basic_info_suggestion
    ADD COLUMN IF NOT EXISTS economic_activity Nullable(String) AFTER status;

ALTER TABLE corpscout.se_company_basic_info
    ADD COLUMN IF NOT EXISTS economic_activity LowCardinality(String) AFTER status_source,
    ADD COLUMN IF NOT EXISTS economic_activity_source LowCardinality(String) AFTER economic_activity;

ALTER TABLE corpscout.se_company_basic_info_history
    ADD COLUMN IF NOT EXISTS economic_activity LowCardinality(String) AFTER status_source,
    ADD COLUMN IF NOT EXISTS economic_activity_source LowCardinality(String) AFTER economic_activity;
