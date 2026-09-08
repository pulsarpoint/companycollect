ALTER TABLE corpscout.se_company_basic_info_history
    DROP COLUMN IF EXISTS economic_activity_source,
    DROP COLUMN IF EXISTS economic_activity;

ALTER TABLE corpscout.se_company_basic_info
    DROP COLUMN IF EXISTS economic_activity_source,
    DROP COLUMN IF EXISTS economic_activity;

ALTER TABLE corpscout.se_company_basic_info_suggestion
    DROP COLUMN IF EXISTS economic_activity;
