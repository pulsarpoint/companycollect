CREATE DATABASE IF NOT EXISTS corpscout;

-- Nothing on the reverted code reads label_sv: corpscout.se_companies_translated projects
-- label_en only, and 000306 (which joins both) is rolled back before this file runs. So
-- the column drops on its own, and a re-seed on the reverted code restores the table.

ALTER TABLE corpscout.se_code_labels
    DROP COLUMN IF EXISTS label_sv;
