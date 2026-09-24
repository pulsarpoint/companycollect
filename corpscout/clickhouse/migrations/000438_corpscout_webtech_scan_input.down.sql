ALTER TABLE corpscout.webtech_domain_scan_results DROP COLUMN IF EXISTS input_id;
ALTER TABLE corpscout.webtech_domain_scan_results DROP COLUMN IF EXISTS task_id;
DROP TABLE IF EXISTS corpscout.webtech_scan_input;
