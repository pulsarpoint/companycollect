CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;
DROP VIEW IF EXISTS corpscout.lv_government_contract_summary;
DROP VIEW IF EXISTS corpscout.lv_government_contracts;
DROP VIEW IF EXISTS corpscout.lv_iub_contract_executions_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notice_winners_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notice_lots_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notices_current;
DROP TABLE IF EXISTS corpscout.lv_iub_contract_executions;
DROP TABLE IF EXISTS corpscout.lv_iub_notice_winners;
DROP TABLE IF EXISTS corpscout.lv_iub_notice_lots;
DROP TABLE IF EXISTS corpscout.lv_iub_notices;

CREATE VIEW corpscout.company_government_contract_summary AS
SELECT * FROM corpscout.se_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fi_government_contract_summary
UNION ALL
SELECT * FROM corpscout.no_government_contract_summary
UNION ALL
SELECT * FROM corpscout.br_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fr_government_contract_summary
UNION ALL
SELECT * FROM corpscout.sk_government_contract_summary;
