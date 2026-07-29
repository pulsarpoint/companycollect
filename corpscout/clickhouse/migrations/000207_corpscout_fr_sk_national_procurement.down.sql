CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;
DROP VIEW IF EXISTS corpscout.fr_government_contract_summary;
DROP VIEW IF EXISTS corpscout.sk_government_contract_summary;
DROP VIEW IF EXISTS corpscout.fr_government_contracts;
DROP VIEW IF EXISTS corpscout.sk_government_contracts;
DROP TABLE IF EXISTS corpscout.fr_decp_contract_holders;
DROP TABLE IF EXISTS corpscout.sk_uvo_procurement_notices;

CREATE VIEW corpscout.company_government_contract_summary AS
SELECT * FROM corpscout.se_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fi_government_contract_summary
UNION ALL
SELECT * FROM corpscout.no_government_contract_summary
UNION ALL
SELECT * FROM corpscout.br_government_contract_summary
UNION ALL
SELECT * FROM corpscout.lv_government_contract_summary
UNION ALL
SELECT * FROM corpscout.ee_government_contract_summary;
