DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

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
SELECT * FROM corpscout.sk_government_contract_summary
UNION ALL
SELECT * FROM corpscout.lv_government_contract_summary;

DROP VIEW IF EXISTS corpscout.ee_government_contract_summary;
DROP VIEW IF EXISTS corpscout.ee_government_contracts;
DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_winners_current;
DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_lots_current;
DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_notices_current;

DROP TABLE IF EXISTS corpscout.ee_rhr_procurement_winners;
DROP TABLE IF EXISTS corpscout.ee_rhr_procurement_lots;
DROP TABLE IF EXISTS corpscout.ee_rhr_procurement_notices;
