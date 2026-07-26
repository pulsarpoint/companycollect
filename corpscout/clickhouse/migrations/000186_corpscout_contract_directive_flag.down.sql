-- 000184 and 000185 recreate these views without the directive flag.
DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

DROP VIEW IF EXISTS corpscout.company_government_contracts;

DROP VIEW IF EXISTS corpscout.no_government_contracts;

DROP VIEW IF EXISTS corpscout.fi_government_contracts;

DROP VIEW IF EXISTS corpscout.se_government_contracts;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS directive_governed;
