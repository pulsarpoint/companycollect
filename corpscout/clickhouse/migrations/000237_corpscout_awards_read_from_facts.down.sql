CREATE DATABASE IF NOT EXISTS corpscout;

-- no_government_contract_awards removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

DROP VIEW IF EXISTS corpscout.br_government_contract_awards;

RENAME TABLE corpscout.br_government_contract_awards_live TO corpscout.br_government_contract_awards;
