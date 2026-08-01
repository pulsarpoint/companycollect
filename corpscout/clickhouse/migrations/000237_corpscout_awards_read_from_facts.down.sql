CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.br_government_contract_awards;

RENAME TABLE corpscout.br_government_contract_awards_live TO corpscout.br_government_contract_awards;

DROP VIEW IF EXISTS corpscout.no_government_contract_awards;

RENAME TABLE corpscout.no_government_contract_awards_live TO corpscout.no_government_contract_awards;
