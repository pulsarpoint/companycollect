CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.br_government_contracts;

RENAME TABLE corpscout.br_government_contracts_live TO corpscout.br_government_contracts;

DROP VIEW IF EXISTS corpscout.ee_government_contracts;

RENAME TABLE corpscout.ee_government_contracts_live TO corpscout.ee_government_contracts;

DROP VIEW IF EXISTS corpscout.fi_government_contracts;

RENAME TABLE corpscout.fi_government_contracts_live TO corpscout.fi_government_contracts;

DROP VIEW IF EXISTS corpscout.fr_government_contracts;

RENAME TABLE corpscout.fr_government_contracts_live TO corpscout.fr_government_contracts;

DROP VIEW IF EXISTS corpscout.lv_government_contracts;

RENAME TABLE corpscout.lv_government_contracts_live TO corpscout.lv_government_contracts;

DROP VIEW IF EXISTS corpscout.no_government_contracts;

RENAME TABLE corpscout.no_government_contracts_live TO corpscout.no_government_contracts;

DROP VIEW IF EXISTS corpscout.se_government_contracts;

RENAME TABLE corpscout.se_government_contracts_live TO corpscout.se_government_contracts;

DROP VIEW IF EXISTS corpscout.sk_government_contracts;

RENAME TABLE corpscout.sk_government_contracts_live TO corpscout.sk_government_contracts;
