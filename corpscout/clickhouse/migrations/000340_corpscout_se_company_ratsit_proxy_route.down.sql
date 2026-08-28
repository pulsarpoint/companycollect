CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_proxy_route;

ALTER TABLE corpscout.se_company_ratsit
    DROP COLUMN IF EXISTS proxy_name;

ALTER TABLE corpscout.se_company_ratsit
    DROP COLUMN IF EXISTS connection_mode;
