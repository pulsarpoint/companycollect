CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.br_company_relations_snapshots
    DROP COLUMN IF EXISTS socios_part_count;
