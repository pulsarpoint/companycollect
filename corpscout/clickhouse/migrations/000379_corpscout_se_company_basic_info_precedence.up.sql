-- The per-field, per-source precedence of section 4 as exported from
-- dagster_v3.defs.se_company.basic_info.precedence by the
-- se_company_basic_info_precedence_clickhouse asset (2026-09-03 SE basic-info design,
-- section 3.5). Read by the backoffice for display and validation. Never edited here,
-- the Python dictionary is the only source. A re-export writes every pair the dictionary
-- names. A pair the dictionary no longer names stays in this table until it is removed by
-- hand, and the export reports it as stale_pairs.
-- Superseded by 000381, which recreates this table with a company scope (slice 3b, 2026-09-05)
CREATE DATABASE IF NOT EXISTS corpscout;
