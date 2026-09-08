-- Basic-info slice 4 (2026-09-08): this migration's objects (the retired se_company_info*
-- tables or their columns) were dropped by hand on the server and their DDL left this
-- file per the dev-phase ledger policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
