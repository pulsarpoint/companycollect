-- SE person slice 0 (2026-09-09): this migration's objects -- the 2026-08-19 people model
-- (the person draft and its legacy twin, the resolved person table, the role and role-draft
-- tables, the correction ledger and enrichment observations with their writer grants, the
-- collision-candidate table and the three per-source read views) -- were dropped by hand on
-- the server and their DDL left this file per the dev-phase ledger policy. The file stays
-- for history.
CREATE DATABASE IF NOT EXISTS corpscout;
