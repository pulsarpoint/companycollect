-- SE address slice 4c (2026-09-08): this migration's objects -- the retired old address
-- chain (the register address history and its current snapshot, the canonical, members,
-- shared-identity and link tables, the per-source address artifacts and the correction
-- ledger, the legacy per-company geocode pair, the geocode serving view and overlay) --
-- were dropped by hand on the server and their DDL left this file per the dev-phase ledger
-- policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
