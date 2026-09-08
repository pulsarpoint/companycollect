-- SE address slice 4c: the old address chain leaves ClickHouse. OWNER-RUN, by hand, after
-- the slice-4c dagster deploy is live and se_address_retirement_precheck.sql is clean.
-- Nothing in this repo executes this file (dev-phase ledger policy, owner ruling
-- 2026-08-25: a drop whose gate cannot be checked at write time never goes in the ledger).
--
-- ORDER MATTERS. Each object is dropped after everything that reads it. The three in the
-- middle are the tight ones: se_address_geocodes_served LEFT JOINs se_addresses_current for
-- postcode and post_town and reads se_address_geocodes_current as its precise base, so the
-- view goes first and its two sources follow.
--
-- Every drop below is async by default (the client is not told to wait): that default is
-- exactly what leaves the roughly 480-second UNDROP window these drops are gated on -- for
-- the nine MergeTree tables. Verified on ClickHouse 26.5: UNDROP TABLE does NOT recover a
-- refreshable materialized view or a plain view -- it returns "has been dropped, or the
-- database engine does not support UNDROP" for se_companies_serving_retired (refreshable
-- MV), se_address_geocodes_served (plain view) and se_address_geocodes_current (refreshable
-- MV). Recovery for those three is recreation, not UNDROP, and this slice emptied the
-- recreation DDL for two of them out of the ledger:
--   * se_address_geocodes_served: git show 65c9c4f1e:corpscout/clickhouse/migrations/000327_corpscout_se_address_geocodes_served_postal_box_fallback.up.sql
--   * se_address_geocodes_current: git show 65c9c4f1e:corpscout/clickhouse/migrations/000320_corpscout_se_address_geocodes_current_mv.up.sql
--   * se_companies_serving_retired (the parked serving render): migrations 000391 and
--     000392, still in the tree.
--
-- se_address_geocodes_current is a REFRESHABLE MATERIALIZED VIEW (migration 000320), and
-- DROP TABLE is what removes one together with its inner MergeTree -- migration 000392 ran
-- exactly that statement against se_companies_serving_retired on production and it worked.
-- se_address_geocodes_served is a plain VIEW (000325/000327) and takes DROP VIEW.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it):
-- corpscout.se_address_geocodes, se_postcode_centroids, se_city_centroids and the address
-- entity's own six tables, whose names se_company_address_legacy and the rest merely
-- prefix-collide with.

DROP TABLE IF EXISTS corpscout.se_companies_serving_retired;
DROP TABLE IF EXISTS corpscout.se_company_address_legacy;
DROP TABLE IF EXISTS corpscout.se_company_address_scb;
DROP TABLE IF EXISTS corpscout.se_company_address_bolagsverket;
DROP TABLE IF EXISTS corpscout.se_company_address_correction;
DROP TABLE IF EXISTS corpscout.se_company_addresses;
DROP TABLE IF EXISTS corpscout.se_company_addresses_current;
DROP TABLE IF EXISTS corpscout.se_company_address_members_current;
DROP VIEW IF EXISTS corpscout.se_address_geocodes_served;
DROP TABLE IF EXISTS corpscout.se_addresses_current;
DROP TABLE IF EXISTS corpscout.se_company_address_links_current;
DROP TABLE IF EXISTS corpscout.se_address_geocodes_current;
