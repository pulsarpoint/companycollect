CREATE DATABASE IF NOT EXISTS corpscout;

-- How UHM describes the two parties to an award, which we have been parsing and
-- then throwing away since the source was built.
--
-- Sector is the only signal we receive anywhere that says who OWNS an entity,
-- as opposed to what legal form it holds. Those are different questions and
-- conflating them mislabels things in both directions. Hässleholm Miljö AB
-- (5565550349) is an ordinary aktiebolag, so classifying it as a company is
-- correct and still misses that it is a municipal waste operation. Nearly half
-- of Sweden's TED buyers are companies of that kind.
--
-- Stored EXACTLY as UHM wrote it. Nothing is mapped, bucketed or translated on
-- the way in. These values are published per award ROW, so they describe a
-- party in a role rather than the company outright, and what an entity IS gets
-- decided in a per-country view -- where a wrong answer costs one statement of
-- DDL instead of re-materializing the register.
--
-- The supplier columns are worth as much as the buyer ones and are easy to
-- overlook: company size and a five-level industry classification, for 96k
-- award rows, which is data we hold for almost no other source.
ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS buyer_sector LowCardinality(String)
    AFTER buyer_id_normalized;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS buyer_subsector LowCardinality(String)
    AFTER buyer_sector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS buyer_legal_form LowCardinality(String)
    AFTER buyer_subsector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS buyer_sni_division LowCardinality(String)
    AFTER buyer_legal_form;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sector LowCardinality(String)
    AFTER supplier_id_normalized;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_legal_form LowCardinality(String)
    AFTER supplier_sector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_size LowCardinality(String)
    AFTER supplier_legal_form;

-- SNI at all five published levels. Keeping only the division would discard the
-- precision that makes the deeper ones worth having.
ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sni_division LowCardinality(String)
    AFTER supplier_size;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sni_main_group LowCardinality(String)
    AFTER supplier_sni_division;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sni_group LowCardinality(String)
    AFTER supplier_sni_main_group;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sni_subgroup LowCardinality(String)
    AFTER supplier_sni_group;

ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS supplier_sni_detail_group LowCardinality(String)
    AFTER supplier_sni_subgroup;
