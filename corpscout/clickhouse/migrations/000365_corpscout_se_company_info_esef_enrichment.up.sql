CREATE DATABASE IF NOT EXISTS corpscout;

-- Additive serving columns, deliberately outside the v1 evidence_hash concat.
-- Changing that provenance expression would rewrite the hash for every row.
ALTER TABLE corpscout.se_company_info_esef
    ADD COLUMN IF NOT EXISTS customer_markets_json String DEFAULT '' AFTER products_and_services_json,
    ADD COLUMN IF NOT EXISTS operating_geographies_json String DEFAULT '' AFTER customer_markets_json,
    ADD COLUMN IF NOT EXISTS material_group_relationships_json String DEFAULT '' AFTER business_segments_json;
