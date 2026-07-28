ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS buyer_sector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS buyer_subsector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS buyer_legal_form;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS buyer_sni_division;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sector;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_legal_form;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_size;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sni_division;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sni_main_group;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sni_group;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sni_subgroup;

ALTER TABLE corpscout.se_uhm_procurement_awards
    DROP COLUMN IF EXISTS supplier_sni_detail_group;
