ALTER TABLE `corpscout_sources`.`fi_prhytj_company_explorer_cache`
  DROP COLUMN IF EXISTS `nace_revision`,
  DROP COLUMN IF EXISTS `nace_code`,
  DROP COLUMN IF EXISTS `nace_normalized_code`,
  DROP COLUMN IF EXISTS `nace_section_code`,
  DROP COLUMN IF EXISTS `nace_division_code`,
  DROP COLUMN IF EXISTS `nace_group_code`,
  DROP COLUMN IF EXISTS `nace_class_code`,
  DROP COLUMN IF EXISTS `nace_title_en`,
  DROP COLUMN IF EXISTS `nace_mapping_method`,
  DROP COLUMN IF EXISTS `nace_mapping_status`;

DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings`;
