CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings` (
  `source_code_set` Nullable(String),
  `source_code` Nullable(String),
  `source_code_prefix4` Nullable(String),
  `source_code_dotted4` Nullable(String),
  `source_extra_digit` Nullable(String),
  `source_description_en` Nullable(String),
  `nace_revision` Nullable(String),
  `nace_code` Nullable(String),
  `nace_normalized_code` Nullable(String),
  `nace_section_code` Nullable(String),
  `nace_division_code` Nullable(String),
  `nace_group_code` Nullable(String),
  `nace_class_code` Nullable(String),
  `nace_title_en` Nullable(String),
  `mapping_method` LowCardinality(String),
  `mapping_status` LowCardinality(String),
  `mapped_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`mapped_at`)
ORDER BY (`source_code_set`, `source_code`)
SETTINGS allow_nullable_key = 1;

ALTER TABLE `corpscout_sources`.`fi_prhytj_company_explorer_cache`
  ADD COLUMN IF NOT EXISTS `nace_revision` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_normalized_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_section_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_division_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_group_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_class_code` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_title_en` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_mapping_method` Nullable(String),
  ADD COLUMN IF NOT EXISTS `nace_mapping_status` Nullable(String);
