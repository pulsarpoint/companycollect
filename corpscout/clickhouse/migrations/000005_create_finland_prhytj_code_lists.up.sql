CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_code_lists` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `file_run_id` Nullable(String),
  `file_key` Nullable(String),
  `code_list` Nullable(String),
  `language_code` Nullable(String),
  `code` Nullable(String),
  `description` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`file_key`, `code_list`, `language_code`, `code`, `source_run_id`)
SETTINGS allow_nullable_key = 1;
