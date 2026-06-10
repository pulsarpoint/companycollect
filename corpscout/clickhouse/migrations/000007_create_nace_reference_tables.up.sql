CREATE DATABASE IF NOT EXISTS `corpscout_reference`;

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_classifications` (
  `revision` String,
  `name` String,
  `valid_from` Nullable(Date),
  `valid_to` Nullable(Date),
  `source_url` Nullable(String),
  `active_codes` UInt64,
  `inactive_codes` UInt64,
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`);

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_codes` (
  `revision` String,
  `code` String,
  `normalized_code` String,
  `level` UInt8,
  `level_name` LowCardinality(String),
  `parent_code` Nullable(String),
  `parent_normalized_code` Nullable(String),
  `title` String,
  `description` Nullable(String),
  `active` Bool,
  `section_code` Nullable(String),
  `division_code` Nullable(String),
  `group_code` Nullable(String),
  `class_code` Nullable(String),
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`, `level`, `normalized_code`);

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_code_aliases` (
  `revision` String,
  `code` String,
  `alias_type` LowCardinality(String),
  `alias_code` String,
  `normalized_alias_code` String,
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`, `alias_type`, `normalized_alias_code`, `code`);
